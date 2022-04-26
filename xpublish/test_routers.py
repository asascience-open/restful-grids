from atexit import register
from logging import getLogger
import logging
import re
from typing import Optional
import io

from fastapi import APIRouter, Body, Depends, HTTPException, Query, Response as FastApiResponse
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from requests import Response
import xarray as xr
import cf_xarray as cfxr
import xpublish
from xpublish.dependencies import get_dataset
from xpublish.routers import base_router, zarr_router
from rasterio.enums import Resampling
from PIL import Image
from matplotlib import cm
import numpy as np
# rioxarray will show as not being used but its necesarry for enabling rio extensions for xarray
import rioxarray

# logger = logging.getLogger(__name__)
logger = logging.getLogger("fastapi")

ds = xr.open_dataset("../datasets/ww3_72_east_coast_2022041112.nc")
# We need a coordinate system to tile 
ds = ds.rio.write_crs(4326)

meanrouter = APIRouter()


@meanrouter.get("/{var_name}/mean")
def get_mean(var_name: str, dataset: xr.Dataset = Depends(get_dataset)):
    if var_name not in dataset.variables:
        raise HTTPException(
            status_code=404, detail=f"Variable `{var_name}` not found in dataset"
        )

    return float(ds[var_name].mean())


edrrouter = APIRouter()


class EDRQuery(BaseModel):
    coords: str = Field(..., title="Point in WKT format")
    z: Optional[str] = None
    datetime: Optional[str] = None
    parameters: Optional[str] = None
    crs: Optional[str] = None
    f: Optional[str] = None

    @property
    def point(self):
        from shapely import wkt

        return wkt.loads(self.coords)


def edr_query_params(
    coords: str = Query(
        ..., title="WKT Coordinates", description="Well Known Text Coordinates"
    ),
    z: Optional[str] = None,
    datetime: Optional[str] = None,
    parameters: Optional[str] = None,
    crs: Optional[str] = None,
    f: Optional[str] = None,
):
    return EDRQuery(
        coords=coords, z=z, datetime=datetime, parameters=parameters, crs=crs, f=f
    )


# POINT(-69.35 43.72)


@edrrouter.get("/position")
def get_position(
    query: EDRQuery = Depends(edr_query_params),
    dataset: xr.Dataset = Depends(get_dataset),
):
    ds = dataset.cf.sel(X=query.point.x, Y=query.point.y, method="nearest")

    if query.parameters:
        ds = ds[query.parameters.split(",")]

    return to_covjson(ds)


def to_covjson(ds: xr.Dataset):
    covjson = {
        "type": "Coverage",
        "domainType": "Grid",
        "axes": {},
        "parameters": {},
        "ranges": {},
    }

    for var in ds.variables:
        if var not in ds.coords:
            da = ds[var]

            parameter = {"type": "Parameter"}

            covjson["parameters"][var] = parameter

            cov_range = {
                "type": "NdArray",
                "dataType": str(da.dtype),
                # "axisNames": ds[var].dims,
                "values": da.values.ravel().tolist(),
            }

            covjson["ranges"][var] = cov_range

    return covjson


image_router = APIRouter()

@image_router.get('/image', response_class=Response)
async def get_image(bbox: str, width: int, height: int, var: str, dataset: xr.Dataset = Depends(get_dataset)):
    xmin, ymin, xmax, ymax = [float(x) for x in bbox.split(',')]
    q = ds.sel({'latitude': slice(ymin, ymax), 'longitude': slice(xmin, xmax)})

    resampled_data = q[var][0][0].rio.reproject(
        ds.rio.crs, 
        shape=(width, height), 
        resampling=Resampling.bilinear,
    )

    # This is autoscaling, we can add more params to make this user controlled 
    min_value = resampled_data.min()
    max_value = resampled_data.max()

    ds_scaled = (resampled_data - min_value) / (max_value - min_value)
    # TODO: Let user pick cm 
    im = Image.fromarray(np.uint8(cm.gist_earth(ds_scaled)*255))

    image_bytes = io.BytesIO()
    im.save(image_bytes, format='PNG')
    image_bytes = image_bytes.getvalue()

    return FastApiResponse(content=image_bytes, media_type='image/png')


# router order is important
rest_collection = xpublish.Rest(
    {"ww3": ds, "bio": ds}, routers=[base_router, edrrouter, meanrouter, image_router, zarr_router]
)
rest_collection.serve(log_level="trace", port=9005)
