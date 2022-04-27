from logging import getLogger
import logging
from typing import Optional

from fastapi import APIRouter, Body, Depends, HTTPException, Query
from pydantic import BaseModel, Field
import xarray as xr
import cf_xarray as cfxr
import xpublish
from xpublish.dependencies import get_dataset
from xpublish.routers import base_router, zarr_router

# logger = logging.getLogger(__name__)
logger = logging.getLogger("fastapi")

ds = xr.open_dataset("../datasets/ww3_72_east_coast_2022041112.nc")

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
        "domain": {"axes": {}},
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
                "axisNames": da.dims,
                "shape": da.shape,
                "values": da.values.ravel().tolist(),
            }

            covjson["ranges"][var] = cov_range

    return covjson


# router order is important
rest_collection = xpublish.Rest(
    {"ww3": ds, "bio": ds}, routers=[base_router, edrrouter, meanrouter, zarr_router]
)
rest_collection.serve(log_level="trace", port=9005)
