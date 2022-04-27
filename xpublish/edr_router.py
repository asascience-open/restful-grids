import logging
from pathlib import Path
from tempfile import TemporaryFile, TemporaryDirectory, tempdir
from typing import Optional

from fastapi import APIRouter, Depends, Response, Query
from fastapi.responses import StreamingResponse, FileResponse
import numpy as np
from pydantic import BaseModel, Field
import xarray as xr
from xpublish.dependencies import get_dataset


logger = logging.getLogger("api")

edr_router = APIRouter()


class EDRQuery(BaseModel):
    coords: str = Field(
        ..., title="Point in WKT format", description="Well Known Text coordinates"
    )
    z: Optional[str] = None
    datetime: Optional[str] = None
    parameters: Optional[str] = None
    crs: Optional[str] = None
    format: Optional[str] = None

    @property
    def point(self):
        from shapely import wkt

        return wkt.loads(self.coords)


def edr_query(
    coords: str = Query(
        ..., title="Point in WKT format", description="Well Known Text coordinates"
    ),
    z: Optional[str] = None,
    datetime: Optional[str] = None,
    parameters: Optional[str] = Query(None, alias="parameter-name"),
    crs: Optional[str] = None,
    f: Optional[str] = None,
):
    return EDRQuery(
        coords=coords, z=z, datetime=datetime, parameters=parameters, crs=crs, format=f
    )


x_axis_names = set(["x", "longitude", "long", "lon"])
y_axis_names = set(["y", "latitude", "lat"])


@edr_router.get("/position")
def get_position(
    query: EDRQuery = Depends(edr_query), dataset: xr.Dataset = Depends(get_dataset)
):
    """
    Return position data based on WKT coordinate. Responses are in CoverageJSON by default, but if
    `f=nc` query param it will return a NetCDF.
    """
    try:
        ds = dataset.cf.sel(X=query.point.x, Y=query.point.y, method="nearest")
    except KeyError:
        logger.warning(
            "Dataset does not have CF Convention compliant metadata, attempting other coord selection methods"
        )
        for coord in dataset.coords.keys():
            if coord.lower() in x_axis_names:
                x_axis = coord
                break
        for coord in dataset.coords.keys():
            if coord.lower() in y_axis_names:
                y_axis = coord
                break

        ds = dataset.sel(
            {x_axis: query.point.x, y_axis: query.point.y}, method="nearest"
        )
        logger.warning(f"Non CF selected dataset: {ds}")

    if query.parameters:
        ds = ds[query.parameters.split(",")]
        logger.warning(f"Dataset selected by query params {ds}")

    if query.format == "nc":
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "position.nc"
            ds.to_netcdf(path)

            with path.open("rb") as f:
                return Response(
                    f.read(),
                    media_type="application/netcdf",
                    headers={
                        "Content-Disposition": 'attachment; filename="position.nc"'
                    },
                )

    return to_covjson(ds)


def to_covjson(ds: xr.Dataset):
    """ Transform an xarray dataset to CoverageJSON """

    covjson = {
        "type": "Coverage",
        "domainType": "Grid",
        "domain": {"axes": {}},
        "parameters": {},
        "ranges": {},
    }

    for name, da in ds.coords.items():
        logger.warning(f"{name} - {da.dtype} - {da.values}")

        if "datetime" in str(da.dtype):
            values = da.dt.strftime("%Y-%m-%dT%H:%M:%SZ").values.tolist()
        else:
            values = da.values
            values = np.where(np.isnan(values), None, values).tolist()
        try:
            if not isinstance(values, list):
                values = [values]
            covjson["domain"]["axes"][name] = {"values": values}
        except (ValueError, TypeError):
            pass

    for var in ds.variables:
        if var not in ds.coords:
            da = ds[var]

            parameter = {"type": "Parameter"}

            covjson["parameters"][var] = parameter

            values = da.values.ravel()
            values = np.where(np.isnan(values), None, values)

            cov_range = {
                "type": "NdArray",
                "dataType": str(da.dtype),
                "axisNames": da.dims,
                "shape": da.shape,
                "values": values.tolist(),
            }

            covjson["ranges"][var] = cov_range

    return covjson
