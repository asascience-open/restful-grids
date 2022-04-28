"""
OGC EDR router for datasets with CF convention metadata
"""
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Optional

from fastapi import APIRouter, Depends, Response, Query, Request, HTTPException
import numpy as np
from pydantic import BaseModel, Field
import xarray as xr
from xpublish.dependencies import get_dataset


logger = logging.getLogger("uvicorn")

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
    z: Optional[str] = Query(
        None, title="Z axis", description="Height or depth of query"
    ),
    datetime: Optional[str] = Query(
        None,
        title="Datetime or datetime range",
        description="Query by a single ISO time or a range of ISO times. To query by a range, split the times with a slash",
    ),
    parameters: Optional[str] = Query(
        None, alias="parameter-name", description="xarray variables to query"
    ),
    crs: Optional[str] = Query(
        None, deprecated=True, description="CRS is not yet implemented"
    ),
    f: Optional[str] = Query(
        None,
        title="Response format",
        description="Data is returned as a CoverageJSON by default, but NetCDF is supported with `f=nc`",
    ),
):
    return EDRQuery(
        coords=coords, z=z, datetime=datetime, parameters=parameters, crs=crs, format=f
    )


edr_query_params = set(["coords", "z", "datetime", "parameter-name", "crs", "f"])


@edr_router.get("/position", summary="Position query")
def get_position(
    request: Request,
    query: EDRQuery = Depends(edr_query),
    dataset: xr.Dataset = Depends(get_dataset),
):
    """
    Return position data based on WKT Point(lon lat) coordinate.

    Extra selecting/slicing parameters can be provided as additional query strings.
    """
    try:
        ds = dataset.cf.sel(X=query.point.x, Y=query.point.y, method="nearest")
    except KeyError:
        raise HTTPException(
            status_code=404,
            detail="Dataset does not have CF Convention compliant metadata",
        )

    if query.z:
        ds = dataset.cf.sel(Z=query.z, method="nearest")

    if query.datetime:
        datetimes = query.datetime.split("/")

        try:
            if len(datetimes) == 1:
                ds = ds.cf.sel(T=datetimes[0], method="nearest")
            elif len(datetimes) == 2:
                ds = ds.cf.sel(T=slice(datetimes[0], datetimes[1]))
            else:
                raise HTTPException(
                    status_code=404, detail="Invalid datetimes submitted"
                )
        except ValueError as e:
            logger.error("Error with datetime", exc_info=1)
            raise HTTPException(
                status_code=404, detail=f"Invalid datetime ({e})"
            ) from e

    if query.parameters:
        try:
            ds = ds.cf[query.parameters.split(",")]
        except KeyError as e:
            raise HTTPException(status_code=404, detail=f"Invalid variable: {e}")

        logger.debug(f"Dataset filtered by query params {ds}")

    query_params = dict(request.query_params)
    for query_param in request.query_params:
        if query_param in edr_query_params:
            del query_params[query_param]

    method = "nearest"

    for key, value in query_params.items():
        split_value = value.split("/")
        if len(split_value) == 1:
            continue
        elif len(split_value) == 2:
            query_params[key] = slice(split_value[0], split_value[1])
            method = None
        else:
            raise HTTPException(404, f"Too many values for selecting {key}")

    ds = ds.sel(query_params, method=method)

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
        "domain": {
            "type": "Domain",
            "domainType": "Grid",
            "axes": {},
            "referencing": [],
        },
        "parameters": {},
        "ranges": {},
    }

    inverted_dims = invert_cf_dims(ds)

    for name, da in ds.coords.items():
        if "datetime" in str(da.dtype):
            values = da.dt.strftime("%Y-%m-%dT%H:%M:%S%Z").values.tolist()
        else:
            values = da.values
            values = np.where(np.isnan(values), None, values).tolist()
        try:
            if not isinstance(values, list):
                values = [values]
            covjson["domain"]["axes"][inverted_dims.get(name, name)] = {
                "values": values
            }
        except (ValueError, TypeError):
            pass

    for var in ds.variables:
        if var not in ds.coords:
            da = ds[var]

            parameter = {"type": "Parameter", "observedProperty": {}}

            try:
                parameter["description"] = {"en": da.attrs["long_name"]}
                parameter["observedProperty"]["label"] = {"en": da.attrs["long_name"]}
            except KeyError:
                pass

            try:
                parameter["unit"] = {"label": {"en": da.attrs["units"]}}
            except KeyError:
                pass

            covjson["parameters"][var] = parameter

            values = da.values.ravel()
            if "datetime" in str(da.dtype):
                values = da.dt.strftime("%Y-%m-%dT%H:%M:%S%Z").values.tolist()
                dataType = "string"
            else:
                values = np.where(np.isnan(values), None, values).tolist()

                if da.dtype.kind in ("i", "u"):
                    values = [int(v) for v in values]
                    dataType = "integer"
                elif da.dtype.kind in ("f", "c"):
                    dataType = "float"
                else:
                    dataType = "string"

            cov_range = {
                "type": "NdArray",
                "dataType": dataType,
                "axisNames": [inverted_dims.get(dim, dim) for dim in da.dims],
                "shape": da.shape,
                "values": values,
            }

            covjson["ranges"][var] = cov_range

    return covjson


def invert_cf_dims(ds):
    inverted = {}
    for key, values in ds.cf.axes.items():
        for value in values:
            inverted[value] = key.lower()
    return inverted
