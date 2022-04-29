"""
OpenDAP router
"""
import logging
import urllib

import cachey
from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import StreamingResponse
import numpy as np
import opendap_protocol as dap
import xarray as xr
from xpublish.dependencies import get_cache, get_dataset


logger = logging.getLogger("uvicorn")


dap_router = APIRouter()


dtype_dap = {
    np.ubyte: dap.Byte,
    np.int16: dap.Int16,
    np.uint16: dap.UInt16,
    np.int32: dap.Int32,
    np.uint32: dap.UInt32,
    np.float32: dap.Float32,
    np.float64: dap.Float64,
    np.str_: dap.String,
    # Not a direct mapping
    np.int64: dap.Float64,
}
dtype_dap = {np.dtype(k): v for k, v in dtype_dap.items()}


def dap_dtype(da: xr.DataArray):
    """ Return a DAP type for the xr.DataArray """
    try:
        return dtype_dap[da.dtype]
    except KeyError as e:
        logger.warning(
            f"Unable to match dtype for {da.name}. Going to assume string will work for now... ({e})"
        )
        return dap.String


def dap_dimension(da: xr.DataArray) -> dap.Array:
    """ Transform an xarray dimension into a DAP dimension """
    encoded_da = xr.conventions.encode_cf_variable(da)
    dim = dap.Array(name=da.name, data=encoded_da.values, dtype=dap_dtype(encoded_da))

    for k, v in encoded_da.attrs.items():
        dim.append(dap.Attribute(name=k, value=v, dtype=dap.String))

    return dim


def dap_grid(da: xr.DataArray, dims: dict[str, dap.Array]) -> dap.Grid:
    """ Transform an xarray DataArray into a DAP Grid"""
    data_array = dap.Grid(
        name=da.name,
        data=da.astype(da.encoding["dtype"]).data,
        dtype=dap_dtype(da),
        dimensions=[dims[dim] for dim in da.dims],
    )

    for k, v in da.attrs.items():
        data_array.append(dap.Attribute(name=k, value=v, dtype=dap.String))

    return data_array


def dap_dataset(ds: xr.Dataset, name: str) -> dap.Dataset:
    """ Create a DAP Dataset for an xarray Dataset """
    dataset = dap.Dataset(name=name)

    dims = {}
    for dim in ds.dims:
        dims[dim] = dap_dimension(ds[dim])

    dataset.append(*dims.values())

    for var in ds.variables:
        if var not in ds.dims:
            data_array = dap_grid(ds[var], dims)
            dataset.append(data_array)

    for k, v in ds.attrs.items():
        dataset.append(dap.Attribute(name=k, value=v, dtype=dap.String))

    return dataset


def get_dap_dataset(
    dataset_id: str,
    ds: xr.Dataset = Depends(get_dataset),
    cache: cachey.Cache = Depends(get_cache),
):
    cache_key = f"opendap_dataset_{dataset_id}"
    dataset = cache.get(cache_key)

    if dataset is None:
        dataset = dap_dataset(ds, dataset_id)

        cache.put(cache_key, dataset, 99999)

    return dataset


@dap_router.get(".dds")
def dds_response(request: Request, dataset: dap.Dataset = Depends(get_dap_dataset)):
    constraint = request.url.components[3]
    return StreamingResponse(
        dataset.dds(constraint=constraint), media_type="text/plain"
    )


@dap_router.get(".das")
def das_response(request: Request, dataset: dap.Dataset = Depends(get_dap_dataset)):
    constraint = request.url.components[3]
    return StreamingResponse(
        dataset.das(constraint=constraint), media_type="text/plain"
    )


@dap_router.get(".dods")
def dods_response(request: Request, dataset: dap.Dataset = Depends(get_dap_dataset)):
    constraint = request.url.components[3]
    return StreamingResponse(
        dataset.dods(constraint=constraint), media_type="application/octet-stream"
    )
