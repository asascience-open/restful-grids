from typing import Optional

from fastapi import APIRouter, Depends
import xarray as xr
from xpublish.dependencies import get_dataset


tree_router = APIRouter()


@tree_router.get("/.zmetadata")
def get_tree_metadata(dataset: xr.Dataset = Depends(get_dataset)):
    pass


@tree_router.get("/.zgroup")
def get_top_zgroup(dataset: xr.Dataset = Depends(get_dataset)):
    pass


@tree_router.get("/.zattrs")
def get_top_zattrs(dataset: xr.Dataset = Depends(get_dataset)):
    pass


@tree_router.get("/{level}/.zgroup")
def get_zgroup(level: int, dataset: xr.Dataset = Depends(get_dataset)):
    pass


@tree_router.get("/{level}/{var_name}/.zarray")
def get_variable_zarray(
    level: int, var_name: str, dataset: xr.Dataset = Depends(get_dataset)
):
    pass


@tree_router.get("/{level}/{var_name}/{chunk}")
def get_variable_chunk(
    level: int, var_name: str, chunk: str, dataset: xr.Dataset = Depends(get_dataset)
):
    pass
