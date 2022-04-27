from typing import Optional

from datatree import DataTree
from fastapi import APIRouter, Depends
from ndpyramid.utils import (
    add_metadata_and_zarr_encoding,
    get_version,
    multiscales_template,
)
import xarray as xr
from xpublish.dependencies import get_dataset
from xpublish.utils.zarr import jsonify_zmetadata


tree_router = APIRouter()


def get_levels():
    """ How many levels / factors should the data tree have """
    return 6


def get_datatree(dataset: xr.Dataset = Depends(get_dataset)):
    pass


def get_pixels_per_tile():
    """ How many pixels should there be per tile """
    return 128


def get_tree_metadata(
    levels: int = Depends(get_levels),
    pixels_per_tile: int = Depends(get_pixels_per_tile),
    dataset: xr.Dataset = Depends(get_dataset),
):
    save_kwargs = {"levels": levels, "pixels_per_tile": pixels_per_tile}
    attrs = {
        "multiscales": multiscales_template(
            datasets=[{"path": str(i)} for i in range(levels)],
            type="reduce",
            method="pyramid_reproject",
            version=get_version(),
            kwargs=save_kwargs,
        )
    }

    metadata = {"metadata": {".zattrs": attrs, ".zgroup": {"zarr_format": 2}}}

    return metadata


@tree_router.get("/.zmetadata")
def get_tree_metadata(metadata: dict = Depends(get_tree_metadata)):
    return metadata


@tree_router.get("/.zgroup")
def get_top_zgroup(metadata: dict = Depends(get_tree_metadata)):
    return metadata["metadata"][".zgroup"]


@tree_router.get("/.zattrs")
def get_top_zattrs(metadata: dict = Depends(get_tree_metadata)):
    return metadata["metadata"][".zattrs"]


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
