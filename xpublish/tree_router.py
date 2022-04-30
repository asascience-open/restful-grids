from typing import Optional

import cachey
import datatree as dt
from fastapi import APIRouter, Depends, Response
import mercantile
from ndpyramid.utils import (
    add_metadata_and_zarr_encoding,
    get_version,
    multiscales_template,
)
from ndpyramid.regrid import make_grid_pyramid, pyramid_regrid
import numpy as np
import xarray as xr
from xarray.backends.zarr import (
    DIMENSION_KEY,
    encode_zarr_attr_value,
    encode_zarr_variable,
    extract_zarr_variable_encoding,
)
from xpublish.dependencies import get_dataset, get_cache
from xpublish.utils.api import DATASET_ID_ATTR_KEY
from xpublish.utils.zarr import (
    jsonify_zmetadata,
    get_data_chunk,
    zarr_metadata_key,
    _extract_dataarray_zattrs,
    _extract_zarray,
    _extract_fill_value,
    encode_chunk
)
from zarr.storage import array_meta_key, attrs_key, default_compressor, group_meta_key
from rasterio.transform import Affine
from rasterio.enums import Resampling


tree_router = APIRouter()


def get_levels():
    """ How many levels / factors should the data tree have """
    return 6


def get_datatree(dataset: xr.Dataset = Depends(get_dataset)):
    pass


def get_pixels_per_tile():
    """ How many pixels should there be per tile """
    return 256


def cache_key_for(ds: xr.Dataset, key: str):
    return ds.attrs.get(DATASET_ID_ATTR_KEY, "") + "-tree/" + key


def extract_zarray(da, encoding, dtype, level):
    """ helper function to extract zarr array metadata. """

    pixels_per_tile = get_pixels_per_tile()
    tile_count = 2 ** level
    
    data_shape = list(da.shape)
    data_shape[-2:] = [pixels_per_tile, pixels_per_tile]
    
    chunk_shape = list(da.shape)
    chunk_shape[-2:] = [tile_count, tile_count]
    chunk_shape[:-2] = [1 for i in range(len(chunk_shape) - 2)]

    meta = {
        'compressor': encoding.get('compressor', da.encoding.get('compressor', default_compressor)),
        'filters': encoding.get('filters', da.encoding.get('filters', None)),
        'chunks': chunk_shape,
        'dtype': dtype.str,
        'fill_value': _extract_fill_value(da, dtype),
        'order': 'C',
        'shape': data_shape,
        'zarr_format': 2,
    }

    if meta['chunks'] is None:
        meta['chunks'] = da.shape

    # # validate chunks
    # if isinstance(da.data, dask_array_type):
    #     var_chunks = tuple([c[0] for c in da.data.chunks])
    # else:
    #     var_chunks = da.shape
    # if not var_chunks == tuple(meta['chunks']):
    #     raise ValueError('Encoding chunks do not match inferred chunks')

    # meta['chunks'] = list(meta['chunks'])  # return chunks as a list

    return meta


def create_tree_metadata(levels: int, pixels_per_tile: int, dataset: xr.Dataset):
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

    metadata = {
        "metadata": {".zattrs": attrs, ".zgroup": {"zarr_format": 2}},
        "zarr_consolidated_format": 1,
    }

    for level in range(levels):
        metadata["metadata"][f"{level}/.zgroup"] = {"zarr_format": 2}

        for key, da in dataset.variables.items():
            # da needs to be resized based on level

            encoded_da = encode_zarr_variable(da, name=key)
            encoding = extract_zarr_variable_encoding(da)
            metadata["metadata"][
                f"{level}/{key}/{attrs_key}"
            ] = _extract_dataarray_zattrs(da)
            metadata["metadata"][f"{level}/{key}/{array_meta_key}"] = extract_zarray(
                encoded_da, encoding, encoded_da.dtype, level
            )

            # convert compressor to dict
            compressor = metadata['metadata'][f'{level}/{key}/{array_meta_key}']['compressor']
            if compressor is not None:
                compressor_config = metadata['metadata'][f'{level}/{key}/{array_meta_key}'][
                    'compressor'
                ].get_config()
                metadata['metadata'][f'{level}/{key}/{array_meta_key}']['compressor'] = compressor_config

    return metadata

def get_tree_metadata(
    levels: int = Depends(get_levels),
    pixels_per_tile: int = Depends(get_pixels_per_tile),
    dataset: xr.Dataset = Depends(get_dataset),
    cache: cachey.Cache = Depends(get_cache),
):

    cache_key = cache_key_for(dataset, zarr_metadata_key)
    metadata = cache.get(cache_key)

    if metadata is None:
        metadata = create_tree_metadata(levels, pixels_per_tile, dataset)

        # cache.put(cache_key, metadata, 99999)

    return metadata


# def get_grid_pyramid(levels: int = Depends(get_levels)) -> dt.DataTree:
#     return make_grid_pyramid(levels)


def get_datatree(
    dataset: xr.Dataset = Depends(get_dataset),
    pixels_per_tile: int = Depends(get_pixels_per_tile),
    cache: cachey.Cache = Depends(get_cache),
) -> dt.DataTree:
    cache_key = cache_key_for(dataset, "datatree")
    dt = cache.get(cache_key)

    if dt is None:
        dt = pyramid_regrid(dataset, levels=levels, pixels_per_tile=pixels_per_tile)

        # cache.put(cache_key, dt, 99999)

    return dt


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
def get_zgroup(level: int, metadata: dict = Depends(get_tree_metadata)):
    return metadata["metadata"][f"{level}/.zgroup"]


@tree_router.get("/{level}/{var_name}/.zattrs")
def get_variable_zattrs(
    level: int, var_name: str, metadata: dict = Depends(get_tree_metadata)
):
    return metadata["metadata"][f"{level}/{var_name}/.zattrs"]

@tree_router.get("/{level}/{var_name}/.zarray")
def get_variable_zarray(
    level: int, var_name: str, metadata: dict = Depends(get_tree_metadata)
):
    return metadata["metadata"][f"{level}/{var_name}/.zarray"]



@tree_router.get("/{level}/{var_name}/{chunk}")
def get_variable_chunk(
    level: int, 
    var_name: str, 
    chunk: str, 
    dataset: xr.Dataset = Depends(get_dataset),
    pixels_per_tile: int = Depends(get_pixels_per_tile)
):
    if not dataset.rio.crs:
        dataset = dataset.rio.write_crs(4326)
    ds = dataset.squeeze()
    
    # Extract the requested tile metadata
    chunk_coords = [int(i) for i in chunk.split(",")]
    x = chunk_coords[-2]
    y = chunk_coords[-1]
    z = level

    # TODO: Get the requested data values
    bbox = mercantile.xy_bounds(x, y, z)

    dim = (2 ** z) * pixels_per_tile
    transform = Affine.translation(bbox.left, bbox.top) * Affine.scale(
       (20037508.342789244 * 2) / float(dim), -(20037508.342789244 * 2) / float(dim)
    )

    resampled_data = ds[var_name].rio.reproject(
        'EPSG:3857', 
        shape=(pixels_per_tile, pixels_per_tile), 
        resampling=Resampling.nearest, 
        transform=transform,
    )

    resampled_data_array = np.asarray(resampled_data)

    # TODO: Encode chunk to zarr chunk
    encoded_chunk = encode_chunk(resampled_data_array.tobytes())
    return Response(encoded_chunk, media_type='application/octet-stream')