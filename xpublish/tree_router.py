from typing import Optional

import cachey
from fastapi import APIRouter, Depends, Response
import mercantile
from ndpyramid.utils import (
    add_metadata_and_zarr_encoding,
    get_version,
    multiscales_template,
)
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


def cache_key_for(ds: xr.Dataset, key: str):
    return ds.attrs.get(DATASET_ID_ATTR_KEY, "") + f"-tree/{key}" 

def cache_key_for_level(ds: xr.Dataset, key: str, level: int):
    return ds.attrs.get(DATASET_ID_ATTR_KEY, "") + f"-tree/{level}/{key}"


def extract_zarray(da: xr.DataArray, encoding: dict, dtype: np.dtype, level: int, tile_size: int):
    """ helper function to extract zarr array metadata. """

    pixels_per_tile = tile_size
    tile_count = 2 ** level
    pixel_count = tile_count * pixels_per_tile
    
    data_shape = list(da.shape)
    data_shape[-2:] = [pixel_count, pixel_count]
    
    chunk_shape = list(da.shape)
    chunk_shape[-2:] = [pixels_per_tile, pixels_per_tile]

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

def create_tree_metadata(levels: list[int, int], tile_size: int, dataset: xr.Dataset):
    save_kwargs = {"levels": range(levels[0], levels[1]), "tile_size": tile_size}
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


def get_levels(levels: str = '0,30'):
    """
    Extracts the levels from a {min}/{max}}
    """
    return [int(l) for l in levels.split(',')]


def get_tile_size(tile_size: int = 256):
    """
    Common dependency for the tile size in pixels 
    """
    return tile_size

def get_tree_metadata(
    levels: int = Depends(get_levels),
    tile_size: int = Depends(get_tile_size),
    dataset: xr.Dataset = Depends(get_dataset),
    cache: cachey.Cache = Depends(get_cache),
):
    cache_key = cache_key_for(dataset, zarr_metadata_key)
    metadata = cache.get(cache_key)

    if metadata is None:
        metadata = create_tree_metadata(levels, tile_size, dataset)

        cache.put(cache_key, metadata, 99999)

    return metadata

def get_variable_zarray(level: int, var_name: str, tile_size: int = Depends(get_tile_size), ds: xr.Dataset = Depends(get_dataset), cache: cachey.Cache = Depends(get_cache)):
    """
    Returns the zarray metadata for a given level and dataarray.
    """
    da = ds[var_name]
    encoded_da = encode_zarr_variable(da, name=var_name)
    encoding = extract_zarr_variable_encoding(da)

    array_metadata = extract_zarray(encoded_da, encoding, encoded_da.dtype, level, tile_size)

    # convert compressor to dict
    compressor = array_metadata['compressor']
    if compressor is not None:
        compressor_config = array_metadata['compressor'].get_config()
        array_metadata['compressor'] = compressor_config

    return array_metadata


@tree_router.get("/{levels}/{tile_size}/.zmetadata")
def get_tree_metadata(metadata: dict = Depends(get_tree_metadata)):
    return metadata


@tree_router.get("/{levels}/{tile_size}/.zgroup")
def get_top_zgroup(metadata: dict = Depends(get_tree_metadata)):
    return metadata["metadata"][".zgroup"]


@tree_router.get("/{levels}/{tile_size}/.zattrs")
def get_top_zattrs(levels: int = Depends(get_levels), tile_size: int = Depends(get_tile_size)):
    return {
        "multiscales": multiscales_template(
            datasets=[{"path": str(i)} for i in range(levels)],
            type="reduce",
            method="pyramid_reproject",
            version=get_version(),
            kwargs={"levels": levels, "tile_size": tile_size},
        )
    }


@tree_router.get("/{levels}/{tile_size}/{level}/.zgroup")
def get_zgroup(level: int):
    return {"zarr_format": 2}


@tree_router.get("/{levels}/{tile_size}/{level}/{var_name}/.zattrs")
def get_variable_zattrs(
    level: int, var_name: str, dataset = Depends(get_dataset)
):
    return _extract_dataarray_zattrs(dataset[var_name])


@tree_router.get("/{levels}/{tile_size}/{level}/{var_name}/.zarray")
def get_variable_zarray(
    zarray: dict = Depends(get_variable_zarray)
):
    return zarray


@tree_router.get("/{levels}/{tile_size}/{level}/{var_name}/{chunk}")
def get_variable_chunk(
    level: int, 
    var_name: str, 
    chunk: str, 
    dataset: xr.Dataset = Depends(get_dataset),
    tile_size: int = Depends(get_tile_size),
):
    if not dataset.rio.crs:
        dataset = dataset.rio.write_crs(4326)
    ds = dataset.squeeze()
    
    # Extract the requested tile metadata
    chunk_coords = [int(i) for i in chunk.split(".")]
    x = chunk_coords[-2]
    y = chunk_coords[-1]
    z = level
    
    bbox = mercantile.xy_bounds(x, y, z)

    dim = (2 ** z) * tile_size
    transform = Affine.translation(bbox.left, bbox.top) * Affine.scale(
       (20037508.342789244 * 2) / float(dim), -(20037508.342789244 * 2) / float(dim)
    )

    resampled_data = ds[var_name].rio.reproject(
        'EPSG:3857', 
        shape=(tile_size, tile_size), 
        resampling=Resampling.cubic, 
        transform=transform,
    )

    resampled_data_array = np.asarray(resampled_data)

    encoded_chunk = encode_chunk(
        resampled_data_array.tobytes(),                     
        filters=resampled_data.encoding.get('filters', None),
        compressor=resampled_data.encoding.get('compressor', default_compressor)
    )
    return Response(encoded_chunk, media_type='application/octet-stream')