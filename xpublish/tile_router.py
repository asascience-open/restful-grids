import io
import logging
from typing import Dict, Optional

import numpy as np
import mercantile
import xarray as xr
from xpublish.dependencies import get_dataset
from fastapi import APIRouter, Depends, Response
from rasterio.enums import Resampling
from rasterio.transform import Affine
from PIL import Image
from matplotlib import cm

# rioxarray and cf_xarray will show as not being used but its necesary for enabling rio extensions for xarray
import cf_xarray
import rioxarray


logger = logging.getLogger("api")

tile_router = APIRouter()

@tile_router.get('/{parameter}/{t}/{z}/{x}/{y}', response_class=Response)
def get_image_tile(parameter: str, t: str, z: int, x: int, y: int, size: int = 256, cmap: str = None, color_range: str = None, dataset: xr.Dataset = Depends(get_dataset)):
    if not dataset.rio.crs:
        dataset = dataset.rio.write_crs(4326)
    ds = dataset.squeeze()
    bbox = mercantile.xy_bounds(x, y, z)

    dim = (2 ** z) * size
    transform = Affine.translation(bbox.left, bbox.top) * Affine.scale(
       (20037508.342789244 * 2) / float(dim), -(20037508.342789244 * 2) / float(dim)
    )

    resampled_data = ds[parameter].rio.reproject(
        'EPSG:3857', 
        shape=(size, size), 
        resampling=Resampling.nearest, 
        transform=transform,
    )

    # This is an image, so only use the timestepm that was requested
    resampled_data = resampled_data.cf.sel({'T': t}).squeeze()
    
    # if the user has supplied a color range, use it. Otherwise autoscale
    if color_range is not None:
        color_range = [float(x) for x in color_range.split(',')]
        min_value = color_range[0]
        max_value = color_range[1]
    else:
        min_value = float(ds[parameter].min())
        max_value = float(ds[parameter].max())

    ds_scaled = (resampled_data - min_value) / (max_value - min_value)

    # Let user pick cm from here https://predictablynoisy.com/matplotlib/gallery/color/colormap_reference.html#sphx-glr-gallery-color-colormap-reference-py
    # Otherwise default to rainbow
    im = Image.fromarray(np.uint8(cm.get_cmap(cmap)(ds_scaled)*255))

    image_bytes = io.BytesIO()
    im.save(image_bytes, format='PNG')
    image_bytes = image_bytes.getvalue()

    return Response(content=image_bytes, media_type='image/png')
