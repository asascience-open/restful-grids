import io
import logging
from typing import Optional

import numpy as np
import cf_xarray
import xarray as xr
from xpublish.dependencies import get_dataset
from fastapi import APIRouter, Depends, Response
from rasterio.enums import Resampling
from PIL import Image
from matplotlib import cm

# rioxarray will show as not being used but its necesary for enabling rio extensions for xarray
import rioxarray


image_router = APIRouter()

@image_router.get('/', response_class=Response)
async def get_image(bbox: str, width: int, height: int, var: str, cmap: Optional[str]=None, dataset: xr.Dataset = Depends(get_dataset)):
    xmin, ymin, xmax, ymax = [float(x) for x in bbox.split(',')]
    q = dataset.sel({'latitude': slice(ymin, ymax), 'longitude': slice(xmin, xmax)})

    # Hack, do everything via cf
    if not q.rio.crs:
        q = q.rio.write_crs(4326)

    resampled_data = q[var][0][0].rio.reproject(
        "EPSG:4326",
        shape=(width, height), 
        resampling=Resampling.cubic,
    )

    # This is autoscaling, we can add more params to make this user controlled 
    # if not min_value: 
    min_value = resampled_data.min()
    # if not max_value:
    max_value = resampled_data.max()

    ds_scaled = (resampled_data - min_value) / (max_value - min_value)

    # Let user pick cm from here https://predictablynoisy.com/matplotlib/gallery/color/colormap_reference.html#sphx-glr-gallery-color-colormap-reference-py
    # Otherwise default to rainbow
    if not cmap:
        cmap = 'rainbow'
    im = Image.fromarray(np.uint8(cm.get_cmap(cmap)(ds_scaled)*255))

    image_bytes = io.BytesIO()
    im.save(image_bytes, format='PNG')
    image_bytes = image_bytes.getvalue()

    return Response(content=image_bytes, media_type='image/png')