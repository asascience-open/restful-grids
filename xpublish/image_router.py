import io
import logging
from typing import Optional

import numpy as np
import cf_xarray
from pydantic import BaseModel, Field
import pyproj
import xarray as xr
from xpublish.dependencies import get_dataset
from fastapi import APIRouter, Depends, Response
from rasterio.enums import Resampling
from PIL import Image
from matplotlib import cm
from pyproj import CRS, Transformer

# rioxarray will show as not being used but its necesary for enabling rio extensions for xarray
import rioxarray


logger = logging.getLogger("api")
logger.setLevel(logging.DEBUG)

image_router = APIRouter()


class ImageQuery(BaseModel):
    bbox: str = Field(..., title="Bbox in xmin,ymin,xmax,ymax format")
    width: int = Field(..., title="Output image width in pixels")
    height: int = Field(..., title="Output image height in pixels")
    parameter: str = Field(..., title="Parameter to map")
    datetime: str = Field(..., title="The datestamp of the map image to get")
    crs: str = Field(..., title="CRS of the requested bbox and resulting image")
    cmap: Optional[str] = None


def image_query(
    bbox: str, 
    width: int, 
    height: int, 
    parameter: str, 
    datetime: str,
    crs: str = None,
    cmap: Optional[str] = None):
    return ImageQuery(bbox=bbox, width=width, height=height, parameter=parameter, cmap=cmap, datetime=datetime, crs=crs)


@image_router.get('/', response_class=Response)
async def get_image(query: ImageQuery = Depends(image_query), dataset: xr.Dataset = Depends(get_dataset)):
    xmin, ymin, xmax, ymax = [float(x) for x in query.bbox.split(',')]

    if query.crs != "EPSG:4326":
        transformer = Transformer.from_crs(query.crs, "EPSG:4326")
        min_coord = transformer.transform(xmin, ymin)
        max_coord = transformer.transform(xmax, ymax)
    else: 
        min_coord = [ymin, xmin]
        max_coord = [ymax, xmax]

    logger.warning(min_coord)
    logger.warning(max_coord)
    q = dataset.cf.sel({'X' : slice(min_coord[1], max_coord[1]), 'Y': slice(min_coord[0], max_coord[0]), 'T': query.datetime }).squeeze()

    # Hack, do everything via cf
    if not q.rio.crs:
        q = q.rio.write_crs(4326)    

    # CRS is hard coded for now, to avoid dealing with reprojecting before slicing, 
    # TODO: Full reprojection handling 
    resampled_data = q[query.parameter].rio.reproject(
        query.crs,
        shape=(query.width, query.height), 
        resampling=Resampling.bilinear,
    )

    # This is autoscaling, we can add more params to make this user controlled 
    # if not min_value: 
    min_value = resampled_data.min()
    # if not max_value:
    max_value = resampled_data.max()

    ds_scaled = (resampled_data - min_value) / (max_value - min_value)

    # Let user pick cm from here https://predictablynoisy.com/matplotlib/gallery/color/colormap_reference.html#sphx-glr-gallery-color-colormap-reference-py
    # Otherwise default to rainbow
    if not query.cmap:
        query.cmap = 'rainbow'
    im = Image.fromarray(np.uint8(cm.get_cmap(query.cmap)(ds_scaled)*255))

    image_bytes = io.BytesIO()
    im.save(image_bytes, format='PNG')
    image_bytes = image_bytes.getvalue()

    return Response(content=image_bytes, media_type='image/png')