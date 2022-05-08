from cmath import isnan
import io
import logging
import xml.etree.ElementTree as ET

import numpy as np
import xarray as xr
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from xpublish.dependencies import get_dataset
from rasterio.enums import Resampling
from rasterio.transform import Affine
from rasterio.warp import calculate_default_transform
from PIL import Image
from matplotlib import cm, colorbar
import matplotlib.pyplot as plt


# These will show as unused to the linter but they are necessary
import cf_xarray
import rioxarray


logger = logging.getLogger("api")

wms_router = APIRouter()


styles = [
    {
        'name': 'raster/default',
        'title': 'Raster',
        'abstract': 'The default raster styling, scaled to the given range. The palette can be overriden by replacing default with a matplotlib colormap name'
    }
]


def lower_case_keys(d: dict) -> dict:
    return dict((k.lower(), v) for k,v in d.items())


def format_timestamp(value):
    return str(value.dt.strftime(date_format='%Y-%m-%dT%H:%M:%S').values)


def strip_float(value): 
    return float(value.values)


def round_float_values(v: list) -> list:
    return [round(x, 5) for x in v]


def create_text_element(root, name: str, text: str):
    element = ET.SubElement(root, name)
    element.text = text
    return element


def create_capability_element(root, name: str, url: str, formats: list[str]):
    cap = ET.SubElement(root, name)
    # TODO: Add more image formats
    for fmt in formats:
        create_text_element(cap, 'Format', fmt)

    dcp_type = ET.SubElement(cap, 'DCPType')
    http = ET.SubElement(dcp_type, 'HTTP')
    get = ET.SubElement(http, 'Get')
    get.append(ET.Element('OnlineResource', attrib={
               'xlink:type': 'simple', 'xlink:href': url}))
    return cap


def get_capabilities(dataset: xr.Dataset, request: Request):
    """
    Return the WMS capabilities for the dataset
    """
    wms_url = f'{request.base_url}{request.url.path.removeprefix("/")}'

    root = ET.Element('WMS_Capabilities', version='1.3.0', attrib={
                      'xmlns': 'http://www.opengis.net/wms', 'xmlns:xlink': 'http://www.w3.org/1999/xlink'})

    service = ET.SubElement(root, 'Service')
    create_text_element(service, 'Name', 'WMS')
    create_text_element(service, 'Title', 'IOOS XPublish WMS')
    create_text_element(service, 'Abstract', 'IOOS XPublish WMS')
    service.append(ET.Element('KeywordList'))
    service.append(ET.Element('OnlineResource', attrib={
                   'xlink:type': 'simple', 'xlink:href': 'http://www.opengis.net/spec/wms_schema_1/1.3.0'}))

    capability = ET.SubElement(root, 'Capability')
    request_tag = ET.SubElement(capability, 'Request')

    get_capabilities = create_capability_element(
        request_tag, 'GetCapabilities', wms_url, ['text/xml'])
    # TODO: Add more image formats
    get_map = create_capability_element(
        request_tag, 'GetMap', wms_url, ['image/png'])
    # TODO: Add more feature info formats
    get_feature_info = create_capability_element(
        request_tag, 'GetFeatureInfo', wms_url, ['text/json'])
    # TODO: Add more image formats
    get_legend_graphic = create_capability_element(
        request_tag, 'GetLegendGraphic', wms_url, ['image/png'])

    exeption_tag = ET.SubElement(capability, 'Exception')
    exception_format = ET.SubElement(exeption_tag, 'Format')
    exception_format.text = 'text/json'

    layer_tag = ET.SubElement(capability, 'Layer')
    create_text_element(layer_tag, 'Title',
                        dataset.attrs.get('title', 'Untitled'))
    create_text_element(layer_tag, 'Description',
                        dataset.attrs.get('description', 'No Description'))
    create_text_element(layer_tag, 'CRS', 'EPSG:4326')
    create_text_element(layer_tag, 'CRS', 'EPSG:3857')
    create_text_element(layer_tag, 'CRS', 'CRS:84')

    for var in dataset.data_vars:
        da = dataset[var]
        attrs = da.cf.attrs
        layer = ET.SubElement(layer_tag, 'Layer', attrib={'queryable': '1'})
        create_text_element(layer, 'Name', var)
        create_text_element(layer, 'Title', attrs['long_name'])
        create_text_element(layer, 'Abstract', attrs['long_name'])
        create_text_element(layer, 'CRS', 'EPSG:4326')
        create_text_element(layer, 'CRS', 'EPSG:3857')
        create_text_element(layer, 'CRS', 'CRS:84')

        create_text_element(layer, 'Units', attrs.get('units', ''))

        # Not sure if this can be copied, its possible variables have different extents within
        # a given dataset probably
        bounding_box_element = ET.SubElement(layer, 'BoundingBox', attrib={
            'CRS': 'EPSG:4326',
            'minx': f'{da["longitude"].min().item()}',
            'miny': f'{da["latitude"].min().item()}',
            'maxx': f'{da["longitude"].max().item()}',
            'maxy': f'{da["latitude"].max().item()}'
        })

        time_dimension_element = ET.SubElement(layer, 'Dimension', attrib={
            'name': 'time',
            'units': 'ISO8601',
            'default': format_timestamp(da.cf['time'].min()),
        })
        # TODO: Add ISO duration specifier
        time_dimension_element.text = f"{format_timestamp(da.cf['time'].min())}/{format_timestamp(da.cf['time'].max())}"

        style_tag = ET.SubElement(layer, 'Style')

        for style in styles:
            style_element = ET.SubElement(
                style_tag, 'Style', attrib={'name': style['name']})
            create_text_element(style_element, 'Title', style['title'])
            create_text_element(style_element, 'Abstract', style['abstract'])

            legend_url = f'{wms_url}?service=WMS&request=GetLegendGraphic&format=image/png&width=20&height=20&layers={var}&styles={style["name"]}'
            create_text_element(style_element, 'LegendURL', legend_url)

    ET.indent(root, space="\t", level=0)
    return Response(ET.tostring(root).decode('utf-8'), media_type='text/xml')


def get_map(dataset: xr.Dataset, query: dict):
    """
    Return the WMS map for the dataset and given parameters
    """
    if not dataset.rio.crs:
        dataset = dataset.rio.write_crs(4326)

    ds = dataset.squeeze()
    bbox = [float(x) for x in query['bbox'].split(',')]
    width = int(query['width'])
    height = int(query['height'])
    crs = query.get('crs', None) or query.get('srs')
    parameter = query['layers']
    t = query.get('time')
    colorscalerange = [float(x) for x in query['colorscalerange'].split(',')]
    autoscale = query.get('autoscale', 'false') != 'false'
    style = query['styles']
    stylename, palettename = style.split('/')

    x_tile_size = bbox[2] - bbox[0]
    y_tile_size = bbox[3] - bbox[1]
    x_resolution = x_tile_size / float(width)
    y_resolution = y_tile_size / float(height)

    # TODO: Calculate the transform
    transform = Affine.translation(
        bbox[0], bbox[3]) * Affine.scale(x_resolution, -y_resolution)

    resampled_data = ds[parameter].rio.reproject(
        crs,
        shape=(width, height),
        resampling=Resampling.bilinear,
        transform=transform,
    )

    # This is an image, so only use the timestep that was requested
    resampled_data = resampled_data.cf.sel({'T': t}).squeeze()

    # if the user has supplied a color range, use it. Otherwise autoscale
    if autoscale:
        min_value = float(ds[parameter].min())
        max_value = float(ds[parameter].max())
    else:
        min_value = colorscalerange[0]
        max_value = colorscalerange[1]

    ds_scaled = (resampled_data - min_value) / (max_value - min_value)

    # Let user pick cm from here https://predictablynoisy.com/matplotlib/gallery/color/colormap_reference.html#sphx-glr-gallery-color-colormap-reference-py
    # Otherwise default to rainbow
    if palettename == 'default':
        palettename = 'rainbow'
    im = Image.fromarray(np.uint8(cm.get_cmap(palettename)(ds_scaled)*255))

    image_bytes = io.BytesIO()
    im.save(image_bytes, format='PNG')
    image_bytes = image_bytes.getvalue()

    return Response(content=image_bytes, media_type='image/png')


def get_feature_info(dataset: xr.Dataset, query: dict):
    """
    Return the WMS feature info for the dataset and given parameters
    """
    if not dataset.rio.crs:
        dataset = dataset.rio.write_crs(4326)

    ds = dataset.squeeze()

    parameters = query['query_layers'].split(',')
    times = [t.replace('Z', '') for t in query['time'].split('/')]
    crs = query.get('crs', None) or query.get('srs')
    bbox = [float(x) for x in query['bbox'].split(',')]
    width = int(query['width'])
    height = int(query['height'])
    x = int(query['x'])
    y = int(query['y'])
    format = query['info_format']

    x_tile_size = bbox[2] - bbox[0]
    y_tile_size = bbox[3] - bbox[1]
    x_resolution = x_tile_size / float(width)
    y_resolution = y_tile_size / float(height)

    # TODO: Calculate the transform
    transform = Affine.translation(
        bbox[0], bbox[3]) * Affine.scale(x_resolution, -y_resolution)

    if len(times) == 1:
        ds = ds.cf.sel({'T': times[0]}).squeeze()
    elif len(times) > 1: 
        ds = ds.cf.sel({'T': slice(times[0], times[1])}).squeeze()
    else: 
        raise HTTPException(500, f"Invalid time requested: {times}")
        
    resampled_data = ds.rio.reproject(
        crs,
        shape=(width, height),
        resampling=Resampling.nearest,
        transform=transform,
    )

    t_axis = [format_timestamp(t) for t in resampled_data.cf['T']]
    x_axis = [strip_float(resampled_data.cf['X'][x])]
    y_axis = [strip_float(resampled_data.cf['Y'][y])]

    parameter_info = {}
    ranges = {}

    for parameter in parameters:
        parameter_info[parameter] = {
            'type': 'Parameter', 
            'description': {
                'en': ds[parameter].cf.attrs['long_name'],
            },
            'observedProperty': {
                'label': {
                    'en': ds[parameter].cf.attrs['long_name'],
                }, 
                'id': ds[parameter].cf.attrs['standard_name'],
            }
        }

        ranges[parameter] = {
            'type': 'NdArray',
            'dataType': 'float',
            # TODO: Some fields might not have a time field? 
            'axisNames': ['t', 'x', 'y'],
            'shape': [len(t_axis), len(x_axis), len(y_axis)],
            'values': round_float_values(resampled_data[parameter].cf.sel({'X': x_axis, 'Y': y_axis}).squeeze().values.tolist()),
        }

    return {
        'type': 'Coverage',
        'title': {
            'en': 'Extracted Profile Feature',
        },
        'domain': {
            'type': 'Domain',
            'domainType': 'PointSeries',
            'axes': {
                't': {
                    'values': t_axis
                },
                'x': {
                    'values': x_axis
                },
                'y': {
                    'values': y_axis
                }
            },
            'referencing': [
                {
                    'coordinates': ['t'], 
                    'system': {
                        'type': 'TemporalRS', 
                        'calendar': 'gregorian',
                    }
                },
                {
                    'coordinates': ['x', 'y'],
                    'system': {
                        'type': 'GeographicCRS',
                        'id': crs,
                    }
                }
            ],
        },
        'parameters': parameter_info,
        'ranges': ranges
    }


def get_legend_info(dataset: xr.Dataset, query: dict):
    """
    Return the WMS legend graphic for the dataset and given parameters
    """
    parameter = query['layers']
    width: int = int(query['width'])
    height: int = int(query['height'])
    vertical = query.get('vertical', 'false') == 'true'
    colorbaronly = query.get('colorbaronly', 'False') == 'True'
    colorscalerange = [float(x) for x in query.get('colorscalerange', 'nan,nan').split(',')]
    if isnan(colorscalerange[0]):
        autoscale = True
    else: 
        autoscale = query.get('autoscale', 'false') != 'false'
    style = query['styles']
    stylename, palettename = style.split('/')

    ds = dataset.squeeze()

    # if the user has supplied a color range, use it. Otherwise autoscale
    if autoscale:
        min_value = float(ds[parameter].min())
        max_value = float(ds[parameter].max())
    else:
        min_value = colorscalerange[0]
        max_value = colorscalerange[1]

    scaled = (np.linspace(min_value, max_value, width) - min_value) / (max_value - min_value)
    data = np.ones((height, width)) * scaled

    if vertical:
        data = np.flipud(data.T)
        data = data.reshape((height, width))

    # Let user pick cm from here https://predictablynoisy.com/matplotlib/gallery/color/colormap_reference.html#sphx-glr-gallery-color-colormap-reference-py
    # Otherwise default to rainbow
    if palettename == 'default':
        palettename = 'rainbow'
    im = Image.fromarray(np.uint8(cm.get_cmap(palettename)(data)*255))

    image_bytes = io.BytesIO()
    im.save(image_bytes, format='PNG')
    image_bytes = image_bytes.getvalue()

    return Response(content=image_bytes, media_type='image/png')


@wms_router.get('/')
def wms_root(request: Request, dataset: xr.Dataset = Depends(get_dataset)):
    query_params = lower_case_keys(request.query_params)
    method = query_params['request']
    if method == 'GetCapabilities':
        return get_capabilities(dataset, request)
    elif method == 'GetMap':
        return get_map(dataset, query_params)
    elif method == 'GetFeatureInfo' or method == 'GetTimeseries':
        return get_feature_info(dataset, query_params)
    elif method == 'GetLegendGraphic':
        return get_legend_info(dataset, query_params)
    else:
        raise HTTPException(
            status_code=404, detail=f"{method} is not a valid option for REQUEST")
