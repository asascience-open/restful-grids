from unicodedata import name
import xml.etree.ElementTree as ET

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from mercantile import children
from requests import request
from xpublish.dependencies import get_dataset
import xarray as xr

# These will show as unused to the linter but they are necessary
import cf_xarray
import rioxarray


wms_router = APIRouter()


styles = [
    {
        'name': 'raster/default',
        'title': 'Raster',
        'abstract': 'The default raster styling, scaled to the given range. The palette can be overriden by replacing default with a matplotlib colormap name'
    }
]


def format_timestamp(value):
    return str(value.dt.strftime(date_format='%Y-%m-%dT%H:%M:%S').values)


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

            legend_url = f'{wms_url}?service=WMS&request=GetLegendGraphic&format=image/png&width=20&height=20&layer={var}&style={style["name"]}'
            create_text_element(style_element, 'LegendURL', legend_url)

    ET.indent(root, space="\t", level=0)
    return Response(ET.tostring(root).decode('utf-8'), media_type='text/xml')


def get_map(dataset: xr.Dataset, query: dict):
    """
    Return the WMS map for the dataset and given parameters
    """
    return ''


def get_feature_info(dataset: xr.Dataset, query: dict):
    """
    Return the WMS feature info for the dataset and given parameters
    """
    return ''


def get_legend_graphic(dataset: xr.Dataset, query: dict):
    """
    Return the WMS legend graphic for the dataset and given parameters
    """
    return ''


@wms_router.get('/')
def wms_root(request: Request, dataset: xr.Dataset = Depends(get_dataset)):
    query_params = request.query_params
    method = query_params['request']
    if method == 'GetCapabilities':
        return get_capabilities(dataset, request)
    elif method == 'GetMap':
        return get_map(dataset, query_params)
    elif method == 'GetFeatureInfo':
        return get_feature_info(dataset, query_params)
    elif method == 'GetLegendGraphic':
        return get_legend_graphic(dataset, query_params)
    else:
        raise HTTPException(
            status_code=404, detail=f"{method} is not a valid option for REQUEST")
