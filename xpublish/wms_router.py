
from fastapi import APIRouter, Depends, HTTPException, Request
from xpublish.dependencies import get_dataset
import xarray as xr
import xml.etree.ElementTree as ET


wms_router = APIRouter()


def get_capabilities(dataset: xr.Dataset):
    """
    Return the WMS capabilities for the dataset
    """

    return ''


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
        return get_capabilities(dataset)
    elif method == 'GetMap':
        return get_map(dataset, query_params)
    elif method == 'GetFeatureInfo':
        return get_feature_info(dataset, query_params)
    elif method == 'GetLegendGraphic':
        return get_legend_graphic(dataset, query_params)
    else: 
        raise HTTPException(status_code=404, detail=f"{method} is not a valid option for REQUEST")
