# Testing accessing datasets based on lazily loaded Pangeo Forge Zarr data

import fsspec
import requests
import xarray as xr
import xpublish
from xpublish import rest


recipe_runs_url = "https://api.pangeo-forge.org/recipe_runs/"


def pangeo_forge_datasets():
    res = requests.get(recipe_runs_url)
    return res.json()


def pangeo_forge_with_data():
    datasets = pangeo_forge_datasets()
    return [r for r in datasets if r["dataset_public_url"]]


def pangeo_forge_dataset_map():
    datasets = pangeo_forge_with_data()
    return {r["recipe_id"]: r["dataset_public_url"] for r in datasets}


def get_pangeo_forge_dataset(dataset_id: str) -> xr.Dataset:
    dataset_map = pangeo_forge_dataset_map()
    zarr_url = dataset_map[dataset_id]

    mapper = fsspec.get_mapper(zarr_url)
    ds = xr.open_zarr(mapper, consolidated=True)
    return ds


class DynamicRest(xpublish.Rest):
    def __init__(self, routers=None, cache_kws=None, app_kws=None):
        self._get_dataset_func = get_pangeo_forge_dataset
        self._datasets = list(pangeo_forge_dataset_map().keys())
        dataset_route_prefix = "/datasets/{dataset_id}"

        self._app_routers = rest._set_app_routers(routers, dataset_route_prefix)

        self._app = None
        self._app_kws = {}
        if app_kws is not None:
            self._app_kws.update(app_kws)

        self._cache = None
        self._cache_kws = {"available_bytes": 1e6}
        if cache_kws is not None:
            self._cache_kws.update(cache_kws)


dynamic = DynamicRest()
dynamic.serve(log_level="trace", port=9005)
