"""
Load Pangeo-Forge and our datasets
"""
import fsspec
import requests
import xarray as xr
import cf_xarray
import xpublish
from xpublish import rest


recipe_runs_url = "https://api.pangeo-forge.org/recipe_runs/"


def pangeo_forge_datasets_map():
    res = requests.get(recipe_runs_url)
    datasets = res.json()
    datasets = [r for r in datasets if r["dataset_public_url"]]
    return {r["recipe_id"]: r["dataset_public_url"] for r in datasets}


def dataset_map():
    datasets = pangeo_forge_datasets_map()
    datasets["ww3"] = "ww3-stub"
    datasets["gfs"] = "https://ioos-code-sprint-2022.s3.amazonaws.com/gfs-wave.zarr"

    return datasets


def get_dataset(dataset_id: str) -> xr.Dataset:
    if dataset_id == "ww3":
        return xr.open_dataset("../datasets/ww3_72_east_coast_2022041112.nc")

    zarr_url = dataset_map()[dataset_id]

    mapper = fsspec.get_mapper(zarr_url)
    ds = xr.open_zarr(mapper, consolidated=True)

    if "X" not in ds.cf.axes:
        x_axis = ds[ds.cf.coordinates["longitude"][0]]
        x_axis.attrs["axis"] = "X"
    if "Y" not in ds.cf.axes:
        y_axis = ds[ds.cf.coordinates["latitude"][0]]
        y_axis.attrs["axis"] = "Y"

    return ds


class DemoRest(xpublish.Rest):
    def __init__(self, routers=None, cache_kws=None, app_kws=None):
        self._get_dataset_func = get_dataset
        self._datasets = list(dataset_map().keys())
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
