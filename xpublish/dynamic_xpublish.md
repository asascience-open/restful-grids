# Dynamically loading datasets with xpublish

Currently [`xpublish.Rest`](https://xpublish.readthedocs.io/en/latest/generated/xpublish.Rest.html) requires datasets to be loaded ahead of time, but with a little subclassing, it's possible to load the datasets on demand.

## Borrowing the Pangeo-Forge API

We attempted this with the [Pangeo-Forge](https://pangeo-forge.org/) recipe_runs API: https://api.pangeo-forge.org/recipe_runs/

```json
[
  {
    "recipe_id": "noaa-oisst-avhrr-only",
    "bakery_id": 1,
    "feedstock_id": 1,
    "head_sha": "c975c63bec53029fcb299bbd98eac2abb43d2cfe",
    "version": "0.0",
    "started_at": "2022-03-04T13:27:43",
    "completed_at": "2022-03-04T13:37:43",
    "conclusion": "success",
    "status": "completed",
    "is_test": true,
    "dataset_type": "zarr",
    "dataset_public_url": "https://ncsa.osn.xsede.org/Pangeo/pangeo-forge-test/prod/recipe-run-5/pangeo-forge/staged-recipes/noaa-oisst-avhrr-only.zarr",
    "message": "{\"flow_id\": \"871c003c-e273-41d8-8440-2622492a2ead\"}",
    "id": 5
  },
]
```

````{margin}
```{admonition} Incomplete

This isn't the best representation of the datasets on Pangeo-Forge, as this API is focused around the processing steps, rather than the datasets themselves.
Therefore, some datasets are duplicated, and others may be missing when the API paginates, but it's good enough to test ideas out with.

```
````

With this API, we can use the `recipe_id` and the `dataset_public_url` to make a mapping of datasets that then we can use with xpublish.

With that we can build a mapper from `recipe_id`s to the Zarr URLs needed to load them.

```py
def pangeo_forge_dataset_map():
    datasets = requests.get(recipe_runs_url)
    datasets = [r for r in datasets if r["dataset_public_url"]]
    return {r["recipe_id"]: r["dataset_public_url"] for r in datasets}
```

## Dataset Loader

From there, we need a function that can will take a `dataset_id` as a string, and return an xarray dataset. xpublish by default [curries a function](https://github.com/xarray-contrib/xpublish/blob/632a720aadba39cebaf062da7043835262d9fa3d/xpublish/rest.py#L16-L28) with the [datasets passed to the init method as a loader](https://github.com/xarray-contrib/xpublish/blob/632a720aadba39cebaf062da7043835262d9fa3d/xpublish/rest.py#L118), but we can get more creative and delay dataset access until needed.

```py
def get_pangeo_forge_dataset(dataset_id: str) -> xr.Dataset:
    dataset_map = pangeo_forge_dataset_map()
    zarr_url = dataset_map[dataset_id]

    mapper = fsspec.get_mapper(zarr_url)
    ds = xr.open_zarr(mapper, consolidated=True)
    return ds
```

## Connecting it together in the `__init__` method

Instead of calling super in the init method and having to pass in mock info, we can override the whole init and change the signature.

```py
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
```

The first three lines of the method are the key ones. We are setting our dataset function for the get_dataset_func, listing the ids of our datasets, and setting the prefix that we want to have multiple dataset access.

The rest of the method is unchanged.

From there, you can call `rest = DynamicRest()` or pass in routers as normal with xpublish.

## What next?

There are a few things that could be further improved with this method.
The biggest improvement would be to cache the `dataset_id`s and datasets themselves.

Since both of these are used as FastAPI dependencies, they can also use dependencies themselves.

````{margin}
```{admonition} Untested
:class: warning

Use as is at your own peril.

```
````

```py
def pangeo_forge_dataset_map(cache: cachey.Cache = Depends(get_cache)):
    cache_key = "dataset_ids"
    datasets = cache.get(cache_key)
    if not datasets:
        datasets = requests.get(recipe_runs_url)
        datasets = [r for r in datasets if r["dataset_public_url"]]
        datasets = {r["recipe_id"]: r["dataset_public_url"] for r in datasets}
        cache.set(cache_key, datasets, NOT_TO_EXPENSIVE_CACHE_COST)

    return datasets


def get_pangeo_forge_dataset(
    dataset_id: str, 
    datasets_map: dict = Depends(pangeo_forge_dataset_map),
    cache: cachey.Cache = Depends(get_cache),
) -> xr.Dataset:
    cache_key = f"dataset-{dataset_id}"
    ds = cache.get(cache_key)
    if not dataset:
        zarr_url = dataset_map[dataset_id]

        mapper = fsspec.get_mapper(zarr_url)
        ds = xr.open_zarr(mapper, consolidated=True)

        cache.set(cache_key, ds, EXPENSIVE_CACHE_COST)

    return ds
```

To truly use the datasets lazily, the dependency needs to be set.
This isn't happening in the init method, but in [`_init_app`](https://github.com/xarray-contrib/xpublish/blob/632a720aadba39cebaf062da7043835262d9fa3d/xpublish/rest.py#L149), so we'd have to change things up a little.

```py
class DynamicRest(xpublish.Rest):
    def __init__(self, routers=None, cache_kws=None, app_kws=None):
        self._get_dataset_func = get_pangeo_forge_dataset
        self._datasets = ["these", "are", "a", "lie"]
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

    def _init_app(self):
        super(self)._init_app()  # let it do the normal setup, then just re-override things

        self._app.dependency_overrides[get_dataset_ids] = pangeo_forge_dataset_map
```