from recipe import recipe

from pangeo_forge_recipes.storage import CacheFSSpecTarget, FSSpecTarget, MetadataTarget, StorageConfig

from fsspec.implementations.local import LocalFileSystem

import os, shutil

if os.path.exists('target'):
    shutil.rmtree('target')

fs = LocalFileSystem()

cache = CacheFSSpecTarget(fs=fs, root_path="./cache/")
target = CacheFSSpecTarget(fs=fs, root_path="./target/")

recipe.storage_config = StorageConfig(target, cache)

from pangeo_forge_recipes.recipes import setup_logging
setup_logging(level="INFO")

recipe_pruned = recipe.copy_pruned(10)

recipe_function = recipe_pruned.to_function()

recipe_function()

import xarray as xr

ds = xr.open_zarr(target.get_mapper())
print(ds)
