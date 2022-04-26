import pandas as pd
from datetime import datetime, date
from pangeo_forge_recipes.patterns import ConcatDim, MergeDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

# URL
# https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/prod/gfs.20220426/00/wave/gridded/gfswave.t00z.atlocn.0p16.f000.grib2 

def make_url(time):

    return (
            "https://nomads.ncep.noaa.gov/pub/data/nccf/com/gfs/"
            "prod/gfs.20220426/00/wave/gridded/"
            f"gfswave.t00z.atlocn.0p16.f{time:03d}.grib2"
        )


# A GFS Wave forecast is every hour for 384 hours
time_concat_dim = ConcatDim("time", range(384), nitems_per_file=1)

pattern = FilePattern(make_url, time_concat_dim)

def process_input(ds, filename):

    ds = ds.expand_dims('time')
    return ds

recipe = XarrayZarrRecipe(file_pattern=pattern,  
                          process_input=process_input,
                          target_chunks={'time': 1, 'latitude':166, 'longitude':151 },
                          xarray_open_kwargs={'engine': 'cfgrib'},
                          copy_input_to_local_file=True
                         )
