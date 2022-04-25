# Goals for this Rest-like API

## Resources
* Amazon Bucket - `s3://ioos-code-sprint-2022`
* Github Repo - https://github.com/asascience/restful-grids

## Current Solutions
* Point to Zarr file --> user subsets

## Goals
* Getting a single point
* Getting a bounding box
* Query using time
* Optimize data retrieval for temporal data
    * Chunk by space
    * Chunk by time
    * Chunk by space + time

## First Steps
* What does it take to subset a point of data from a cloud hosted dataset?
    * What dataset?
        * GFS!!
        * https://registry.opendata.aws/noaa-gfs-bdp-pds/#usageexamples
    * Wave Watch + Buoy
* Consider OGC API integration with xarray; see where pain points are
* Try pygeoapi, but know there are existing issues


## Existing solutions
* OGC PyGEOAPI - https://pygeoapi.io
* stack STAC - https://stackstac.readthedocs.io/en/latest/basic.html
* Xpublish - https://github.com/xarray-contrib/xpublish
* OGC Environment Data Retrieval - https://github.com/opengeospatial/ogcapi-environmental-data-retrieval
* NetCDF subset - https://www.unidata.ucar.edu/software/tds/current/reference/NetcdfSubsetServiceReference.html
* ERDDAP

## Defining IO
* In - zarr dataset
* Out
    * Json, binary, or text
    * Provide a tile

## Datasets
* NECOFS