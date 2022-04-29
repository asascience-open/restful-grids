# Run with `uvicorn --port 9005 main:app --reload`
from xpublish.routers import base_router, zarr_router
from fastapi.staticfiles import StaticFiles

from demo_rest import DemoRest
from edr_router import edr_router
from tree_router import tree_router
from tile_router import tile_router


rest = DemoRest(
    routers=[
        (base_router, {"tags": ["info"]}),
        (edr_router, {"tags": ["edr"], "prefix": "/edr"}),
        (tree_router, {"tags": ["datatree"], "prefix": "/tree"}),
        (tile_router, {"tags": ["image"], "prefix": "/tile"}),
        (zarr_router, {"tags": ["zarr"], "prefix": "/zarr"}),
    ]
)

app = rest.app

app.description = "Hacking on xpublish during the IOOS Code Sprint"
app.title = "IOOS xpublish"

edr_description = """
OGC Environmental Data Retrieval API

Currently the position query is supported, which takes a single Well Known Text point.
"""

datatree_description = """
Dynamic generation of Zarr ndpyramid/Datatree for access from webmaps.

- [carbonplan/maps](https://carbonplan.org/blog/maps-library-release)
- [xpublish#92](https://github.com/xarray-contrib/xpublish/issues/92)
"""

zarr_description = """
Zarr access to NetCDF datasets.

Load by using an fsspec mapper

```python
mapper = fsspec.get_mapper("/datasets/{dataset_id}/zarr/")
ds = xr.open_zarr(mapper, consolidated=True)
```
"""

app.openapi_tags = [
    {"name": "info"},
    {
        "name": "edr",
        "description": edr_description,
        "externalDocs": {
            "description": "OGC EDR Reference",
            "url": "https://ogcapi.ogc.org/edr/",
        },
    },
    {"name": "image", "description": "WMS-like image generation"},
    {"name": "datatree", "description": datatree_description},
    {"name": "zarr", "description": zarr_description},
]

app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn

    # When run directly, run in debug mode
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=9005,
        reload=True,
        log_level="debug",
        debug=True,
    )
