import xarray as xr
import xpublish

ds = xr.open_dataset("../datasets/ww3_72_east_coast_2022041112.nc")


# ds.rest.serve(log_level="debug")

rest_collection = xpublish.Rest(ds)
# rest_collection = xpublish.Rest({"ww3": ds, "bio": ds})
rest_collection.serve(log_level="trace", port=9005)
