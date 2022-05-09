import os

from dask.distributed import Client
from fastapi import Depends

DASK_SCHDEULER_URL = os.environ.get("DASK_SCHEDULER_URL", "localhost:8786")


def get_dask_cluster():
    yield DASK_SCHDEULER_URL


def get_dask_client(cluster=Depends(get_dask_cluster)):
    with Client(cluster) as client:
        yield client
