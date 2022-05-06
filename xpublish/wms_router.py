
from fastapi import APIRouter


wms_router = APIRouter()


@wms_router.get('/')
def wms_root():
    return ''