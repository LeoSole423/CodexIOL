from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from .routes_api import router as api_router
from .routes_pages import router as pages_router


app = FastAPI(title="IOL Portfolio Dashboard", version="0.1.0")

app.include_router(api_router)
app.include_router(pages_router)

app.mount("/static", StaticFiles(packages=[("iol_web", "static")]), name="static")

