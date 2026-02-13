import os
import time

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles

from .routes_api import router as api_router
from .routes_pages import router as pages_router


# Cache-busting for static assets (app.js/styles.css) referenced as `?v={{ request.app.version }}`.
# Uvicorn runs without `--reload`, so restarting the container should reliably refresh assets in the browser.
build_id = (os.getenv("IOL_WEB_BUILD_ID") or "").strip() or str(int(time.time()))
app = FastAPI(title="IOL Portfolio Dashboard", version=build_id)

app.include_router(api_router)
app.include_router(pages_router)


@app.middleware("http")
async def _no_cache_static(request: Request, call_next):
    resp = await call_next(request)
    if request.url.path.startswith("/static/"):
        resp.headers["Cache-Control"] = "no-store, max-age=0, must-revalidate"
        resp.headers["Pragma"] = "no-cache"
    return resp


app.mount("/static", StaticFiles(packages=[("iol_web", "static")]), name="static")
