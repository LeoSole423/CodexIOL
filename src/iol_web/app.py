from fastapi import FastAPI

from .routes_api import router as api_router


app = FastAPI(title="IOL Portfolio API")

app.include_router(api_router)
