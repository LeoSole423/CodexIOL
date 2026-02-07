from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from .templates import templates

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request):
    return templates.TemplateResponse("dashboard.html", {"request": request})


@router.get("/assets", response_class=HTMLResponse)
def assets(request: Request):
    return templates.TemplateResponse("assets.html", {"request": request})


@router.get("/history", response_class=HTMLResponse)
def history(request: Request):
    return templates.TemplateResponse("history.html", {"request": request})

