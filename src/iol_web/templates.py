from pathlib import Path

from fastapi.templating import Jinja2Templates

_BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(_BASE_DIR / "templates"))

