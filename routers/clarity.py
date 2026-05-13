from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from pathlib import Path
from services.clarity_service import get_clarity_status

templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))
router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def clarity_page(request: Request):
    status = get_clarity_status()
    return templates.TemplateResponse("clarity.html", {
        "request": request,
        "page": "clarity",
        **status,
    })
