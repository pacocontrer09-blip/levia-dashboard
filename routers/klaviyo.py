from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

from services.treasury_service import get_treasury

templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))
router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def treasury_page(request: Request):
    data = await get_treasury()
    return templates.TemplateResponse("klaviyo.html", {
        "request": request,
        "page": "klaviyo",
        **data,
    })
