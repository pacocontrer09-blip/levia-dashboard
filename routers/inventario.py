import json
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, Request, Form
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, RedirectResponse
from services.shopify_service import get_orders_with_lineitems

templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))
router = APIRouter()

from services.data_dir import DATA_DIR
INVENTORY_PATH = DATA_DIR / "inventory_state.json"
COGS_USD = 26.0
FX_DEFAULT = 17.5
REORDER_DAYS = 30


def _read_inventory() -> dict:
    try:
        return json.loads(INVENTORY_PATH.read_text())
    except Exception:
        return {"units": 0, "updated_at": ""}


def _write_inventory(data: dict):
    INVENTORY_PATH.write_text(json.dumps(data, ensure_ascii=False))


@router.get("/", response_class=HTMLResponse)
async def inventario_page(request: Request):
    inv = _read_inventory()
    units_on_hand = int(inv.get("units", 0))
    updated_at = inv.get("updated_at", "")

    lineitems = await get_orders_with_lineitems(days=30)
    sold_30d = lineitems.get("count", 0)
    daily_velocity = sold_30d / 30 if sold_30d > 0 else 0
    days_coverage = int(units_on_hand / daily_velocity) if daily_velocity > 0 else None

    reorder_point = int(daily_velocity * REORDER_DAYS)
    needs_reorder = units_on_hand <= reorder_point and units_on_hand > 0

    cogs_unit_mxn = COGS_USD * FX_DEFAULT
    inventory_value_mxn = units_on_hand * cogs_unit_mxn
    inventory_value_usd = units_on_hand * COGS_USD

    coverage_color = "red" if days_coverage is not None and days_coverage < 30 else (
        "yellow" if days_coverage is not None and days_coverage < 60 else "green"
    )

    return templates.TemplateResponse("inventario.html", {
        "request": request,
        "page": "inventario",
        "units_on_hand": units_on_hand,
        "updated_at": updated_at[:10] if updated_at else "—",
        "sold_30d": sold_30d,
        "daily_velocity": round(daily_velocity, 1),
        "days_coverage": days_coverage,
        "coverage_color": coverage_color,
        "reorder_point": reorder_point,
        "needs_reorder": needs_reorder,
        "inventory_value_mxn": round(inventory_value_mxn, 0),
        "inventory_value_usd": round(inventory_value_usd, 0),
        "cogs_unit_mxn": round(cogs_unit_mxn, 0),
        "fetched_at": datetime.now().strftime("%H:%M"),
    })


@router.post("/update")
async def update_inventory(units: int = Form(...)):
    data = {"units": max(0, units), "updated_at": datetime.now().isoformat()}
    _write_inventory(data)
    return RedirectResponse(url="/inventario", status_code=303)
