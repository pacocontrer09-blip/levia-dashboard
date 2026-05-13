import os
from datetime import date, datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from services import facturapi_service
from services.facturapi_service import get_iva_acreditable_from_bank
from services.shopify_service import get_orders_historical

BASE_DIR = Path(__file__).parent.parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

router = APIRouter()


# ---------------------------------------------------------------------------
# Página principal
# ---------------------------------------------------------------------------

@router.get("/sat", response_class=HTMLResponse)
async def sat_page(request: Request):
    year = datetime.now().year
    month = datetime.now().strftime("%Y-%m")
    kpis = facturapi_service.get_fiscal_kpis(year)
    invoices = facturapi_service.list_invoices()[:30]
    iva_data = get_iva_acreditable_from_bank(month)

    return templates.TemplateResponse("sat.html", {
        "request": request,
        "page": "sat",
        "kpis": kpis,
        "invoices": invoices,
        "year": year,
        "sat_rfc": os.getenv("SAT_RFC", ""),
        "api_configurada": kpis["api_configurada"],
        "modo_live": kpis["modo_live"],
        "iva_data": iva_data,
        "month": month,
    })


# ---------------------------------------------------------------------------
# Generar CFDI global de un día específico
# ---------------------------------------------------------------------------

@router.post("/sat/cfdi/global")
async def generate_global_cfdi(request: Request):
    body = await request.json()
    target_date_str = body.get("date")  # "YYYY-MM-DD"

    if target_date_str:
        try:
            target_date = date.fromisoformat(target_date_str)
        except ValueError:
            return JSONResponse({"ok": False, "error": "Fecha inválida"}, status_code=400)
    else:
        # Por defecto: ayer
        target_date = date.today() - timedelta(days=1)

    # Obtener órdenes de Shopify del día objetivo
    orders_raw = await get_orders_historical(days=90)
    day_str = target_date.isoformat()
    day_orders = [
        o for o in orders_raw
        if o.get("created_at", "").startswith(day_str) and o.get("financial_status") == "paid"
    ]

    result = await facturapi_service.create_global_invoice(target_date, day_orders)
    return JSONResponse(result)


# ---------------------------------------------------------------------------
# Historial de CFDIs (JSON)
# ---------------------------------------------------------------------------

@router.get("/sat/cfdi/list")
async def list_cfdi(month: int | None = None, year: int | None = None):
    invoices = facturapi_service.list_invoices(month=month, year=year)
    return JSONResponse({"ok": True, "invoices": invoices, "total": len(invoices)})


# ---------------------------------------------------------------------------
# KPIs fiscales (JSON)
# ---------------------------------------------------------------------------

@router.get("/sat/kpis")
async def fiscal_kpis(year: int | None = None):
    if not year:
        year = datetime.now().year
    return JSONResponse(facturapi_service.get_fiscal_kpis(year))


# ---------------------------------------------------------------------------
# Job manual: generar CFDI de ayer (llamado también por el scheduler)
# ---------------------------------------------------------------------------

async def auto_generate_yesterday_cfdi():
    """Llamado por APScheduler a las 8 AM cada día."""
    yesterday = date.today() - timedelta(days=1)
    print(f"[sat] Generando CFDI global automático para {yesterday}")

    orders_raw = await get_orders_historical(days=3)
    day_str = yesterday.isoformat()
    day_orders = [
        o for o in orders_raw
        if o.get("created_at", "").startswith(day_str) and o.get("financial_status") == "paid"
    ]

    result = await facturapi_service.create_global_invoice(yesterday, day_orders)
    if result.get("ok"):
        print(f"[sat] CFDI timbrado correctamente — {len(day_orders)} órdenes")
    else:
        print(f"[sat] Error al timbrar CFDI: {result.get('error')}")
