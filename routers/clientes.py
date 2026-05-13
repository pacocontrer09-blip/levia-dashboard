import asyncio
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from services.shopify_service import get_customer_count, get_customers_detail, get_abandoned_checkouts, get_draft_orders
from services.analytics_service import compute_rfm_segments

templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))
router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def clientes_page(request: Request):
    try:
        customer_count, customers_detail, abandoned, drafts = await asyncio.gather(
            get_customer_count(),
            get_customers_detail(limit=50),
            get_abandoned_checkouts(limit=50),
            get_draft_orders(),
        )
    except Exception:
        customer_count = 0
        customers_detail = {"customers": [], "avg_ltv": 0, "returning_pct": 0, "accepts_marketing_pct": 0, "new_this_month": 0}
        abandoned = {"count": 0, "revenue_at_risk": 0, "checkouts": []}
        drafts = {"count": 0, "total_value": 0, "drafts": []}

    customers = customers_detail.get("customers", [])
    avg_ltv = customers_detail.get("avg_ltv", 0)
    returning_pct = customers_detail.get("returning_pct", 0)
    accepts_marketing_pct = customers_detail.get("accepts_marketing_pct", 0)
    new_this_month = customers_detail.get("new_this_month", 0)

    rfm = compute_rfm_segments(customers)
    rfm_counts = rfm.get("counts", {})
    customers_at_risk = rfm.get("customers_at_risk", [])
    churn_rate = rfm.get("churn_rate", 0)

    abandoned_count = abandoned.get("count", 0)
    revenue_at_risk = abandoned.get("revenue_at_risk", 0)
    abandoned_rows = abandoned.get("checkouts", [])

    draft_count = drafts.get("count", 0)
    draft_value = drafts.get("total_value", 0)
    draft_rows = drafts.get("drafts", [])

    has_customers = len(customers) > 0

    return templates.TemplateResponse("clientes.html", {
        "request": request,
        "page": "clientes",
        "customer_count": customer_count,
        "avg_ltv": avg_ltv,
        "returning_pct": returning_pct,
        "accepts_marketing_pct": accepts_marketing_pct,
        "new_this_month": new_this_month,
        "abandoned_count": abandoned_count,
        "revenue_at_risk": revenue_at_risk,
        "abandoned_rows": abandoned_rows,
        "draft_count": draft_count,
        "draft_value": draft_value,
        "draft_rows": draft_rows,
        "customers": customers,
        "has_customers": has_customers,
        "rfm_counts": rfm_counts,
        "customers_at_risk": customers_at_risk,
        "churn_rate": churn_rate,
        "fetched_at": datetime.now().strftime("%H:%M"),
    })
