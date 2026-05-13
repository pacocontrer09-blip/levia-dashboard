import asyncio
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from services.shopify_service import (
    get_orders_with_lineitems, get_orders_month, get_customer_count,
    get_orders_with_refunds, get_discount_codes_stats, get_orders_with_geo,
)
from services.analytics_service import compute_day_of_week, compute_geographic_breakdown

templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))
router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def ventas_page(request: Request, days: int = 30):
    if days not in (7, 30, 90):
        days = 30

    try:
        lineitems, current_month, customer_count, refunds, discounts, geo_orders = await asyncio.gather(
            get_orders_with_lineitems(days=days),
            get_orders_month(),
            get_customer_count(),
            get_orders_with_refunds(days=days),
            get_discount_codes_stats(days=days),
            get_orders_with_geo(days=days),
        )
    except Exception:
        lineitems = {"daily": [], "top_products": [], "revenue_mxn": 0, "count": 0}
        current_month = {}
        customer_count = 0
        refunds = {"revenue_net": 0, "total_refunded": 0, "refund_rate_pct": 0, "unfulfilled_count": 0, "unfulfilled_revenue": 0}
        discounts = {"codes": [], "total_discounted_revenue": 0}
        geo_orders = {"orders": []}
    try:
        prev_month_data = await get_orders_month(_prev_month())
    except Exception:
        prev_month_data = {}

    daily = lineitems.get("daily", [])
    top_products = lineitems.get("top_products", [])
    total_revenue = lineitems.get("revenue_mxn", 0)
    total_orders = lineitems.get("count", 0)
    aov = round(total_revenue / total_orders, 0) if total_orders > 0 else 0

    curr_revenue = current_month.get("revenue_mxn", 0)
    prev_revenue = prev_month_data.get("revenue_mxn", 0)
    curr_orders = current_month.get("count", 0)
    prev_orders = prev_month_data.get("count", 0)
    revenue_delta = _delta_pct(curr_revenue, prev_revenue)
    orders_delta = _delta_pct(curr_orders, prev_orders)

    chart_labels = [d["date"][5:] for d in daily]
    chart_revenue = [d["revenue"] for d in daily]
    chart_orders = [d["orders"] for d in daily]

    has_data = total_revenue > 0 or total_orders > 0

    revenue_net = refunds.get("revenue_net", total_revenue)
    total_refunded = refunds.get("total_refunded", 0)
    refund_rate_pct = refunds.get("refund_rate_pct", 0)
    unfulfilled_count = refunds.get("unfulfilled_count", 0)
    unfulfilled_revenue = refunds.get("unfulfilled_revenue", 0)

    # Day of week from daily data (convert to order-like list for analytics)
    order_list = [{"created_at": d["date"], "total_price": d["revenue"]} for d in daily if d["orders"] > 0]
    dow = compute_day_of_week(order_list)

    # Geographic breakdown
    geo_raw = geo_orders.get("orders", [])
    geo = compute_geographic_breakdown(geo_raw)

    return templates.TemplateResponse("ventas.html", {
        "request": request,
        "page": "ventas",
        "days": days,
        "total_revenue": total_revenue,
        "total_orders": total_orders,
        "aov": aov,
        "customer_count": customer_count,
        "top_products": top_products[:10],
        "chart_labels": chart_labels,
        "chart_revenue": chart_revenue,
        "chart_orders": chart_orders,
        "has_data": has_data,
        "curr_month": datetime.now().strftime("%b %Y"),
        "prev_month": _prev_month(),
        "curr_revenue": curr_revenue,
        "prev_revenue": prev_revenue,
        "curr_orders": curr_orders,
        "prev_orders": prev_orders,
        "revenue_delta": revenue_delta,
        "orders_delta": orders_delta,
        "revenue_net": revenue_net,
        "total_refunded": total_refunded,
        "refund_rate_pct": refund_rate_pct,
        "unfulfilled_count": unfulfilled_count,
        "unfulfilled_revenue": unfulfilled_revenue,
        "fetched_at": datetime.now().strftime("%H:%M"),
        # Discounts
        "discount_codes": discounts.get("codes", [])[:10],
        "total_discounted_revenue": discounts.get("total_discounted_revenue", 0),
        # Day of week
        "dow_labels": dow.get("labels", []),
        "dow_avg_revenue": dow.get("avg_revenue", []),
        "dow_best_day": dow.get("best_day", "—"),
        # Geography
        "geo_states": geo.get("states", [])[:10],
        "geo_top_state": geo.get("top_state", "—"),
    })


def _prev_month() -> str:
    now = datetime.now()
    if now.month == 1:
        return f"{now.year - 1}-12"
    return f"{now.year}-{now.month - 1:02d}"


def _delta_pct(current: float, previous: float) -> float | None:
    if previous == 0:
        return None
    return round((current - previous) / previous * 100, 1)
