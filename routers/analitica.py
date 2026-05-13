import asyncio
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from services.shopify_service import get_orders_historical, get_customers_detail, get_orders_with_refunds
from services.meta_service import get_campaigns
from services.analytics_service import (
    compute_day_of_week,
    compute_forecasting,
    compute_cohort_retention,
    compute_ltv_cac_metrics,
    compute_geographic_breakdown,
)

templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))
router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def analitica_page(request: Request):
    try:
        historical, customers_data, meta_data, refunds_data = await asyncio.gather(
            get_orders_historical(days=90),
            get_customers_detail(limit=200),
            get_campaigns(),
            get_orders_with_refunds(days=90),
        )
    except Exception:
        historical = {"orders": [], "revenue_mxn": 0, "count": 0}
        customers_data = {"customers": [], "total_count": 0}
        meta_data = {"campaigns": []}
        refunds_data = {}

    orders = historical.get("orders", [])
    customers = customers_data.get("customers", [])
    live_campaigns = meta_data.get("campaigns", [])
    total_ad_spend = sum(c.get("spend_mxn", 0) for c in live_campaigns)

    # ── Compute analytics ──
    dow = compute_day_of_week(orders)
    forecast = compute_forecasting(orders, days_ahead=30)
    cohorts = compute_cohort_retention(customers)
    ltv_cac = compute_ltv_cac_metrics(customers, total_ad_spend)
    geo = compute_geographic_breakdown(orders)

    # Trend icon mapping
    trend_icon = {
        "up_strong": "↑↑", "up": "↑",
        "flat": "→",
        "down": "↓", "down_strong": "↓↓",
    }.get(forecast.get("trend", "flat"), "→")
    trend_color = {
        "up_strong": "#065f46", "up": "#065f46",
        "flat": "#4A5B76",
        "down": "#991b1b", "down_strong": "#991b1b",
    }.get(forecast.get("trend", "flat"), "#4A5B76")

    confidence_label = {"high": "Alta", "medium": "Media", "low": "Baja"}.get(
        forecast.get("confidence", "low"), "Baja"
    )

    return templates.TemplateResponse("analitica.html", {
        "request": request,
        "page": "analitica",
        "now": datetime.now().strftime("%d %b %Y · %H:%M"),
        # LTV:CAC
        "ltv": ltv_cac.get("ltv", 0),
        "cac": ltv_cac.get("cac", 0),
        "ltv_cac_ratio": ltv_cac.get("ltv_cac_ratio", 0),
        "payback_days": ltv_cac.get("payback_days", 0),
        "ratio_color": ltv_cac.get("ratio_color", "red"),
        "payback_color": ltv_cac.get("payback_color", "red"),
        "ratio_label": ltv_cac.get("ratio_label", "—"),
        # Forecasting
        "forecast_revenue": forecast.get("forecast_revenue", 0),
        "forecast_orders": forecast.get("forecast_orders", 0),
        "daily_avg": forecast.get("daily_avg", 0),
        "daily_avg_trend": forecast.get("daily_avg_trend", 0),
        "trend_icon": trend_icon,
        "trend_color": trend_color,
        "confidence_label": confidence_label,
        "hist_labels": forecast.get("hist_labels", []),
        "hist_revenue": forecast.get("hist_revenue", []),
        "proj_labels": forecast.get("proj_labels", []),
        "proj_revenue": forecast.get("proj_revenue", []),
        # Day of week
        "dow_labels": dow.get("labels", []),
        "dow_avg_revenue": dow.get("avg_revenue", []),
        "dow_avg_orders": dow.get("avg_orders", []),
        "dow_best_day": dow.get("best_day", "—"),
        "dow_best_revenue": dow.get("best_day_revenue", 0),
        # Cohorts
        "cohort_rows": cohorts.get("cohorts", []),
        "overall_retention": cohorts.get("overall_retention_pct", 0),
        "overall_acquired": cohorts.get("overall_acquired", 0),
        # Geography
        "geo_states": geo.get("states", [])[:15],
        "geo_top_state": geo.get("top_state", "—"),
        "geo_total_states": geo.get("total_states", 0),
        # Meta
        "total_customers": customers_data.get("total_count", 0),
        "total_orders_90d": historical.get("count", 0),
        "total_revenue_90d": historical.get("revenue_mxn", 0),
        "ad_spend_7d": total_ad_spend,
    })
