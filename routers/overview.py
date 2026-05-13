import asyncio
import json
from datetime import datetime, timedelta
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from services.meta_service import read_agent_state, get_campaigns, get_campaigns_daily
from services import sse_service
from services.shopify_service import (
    get_orders_today, get_orders_historical, get_orders_with_lineitems,
    get_customer_count, get_abandoned_checkouts, get_orders_with_refunds,
)

LEVIA_DIR = Path(__file__).parent.parent.parent
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))
router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def overview(request: Request):
    state = read_agent_state()
    today, hist, meta_data, customer_count, lineitems, abandoned, refunds, daily_meta = await asyncio.gather(
        get_orders_today(),
        get_orders_historical(days=90),
        get_campaigns(),
        get_customer_count(),
        get_orders_with_lineitems(days=30),
        get_abandoned_checkouts(),
        get_orders_with_refunds(days=90),
        get_campaigns_daily(days=14),
    )

    orders_today = today.get("count", 0)
    revenue_today = today.get("revenue_mxn", 0)
    hist_orders = hist.get("count", 0)
    hist_revenue = hist.get("revenue_mxn", 0)

    live_campaigns = meta_data.get("campaigns", [])
    meta_spend = sum(c.get("spend_mxn", 0) for c in live_campaigns)
    total_impressions = sum(c.get("impressions", 0) for c in live_campaigns)
    total_clicks = sum(c.get("clicks", 0) for c in live_campaigns)
    meta_ctr = round(total_clicks / total_impressions * 100, 1) if total_impressions > 0 else 0
    total_reach = sum(c.get("reach", 0) for c in live_campaigns)
    avg_frequency = round(sum(c.get("frequency", 0) for c in live_campaigns) / len(live_campaigns), 2) if live_campaigns else 0

    roas = round(hist_revenue / meta_spend, 2) if meta_spend > 0 else 0
    roas_color = "green" if roas >= 1.2 else ("yellow" if roas >= 0.8 else ("red" if roas > 0 else ""))
    mer = round(hist_revenue / meta_spend, 2) if meta_spend > 0 else 0
    mer_color = "green" if mer >= 2.5 else ("yellow" if mer >= 1.5 else ("red" if mer > 0 else ""))
    aov_mxn = round(hist_revenue / hist_orders, 0) if hist_orders > 0 else 0

    budget_total = state.get("budget_total", 5000)
    budget_remaining = max(0, budget_total - meta_spend)
    budget_pct = int(meta_spend / budget_total * 100) if budget_total > 0 else 0

    # Week-over-week comparison from 30d daily data
    daily_30 = lineitems.get("daily", [])
    this_week_revenue = sum(d["revenue"] for d in daily_30[-7:])
    prev_week_revenue = sum(d["revenue"] for d in daily_30[-14:-7])
    this_week_orders = sum(d["orders"] for d in daily_30[-7:])
    prev_week_orders = sum(d["orders"] for d in daily_30[-14:-7])

    def _delta(curr, prev):
        if prev == 0:
            return None
        return round((curr - prev) / prev * 100, 1)

    revenue_wow = _delta(this_week_revenue, prev_week_revenue)
    orders_wow = _delta(this_week_orders, prev_week_orders)

    # Meta 7d vs prev-7d velocity deltas
    def _avg(lst):
        lst = [x for x in (lst or []) if x and x > 0]
        return sum(lst) / len(lst) if lst else 0

    dm_spend  = daily_meta.get("spend", [])
    dm_roas   = daily_meta.get("roas", [])
    dm_ctr    = daily_meta.get("ctr", [])
    dm_cpm    = daily_meta.get("cpm", [])
    dm_reach  = daily_meta.get("reach", [])

    spend_delta = _delta(sum(dm_spend[-7:]), sum(dm_spend[-14:-7])) if len(dm_spend) >= 7 else None
    roas_delta  = _delta(_avg(dm_roas[-7:]),  _avg(dm_roas[-14:-7]))  if len(dm_roas)  >= 7 else None
    ctr_delta   = _delta(_avg(dm_ctr[-7:]),   _avg(dm_ctr[-14:-7]))   if len(dm_ctr)   >= 7 else None
    cpm_delta   = _delta(_avg(dm_cpm[-7:]),   _avg(dm_cpm[-14:-7]))   if len(dm_cpm)   >= 7 else None
    reach_delta = _delta(sum(dm_reach[-7:]),  sum(dm_reach[-14:-7]))  if len(dm_reach) >= 7 else None

    # UGC pipeline
    pending_path = LEVIA_DIR / "12_CREATIVOS_UGC" / "pending_manual.json"
    pending_count = 0
    if pending_path.exists():
        try:
            pending_count = len(json.loads(pending_path.read_text()))
        except Exception:
            pass

    ugc_output = LEVIA_DIR / "12_CREATIVOS_UGC" / "output"
    week_folders = sorted([d.name for d in ugc_output.iterdir() if d.is_dir()]) if ugc_output.exists() else []
    current_week = week_folders[-1] if week_folders else "—"

    decisions = state.get("decisions_log", [])[-3:][::-1]

    # 30-day daily chart
    chart_labels = [d["date"][5:] for d in daily_30]
    chart_revenue = [d["revenue"] for d in daily_30]
    chart_orders = [d["orders"] for d in daily_30]

    has_data = hist_revenue > 0 or orders_today > 0

    return templates.TemplateResponse("overview.html", {
        "request": request,
        "page": "overview",
        "now": datetime.now().strftime("%d %b %Y · %H:%M"),
        "roas": roas,
        "roas_color": roas_color,
        "mer": mer,
        "mer_color": mer_color,
        "total_spend_mxn": meta_spend,
        "total_revenue_mxn": hist_revenue,
        "total_purchases": hist_orders,
        "aov_mxn": aov_mxn,
        "meta_ctr": meta_ctr,
        "meta_cpa_mxn": 0,
        "orders_today": orders_today,
        "revenue_today": revenue_today,
        "hist_orders": hist_orders,
        "hist_revenue": hist_revenue,
        "customer_count": customer_count,
        "pending_ugc": pending_count,
        "current_week": current_week,
        "decisions": decisions,
        "phase": state.get("phase", "—"),
        "budget_remaining": budget_remaining,
        "budget_total": budget_total,
        "budget_pct": budget_pct,
        "meta_source": meta_data.get("source", "unavailable"),
        "kpis_date": datetime.now().strftime("%d %b"),
        "fetched_at": datetime.now().strftime("%H:%M"),
        "has_data": has_data,
        "chart_labels": chart_labels,
        "chart_revenue": chart_revenue,
        "chart_orders": chart_orders,
        "abandoned_count": abandoned.get("count", 0),
        "revenue_at_risk": abandoned.get("revenue_at_risk", 0),
        "unfulfilled_count": refunds.get("unfulfilled_count", 0),
        "unfulfilled_revenue": refunds.get("unfulfilled_revenue", 0),
        # Week-over-week
        "revenue_wow": revenue_wow,
        "orders_wow": orders_wow,
        "this_week_revenue": this_week_revenue,
        "prev_week_revenue": prev_week_revenue,
        # Meta extras
        "total_reach": total_reach,
        "avg_frequency": avg_frequency,
        # Meta velocity deltas (7d vs prev 7d)
        "spend_delta": spend_delta,
        "roas_delta": roas_delta,
        "ctr_delta": ctr_delta,
        "cpm_delta": cpm_delta,
        "reach_delta": reach_delta,
    })


@router.get("/api/overview/stats", response_class=HTMLResponse)
async def overview_stats(request: Request):
    today, hist, meta_data, _, _ = await asyncio.gather(
        get_orders_today(),
        get_orders_historical(days=90),
        get_campaigns(),
        get_customer_count(),
        get_abandoned_checkouts(),
    )
    live_campaigns = meta_data.get("campaigns", [])
    meta_spend = sum(c.get("spend_mxn", 0) for c in live_campaigns)
    hist_revenue = hist.get("revenue_mxn", 0)
    roas = round(hist_revenue / meta_spend, 2) if meta_spend > 0 else 0
    roas_color = "green" if roas >= 1.2 else ("yellow" if roas >= 0.8 else ("red" if roas > 0 else ""))

    return templates.TemplateResponse("partials/stats_cards.html", {
        "request": request,
        "roas": roas,
        "roas_color": roas_color,
        "total_spend_mxn": meta_spend,
        "total_revenue_mxn": hist_revenue,
        "orders_today": today.get("count", 0),
        "revenue_today": today.get("revenue_mxn", 0),
        "hist_orders": hist.get("count", 0),
        "fetched_at": datetime.now().strftime("%H:%M"),
    })


@router.get("/api/events")
async def sse_events(request: Request):
    """Server-Sent Events stream — pushes order/event notifications to connected browsers."""
    q = sse_service.subscribe()

    async def stream():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=25)
                    yield f"data: {json.dumps(msg)}\n\n"
                except asyncio.TimeoutError:
                    yield ": heartbeat\n\n"
        finally:
            sse_service.unsubscribe(q)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/api/latest-order")
async def latest_order():
    """Polling endpoint — browser checks every 3s for new order events."""
    return JSONResponse(sse_service.get_last_event())
