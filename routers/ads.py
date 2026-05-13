import asyncio
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from services.meta_service import get_campaigns, get_campaigns_daily, get_adsets, get_ads_insights, get_placement_breakdown, read_agent_state

templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))
router = APIRouter()

ROAS_TARGET = 1.2
CPA_MAX_MXN = 340
CTR_MIN = 1.0
FREQ_FATIGUE = 3.5

COGS_MXN = 26 * 17.3
UNIT_PRICE_MXN = 1299
BREAKEVEN_ROAS = round(UNIT_PRICE_MXN / (UNIT_PRICE_MXN - COGS_MXN), 2)


def _classify_campaign(name: str) -> str:
    n = name.lower()
    if any(x in n for x in ["asc", "advantage", "shopping"]): return "ASC"
    if any(x in n for x in ["test", "prueba", "creativ"]): return "Testing"
    if any(x in n for x in ["retarget", "remarketing", " rt ", "retar"]): return "Retargeting"
    return "Prospecting"


@router.get("/", response_class=HTMLResponse)
async def ads_page(request: Request):
    state = read_agent_state()
    if state.get("_mock"):
        state = {}
    try:
        meta_data, daily_data, adsets_data, ads_data, placement_data = await asyncio.gather(
            get_campaigns(),
            get_campaigns_daily(days=14),
            get_adsets(),
            get_ads_insights(),
            get_placement_breakdown(),
        )
    except Exception:
        meta_data = {"campaigns": [], "source": "error"}
        daily_data = {"labels": [], "spend": [], "revenue": [], "roas": [], "ctr": [], "cpm": [], "reach": [], "frequency": [], "hook_rate": []}
        adsets_data = {"adsets": []}
        ads_data = {"ads": []}
        placement_data = {"placements": []}

    live_campaigns = meta_data.get("campaigns", [])
    data_source = meta_data.get("source", "unavailable")
    fetched_at = meta_data.get("fetched_at", "")

    # Semáforos por campaña
    def _semaforo(obj: dict):
        obj["roas_color"] = "green" if obj.get("roas", 0) >= ROAS_TARGET else ("yellow" if obj.get("roas", 0) >= 0.8 else "red")
        obj["ctr_color"] = "green" if obj.get("ctr_pct", 0) >= CTR_MIN else ("yellow" if obj.get("ctr_pct", 0) >= 0.7 else "red")
        obj["cpa_color"] = "green" if 0 < obj.get("cpa_mxn", 9999) <= CPA_MAX_MXN else ("red" if obj.get("cpa_mxn", 0) > CPA_MAX_MXN else "")
        freq = obj.get("frequency", 0)
        obj["freq_color"] = "red" if freq >= FREQ_FATIGUE else ("yellow" if freq >= 2.5 else "green")

    for c in live_campaigns:
        _semaforo(c)
        qr = c.get("quality_ranking", "—")
        c["quality_badge"] = "green" if qr == "ABOVE_AVERAGE" else ("red" if qr == "BELOW_AVERAGE" else "")

    for a in adsets_data.get("adsets", []):
        _semaforo(a)

    # Aggregate KPIs
    total_spend = sum(c.get("spend_mxn", 0) for c in live_campaigns)
    total_revenue = sum(c.get("revenue_mxn", 0) for c in live_campaigns)
    total_purchases = sum(c.get("purchases", 0) for c in live_campaigns)
    total_impressions = sum(c.get("impressions", 0) for c in live_campaigns)
    total_clicks = sum(c.get("clicks", 0) for c in live_campaigns)
    total_reach = sum(c.get("reach", 0) for c in live_campaigns)
    total_video_plays = sum(c.get("video_plays", 0) for c in live_campaigns)
    total_thruplays = sum(c.get("video_thruplays", 0) for c in live_campaigns)
    avg_watch = (sum(c.get("video_avg_watch_sec", 0) for c in live_campaigns) / len(live_campaigns)) if live_campaigns else 0

    global_roas = round(total_revenue / total_spend, 2) if total_spend > 0 else 0
    meta_ctr = round(total_clicks / total_impressions * 100, 2) if total_impressions > 0 else 0
    meta_cpm = round(total_spend / total_impressions * 1000, 0) if total_impressions > 0 else 0
    meta_cpa = round(total_spend / total_purchases, 0) if total_purchases > 0 else 0
    meta_cpc = round(total_spend / total_clicks, 2) if total_clicks > 0 else 0
    avg_frequency = round(sum(c.get("frequency", 0) for c in live_campaigns) / len(live_campaigns), 2) if live_campaigns else 0
    global_hook_rate = round(total_video_plays / total_impressions * 100, 1) if total_impressions > 0 and total_video_plays > 0 else 0
    global_hold_rate = round(total_thruplays / total_video_plays * 100, 1) if total_video_plays > 0 else 0
    has_video = total_video_plays > 0

    # Funnel aggregates
    total_view_content = sum(c.get("view_content", 0) for c in live_campaigns)
    total_atc = sum(c.get("add_to_cart", 0) for c in live_campaigns)
    total_init_checkout = sum(c.get("init_checkout", 0) for c in live_campaigns)
    total_add_payment = sum(c.get("add_payment_info", 0) for c in live_campaigns)

    # Funnel costs — weighted average across campaigns (use spend-weighted mean)
    def _wavg_cost(field: str) -> float:
        vals = [(c.get(field, 0), c.get("spend_mxn", 0)) for c in live_campaigns if c.get(field, 0) > 0]
        total_w = sum(w for _, w in vals)
        return round(sum(v * w for v, w in vals) / total_w, 0) if total_w > 0 else 0

    # Fallback: compute directly from aggregates if per-campaign cost_per fields are 0
    cost_per_view_content = _wavg_cost("cost_per_view_content") or (round(total_spend / total_view_content, 0) if total_view_content > 0 else 0)
    cost_per_atc = _wavg_cost("cost_per_atc") or (round(total_spend / total_atc, 0) if total_atc > 0 else 0)
    cost_per_init_checkout = _wavg_cost("cost_per_init_checkout") or (round(total_spend / total_init_checkout, 0) if total_init_checkout > 0 else 0)
    cost_per_purchase = meta_cpa  # already computed above

    # Funnel conversion rates (aggregate level)
    funnel_imp_to_click = round(total_clicks / total_impressions * 100, 2) if total_impressions > 0 else 0
    funnel_click_to_vc = round(total_view_content / total_clicks * 100, 1) if total_clicks > 0 else 0
    funnel_vc_to_atc = round(total_atc / total_view_content * 100, 1) if total_view_content > 0 else 0
    funnel_atc_to_ic = round(total_init_checkout / total_atc * 100, 1) if total_atc > 0 else 0
    funnel_ic_to_pur = round(total_purchases / total_init_checkout * 100, 1) if total_init_checkout > 0 else 0
    funnel_overall = round(total_purchases / total_clicks * 100, 2) if total_clicks > 0 else 0

    budget_total = state.get("budget_total", 5000)
    budget_remaining = max(0, budget_total - total_spend)
    budget_used = total_spend
    budget_pct = int(budget_used / budget_total * 100) if budget_total > 0 else 0

    # Budget distribution by campaign type (A3)
    budget_by_type: dict = {"ASC": 0.0, "Testing": 0.0, "Retargeting": 0.0, "Prospecting": 0.0}
    for c in live_campaigns:
        t = _classify_campaign(c.get("name", ""))
        budget_by_type[t] = budget_by_type.get(t, 0) + c.get("spend_mxn", 0)
    budget_type_labels = list(budget_by_type.keys())
    budget_type_values = [round(v, 0) for v in budget_by_type.values()]

    # Creative Scorecard classification (C1)
    for ad in ads_data.get("ads", []):
        hook = ad.get("hook_rate_pct", 0)
        cpa = ad.get("cpa_mxn", 9999)
        if hook > 30 and 0 < cpa <= CPA_MAX_MXN:
            ad["scorecard_status"] = "escalar"
            ad["scorecard_color"] = "green"
        elif hook < 15 or (cpa > CPA_MAX_MXN * 1.5 and cpa > 0):
            ad["scorecard_status"] = "matar"
            ad["scorecard_color"] = "red"
        else:
            ad["scorecard_status"] = "iterar"
            ad["scorecard_color"] = "yellow"

    alerts = []
    if meta_cpa > CPA_MAX_MXN and meta_cpa > 0:
        alerts.append({"type": "red", "msg": f"CPA ${meta_cpa:.0f} MXN supera límite de ${CPA_MAX_MXN}"})
    if 0 < meta_ctr < CTR_MIN:
        alerts.append({"type": "yellow", "msg": f"CTR {meta_ctr}% por debajo del target {CTR_MIN}%"})
    if avg_frequency >= FREQ_FATIGUE:
        alerts.append({"type": "yellow", "msg": f"Frecuencia promedio {avg_frequency:.1f}x — posible fatiga creativa"})
    fatigue_campaigns = [c["name"] for c in live_campaigns if c.get("ad_fatigue")]
    if fatigue_campaigns:
        alerts.append({"type": "yellow", "msg": f"Fatiga en: {', '.join(fatigue_campaigns[:3])}"})
    # B1: Dynamic hook rate decay alert
    daily_hook = daily_data.get("hook_rate", [])
    if len(daily_hook) >= 3:
        recent_nonzero = [v for v in daily_hook[-3:] if v > 0]
        if len(recent_nonzero) >= 2 and (recent_nonzero[0] - recent_nonzero[-1]) >= 5:
            drop = round(recent_nonzero[0] - recent_nonzero[-1], 1)
            alerts.append({"type": "yellow", "msg": f"Hook Rate cayó {drop}pp en los últimos 3 días — posible fatiga de creativo"})

    decisions = state.get("decisions_log", [])[::-1][:10]

    if fetched_at and "T" in fetched_at:
        fetched_display = fetched_at[11:16]
    else:
        fetched_display = datetime.now().strftime("%H:%M")

    # Daily chart data
    daily_labels = daily_data.get("labels", [])

    # Adsets semáforos
    for a in adsets_data.get("adsets", []):
        a["roas_color"] = "green" if a.get("roas", 0) >= ROAS_TARGET else ("yellow" if a.get("roas", 0) >= 0.8 else "red")
        a["ctr_color"] = "green" if a.get("ctr_pct", 0) >= CTR_MIN else ("yellow" if a.get("ctr_pct", 0) >= 0.7 else "red")
        a["freq_color"] = "red" if a.get("frequency", 0) >= FREQ_FATIGUE else ("yellow" if a.get("frequency", 0) >= 2.5 else "green")

    return templates.TemplateResponse("ads.html", {
        "request": request,
        "page": "ads",
        "phase": state.get("phase", "—"),
        "live_campaigns": live_campaigns,
        "decisions": decisions,
        # KPIs fila 1
        "global_roas": global_roas,
        "total_spend_mxn": total_spend,
        "total_revenue_mxn": total_revenue,
        "meta_ctr": meta_ctr,
        "meta_cpm": meta_cpm,
        # KPIs fila 2
        "total_reach": total_reach,
        "avg_frequency": avg_frequency,
        "meta_cpc": meta_cpc,
        "meta_cpa": meta_cpa,
        "total_purchases": total_purchases,
        # Video
        "has_video": has_video,
        "global_hook_rate": global_hook_rate,
        "global_hold_rate": global_hold_rate,
        "total_video_plays": total_video_plays,
        "total_thruplays": total_thruplays,
        "avg_watch_sec": round(avg_watch, 1),
        # Budget
        "budget_total": budget_total,
        "budget_remaining": budget_remaining,
        "budget_used": budget_used,
        "budget_pct": budget_pct,
        # Targets
        "roas_target": ROAS_TARGET,
        "cpa_max": CPA_MAX_MXN,
        "ctr_min": CTR_MIN,
        "freq_fatigue": FREQ_FATIGUE,
        "breakeven_roas": BREAKEVEN_ROAS,
        # Alerts
        "alerts": alerts,
        # Source
        "data_source": data_source,
        "fetched_at": fetched_display,
        # Daily trend charts
        "daily_labels": daily_labels,
        "daily_spend": daily_data.get("spend", []),
        "daily_revenue": daily_data.get("revenue", []),
        "daily_roas": daily_data.get("roas", []),
        "daily_ctr": daily_data.get("ctr", []),
        "daily_cpm": daily_data.get("cpm", []),
        "daily_reach": daily_data.get("reach", []),
        "daily_frequency": daily_data.get("frequency", []),
        # Adsets
        "adsets": adsets_data.get("adsets", []),
        # Budget by campaign type (A3)
        "budget_type_labels": budget_type_labels,
        "budget_type_values": budget_type_values,
        # Creative Scorecard (C1)
        "creative_ads": ads_data.get("ads", []),
        # Placement breakdown (C2)
        "placements": placement_data.get("placements", []),
        # Funnel
        "total_impressions": total_impressions,
        "total_clicks": total_clicks,
        "total_view_content": total_view_content,
        "total_atc": total_atc,
        "total_init_checkout": total_init_checkout,
        "total_add_payment": total_add_payment,
        "cost_per_view_content": cost_per_view_content,
        "cost_per_atc": cost_per_atc,
        "cost_per_init_checkout": cost_per_init_checkout,
        "cost_per_purchase": cost_per_purchase,
        "funnel_imp_to_click": funnel_imp_to_click,
        "funnel_click_to_vc": funnel_click_to_vc,
        "funnel_vc_to_atc": funnel_vc_to_atc,
        "funnel_atc_to_ic": funnel_atc_to_ic,
        "funnel_ic_to_pur": funnel_ic_to_pur,
        "funnel_overall": funnel_overall,
    })


@router.post("/clear-cache")
async def clear_ads_cache():
    """Borra el cache de Meta para forzar datos frescos."""
    import os
    cache_dir = Path(__file__).parent.parent / "cache"
    for key in ["meta_campaigns", "meta_adsets", "meta_ads_insights", "meta_placement_7d", "meta_campaigns_daily_14"]:
        f = cache_dir / f"{key}.json"
        if f.exists():
            f.unlink()
    return JSONResponse({"ok": True})
