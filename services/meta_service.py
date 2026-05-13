import json
import os
import httpx
from datetime import datetime, timedelta
from pathlib import Path
from services.cache import get_cached, set_cached

LEVIA_DIR = Path(__file__).parent.parent.parent
AGENT_STATE_PATH = LEVIA_DIR / "11_MARKETING_AGENCY" / "agent_state.json"

META_TOKEN = os.getenv("META_ACCESS_TOKEN", "")
META_ACCOUNT = os.getenv("META_AD_ACCOUNT_ID", "")
GRAPH = "https://graph.facebook.com/v19.0"

INSIGHTS_FIELDS = ",".join([
    "campaign_name", "campaign_id",
    "spend", "impressions", "clicks", "unique_clicks",
    "actions", "purchase_roas",
    "cpm", "cpc", "ctr", "unique_ctr",
    "reach", "frequency",
    "cost_per_result",
    "cost_per_action_type",
    "video_avg_time_watched_actions",
    "video_thruplay_actions",
    "video_play_actions",
    "inline_link_click_ctr",
    "quality_ranking",
    "engagement_rate_ranking",
    "conversion_rate_ranking",
])


def read_agent_state() -> dict:
    try:
        return json.loads(AGENT_STATE_PATH.read_text())
    except Exception:
        return {}


def _parse_insight_row(row: dict) -> dict:
    """Parse a single insight row into clean metrics dict."""

    def _action_sum(actions_list, *action_types):
        total = 0.0
        for a in (actions_list or []):
            if a.get("action_type") in action_types:
                total += float(a.get("value", 0))
        return total

    def _cpat(cpat_list, *action_types):
        """Get cost per action type from cost_per_action_type array."""
        for a in (cpat_list or []):
            if a.get("action_type") in action_types:
                return float(a.get("value", 0))
        return 0.0

    actions = row.get("actions", [])
    cpat = row.get("cost_per_action_type", [])

    purchases = int(_action_sum(actions, "purchase", "offsite_conversion.fb_pixel_purchase"))
    roas_list = row.get("purchase_roas", [])
    roas = float(roas_list[0].get("value", 0)) if roas_list else 0.0
    spend = float(row.get("spend", 0))
    impressions = int(row.get("impressions", 0))
    clicks = int(row.get("clicks", 0))
    unique_clicks = int(row.get("unique_clicks", 0))
    reach = int(row.get("reach", 0))
    frequency = float(row.get("frequency", 0))
    cpm = float(row.get("cpm", 0))
    cpc = float(row.get("cpc", 0))
    ctr_raw = row.get("ctr", 0)
    ctr = float(ctr_raw) * 100 if ctr_raw and float(ctr_raw) < 1 else (float(ctr_raw) if ctr_raw else
          round(clicks / impressions * 100, 2) if impressions > 0 else 0)
    unique_ctr = float(row.get("unique_ctr", 0) or 0)
    if unique_ctr < 1 and unique_ctr > 0:
        unique_ctr *= 100
    cpa = round(spend / purchases, 2) if purchases > 0 else 0
    cost_per_result_raw = row.get("cost_per_result") or 0
    cost_per_result = float(cost_per_result_raw.get("value", 0)) if isinstance(cost_per_result_raw, dict) else float(cost_per_result_raw or 0)

    # ── Full funnel actions ──────────────────────────────────────────────────
    view_content   = int(_action_sum(actions, "view_content", "offsite_conversion.fb_pixel_view_content"))
    add_to_cart    = int(_action_sum(actions, "add_to_cart", "offsite_conversion.fb_pixel_add_to_cart"))
    init_checkout  = int(_action_sum(actions, "initiate_checkout", "offsite_conversion.fb_pixel_initiate_checkout"))
    add_payment    = int(_action_sum(actions, "add_payment_info", "offsite_conversion.fb_pixel_add_payment_info"))
    leads          = int(_action_sum(actions, "lead", "offsite_conversion.fb_pixel_lead"))
    link_clicks    = int(_action_sum(actions, "link_click"))
    post_engagement = int(_action_sum(actions, "post_engagement"))
    page_engagement = int(_action_sum(actions, "page_engagement"))
    comments        = int(_action_sum(actions, "comment"))
    shares          = int(_action_sum(actions, "post"))
    reactions       = int(_action_sum(actions, "like", "rsvp"))

    # ── Funnel costs (from cost_per_action_type) ─────────────────────────────
    cost_per_view_content  = _cpat(cpat, "view_content", "offsite_conversion.fb_pixel_view_content")
    cost_per_atc           = _cpat(cpat, "add_to_cart", "offsite_conversion.fb_pixel_add_to_cart")
    cost_per_init_checkout = _cpat(cpat, "initiate_checkout", "offsite_conversion.fb_pixel_initiate_checkout")
    cost_per_add_payment   = _cpat(cpat, "add_payment_info", "offsite_conversion.fb_pixel_add_payment_info")
    cost_per_lead          = _cpat(cpat, "lead", "offsite_conversion.fb_pixel_lead")

    # Fallback calc if cost_per_action_type not available
    if cost_per_atc == 0 and add_to_cart > 0 and spend > 0:
        cost_per_atc = round(spend / add_to_cart, 2)
    if cost_per_init_checkout == 0 and init_checkout > 0 and spend > 0:
        cost_per_init_checkout = round(spend / init_checkout, 2)
    if cost_per_view_content == 0 and view_content > 0 and spend > 0:
        cost_per_view_content = round(spend / view_content, 2)

    # ── Funnel conversion rates ──────────────────────────────────────────────
    ctr_to_vc    = round(view_content / clicks * 100, 1) if clicks > 0 and view_content > 0 else 0
    vc_to_atc    = round(add_to_cart / view_content * 100, 1) if view_content > 0 else 0
    atc_to_ic    = round(init_checkout / add_to_cart * 100, 1) if add_to_cart > 0 else 0
    ic_to_pur    = round(purchases / init_checkout * 100, 1) if init_checkout > 0 else 0
    click_to_pur = round(purchases / clicks * 100, 2) if clicks > 0 and purchases > 0 else 0
    impression_to_pur = round(purchases / impressions * 100, 3) if impressions > 0 and purchases > 0 else 0

    # ── Video metrics ────────────────────────────────────────────────────────
    def _avt(actions_list, action_type):
        for a in (actions_list or []):
            if a.get("action_type") == action_type:
                return float(a.get("value", 0))
        return 0.0

    video_plays = _avt(row.get("video_play_actions"), "video_view")
    video_thruplays = _avt(row.get("video_thruplay_actions"), "video_view")
    video_avg_watch = _avt(row.get("video_avg_time_watched_actions"), "video_view")

    hook_rate = round(video_plays / impressions * 100, 1) if impressions > 0 and video_plays > 0 else 0
    hold_rate = round(video_thruplays / video_plays * 100, 1) if video_plays > 0 else 0
    inline_ctr = float(row.get("inline_link_click_ctr", 0) or 0)
    if inline_ctr > 0 and inline_ctr < 1:
        inline_ctr *= 100

    ad_fatigue = frequency >= 3.5 and ctr < 0.7

    return {
        # Base
        "spend_mxn": spend,
        "impressions": impressions,
        "clicks": clicks,
        "unique_clicks": unique_clicks,
        "link_clicks": link_clicks,
        "reach": reach,
        "frequency": round(frequency, 2),
        "purchases": purchases,
        "roas": round(roas, 2),
        "ctr_pct": round(ctr, 2),
        "unique_ctr_pct": round(unique_ctr, 2),
        "cpa_mxn": cpa,
        "cpm_mxn": round(cpm, 2),
        "cpc_mxn": round(cpc, 2),
        "cost_per_result": round(cost_per_result, 2),
        "revenue_mxn": round(spend * roas, 2) if roas else 0,
        # Funnel counts
        "view_content": view_content,
        "add_to_cart": add_to_cart,
        "init_checkout": init_checkout,
        "add_payment_info": add_payment,
        "leads": leads,
        # Funnel costs
        "cost_per_view_content": round(cost_per_view_content, 2),
        "cost_per_atc": round(cost_per_atc, 2),
        "cost_per_init_checkout": round(cost_per_init_checkout, 2),
        "cost_per_add_payment": round(cost_per_add_payment, 2),
        "cost_per_lead": round(cost_per_lead, 2),
        # Funnel conversion rates
        "ctr_to_vc_pct": ctr_to_vc,
        "vc_to_atc_pct": vc_to_atc,
        "atc_to_ic_pct": atc_to_ic,
        "ic_to_pur_pct": ic_to_pur,
        "click_to_pur_pct": click_to_pur,
        "impression_to_pur_pct": impression_to_pur,
        # Engagement
        "post_engagement": post_engagement,
        "page_engagement": page_engagement,
        "comments": comments,
        "shares": shares,
        "reactions": reactions,
        # Video
        "video_plays": int(video_plays),
        "video_thruplays": int(video_thruplays),
        "video_avg_watch_sec": round(video_avg_watch, 1),
        "hook_rate_pct": hook_rate,
        "hold_rate_pct": hold_rate,
        "inline_ctr_pct": round(inline_ctr, 2),
        # Rankings
        "quality_ranking": row.get("quality_ranking", "—"),
        "engagement_rate_ranking": row.get("engagement_rate_ranking", "—"),
        "conversion_rate_ranking": row.get("conversion_rate_ranking", "—"),
        "ad_fatigue": ad_fatigue,
    }


async def get_campaigns() -> dict:
    cached = get_cached("meta_campaigns", ttl_seconds=120)
    if cached:
        return cached

    if META_TOKEN and META_ACCOUNT:
        result = await _fetch_meta_live()
        if result:
            set_cached("meta_campaigns", result)
            return result

    return {"campaigns": [], "source": "unavailable", "fetched_at": datetime.now().isoformat()}


async def _fetch_meta_live() -> dict | None:
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            camps_url = (
                f"{GRAPH}/{META_ACCOUNT}/campaigns"
                f"?fields=id,name,status,daily_budget,effective_status,learning_stage_info"
                f"&access_token={META_TOKEN}"
            )
            cr = await client.get(camps_url)
            if cr.status_code != 200:
                return None
            camp_list = cr.json().get("data", [])

            insights_url = (
                f"{GRAPH}/{META_ACCOUNT}/insights"
                f"?fields={INSIGHTS_FIELDS}"
                f"&date_preset=last_7d&level=campaign&limit=20"
                f"&access_token={META_TOKEN}"
            )
            ir = await client.get(insights_url)
            insights_by_id: dict = {}
            if ir.status_code == 200:
                for row in ir.json().get("data", []):
                    cid = row.get("campaign_id", "")
                    insights_by_id[cid] = _parse_insight_row(row)

        _camp_defaults = {
            "spend_mxn": 0.0, "impressions": 0, "clicks": 0, "unique_clicks": 0,
            "link_clicks": 0, "reach": 0, "frequency": 0.0, "purchases": 0,
            "roas": 0.0, "ctr_pct": 0.0, "unique_ctr_pct": 0.0, "cpa_mxn": 0,
            "cpm_mxn": 0.0, "cpc_mxn": 0.0, "cost_per_result": 0.0, "revenue_mxn": 0.0,
            "view_content": 0, "add_to_cart": 0, "init_checkout": 0,
            "add_payment_info": 0, "leads": 0, "video_plays": 0, "video_thruplays": 0,
            "video_avg_watch_sec": 0.0, "hook_rate_pct": 0.0, "hold_rate_pct": 0.0,
            "inline_ctr_pct": 0.0, "ad_fatigue": False, "quality_ranking": "—",
            "engagement_ranking": "—", "conversion_ranking": "—",
            "cost_per_view_content": 0, "cost_per_atc": 0, "cost_per_init_checkout": 0,
            "cost_per_add_payment": 0, "cost_per_lead": 0,
        }

        campaigns = []
        for c in camp_list:
            cid = c.get("id", "")
            ins = {**_camp_defaults, **insights_by_id.get(cid, {})}
            campaigns.append({
                "name": c.get("name", "—"),
                "status": c.get("effective_status", c.get("status", "UNKNOWN")),
                "learning_phase": c.get("learning_stage_info", {}).get("status", ""),
                "platform": "meta",
                "updated_at": datetime.now().strftime("%H:%M"),
                **ins,
            })

        return {"campaigns": campaigns, "source": "live", "fetched_at": datetime.now().isoformat()}
    except Exception:
        return None


async def get_campaigns_daily(days: int = 14) -> dict:
    """Daily time-series for trend charts: spend, revenue, ROAS, CTR, reach, frequency."""
    cache_key = f"meta_campaigns_daily_{days}"
    cached = get_cached(cache_key, ttl_seconds=1800)
    if cached:
        return cached

    if not (META_TOKEN and META_ACCOUNT):
        return {"labels": [], "spend": [], "revenue": [], "roas": [], "ctr": [], "cpm": [], "reach": [], "frequency": [], "video_plays": [], "video_thruplays": [], "hook_rate": [], "source": "unavailable"}

    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
    until = datetime.now().strftime("%Y-%m-%d")

    fields = "spend,impressions,clicks,actions,purchase_roas,cpm,reach,frequency,video_play_actions,video_thruplay_actions"
    import json as _json
    daily_params = {
        "fields": fields,
        "time_range": _json.dumps({"since": since, "until": until}),
        "time_increment": "1",
        "level": "account",
        "limit": "50",
        "access_token": META_TOKEN,
    }

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(f"{GRAPH}/{META_ACCOUNT}/insights", params=daily_params)
            if r.status_code != 200:
                raise ValueError(f"Meta API error {r.status_code}: {r.text[:200]}")
            data = r.json().get("data", [])

        def _daily_video(actions_list, action_type):
            for a in (actions_list or []):
                if a.get("action_type") == action_type:
                    return float(a.get("value", 0))
            return 0.0

        # Index by date
        by_date: dict = {}
        for row in data:
            date = row.get("date_start", "")[:10]
            spend = float(row.get("spend", 0))
            impressions = int(row.get("impressions", 0))
            clicks = int(row.get("clicks", 0))
            roas_list = row.get("purchase_roas", [])
            roas = float(roas_list[0].get("value", 0)) if roas_list else 0.0
            cpm = float(row.get("cpm", 0))
            reach = int(row.get("reach", 0))
            frequency = float(row.get("frequency", 0))
            ctr = round(clicks / impressions * 100, 2) if impressions > 0 else 0
            video_plays = _daily_video(row.get("video_play_actions"), "video_view")
            video_thruplays = _daily_video(row.get("video_thruplay_actions"), "video_view")
            hook_rate = round(video_plays / impressions * 100, 1) if impressions > 0 and video_plays > 0 else 0
            by_date[date] = {
                "spend": round(spend, 0),
                "revenue": round(spend * roas, 0),
                "roas": round(roas, 2),
                "ctr": ctr,
                "cpm": round(cpm, 0),
                "reach": reach,
                "frequency": round(frequency, 2),
                "video_plays": int(video_plays),
                "video_thruplays": int(video_thruplays),
                "hook_rate": hook_rate,
                "impressions": impressions,
            }

        # Build filled array
        labels, spend_arr, revenue_arr, roas_arr, ctr_arr, cpm_arr, reach_arr, freq_arr = [], [], [], [], [], [], [], []
        video_plays_arr, video_thruplays_arr, hook_rate_arr, impressions_arr = [], [], [], []
        for i in range(days):
            date = (datetime.now() - timedelta(days=days - 1 - i)).strftime("%Y-%m-%d")
            d = by_date.get(date, {})
            labels.append(date[5:])  # MM-DD
            spend_arr.append(d.get("spend", 0))
            revenue_arr.append(d.get("revenue", 0))
            roas_arr.append(d.get("roas", 0))
            ctr_arr.append(d.get("ctr", 0))
            cpm_arr.append(d.get("cpm", 0))
            reach_arr.append(d.get("reach", 0))
            freq_arr.append(d.get("frequency", 0))
            video_plays_arr.append(d.get("video_plays", 0))
            video_thruplays_arr.append(d.get("video_thruplays", 0))
            hook_rate_arr.append(d.get("hook_rate", 0))
            impressions_arr.append(d.get("impressions", 0))

        result = {
            "labels": labels, "spend": spend_arr, "revenue": revenue_arr,
            "roas": roas_arr, "ctr": ctr_arr, "cpm": cpm_arr,
            "reach": reach_arr, "frequency": freq_arr,
            "video_plays": video_plays_arr, "video_thruplays": video_thruplays_arr,
            "hook_rate": hook_rate_arr, "impressions": impressions_arr,
            "source": "live",
        }
        set_cached(cache_key, result)
        return result
    except Exception:
        return {"labels": [], "spend": [], "revenue": [], "roas": [], "ctr": [], "cpm": [], "reach": [], "frequency": [], "video_plays": [], "video_thruplays": [], "hook_rate": [], "source": "error"}


async def get_adsets() -> dict:
    """Adset-level breakdown with spend, reach, CTR, ROAS per audience/targeting group."""
    cached = get_cached("meta_adsets", ttl_seconds=120)
    if cached:
        return cached

    if not (META_TOKEN and META_ACCOUNT):
        return {"adsets": [], "source": "unavailable"}

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            # Get adsets
            adsets_url = (
                f"{GRAPH}/{META_ACCOUNT}/adsets"
                f"?fields=id,name,status,daily_budget,effective_status"
                f"&access_token={META_TOKEN}&limit=50"
            )
            ar = await client.get(adsets_url)
            if ar.status_code != 200:
                return {"adsets": [], "source": "unavailable"}
            adset_list = ar.json().get("data", [])

            # Get insights per adset
            ins_url = (
                f"{GRAPH}/{META_ACCOUNT}/insights"
                f"?fields=adset_id,adset_name,spend,impressions,clicks,reach,frequency,cpm,ctr,actions,purchase_roas"
                f"&date_preset=last_7d&level=adset&limit=50"
                f"&access_token={META_TOKEN}"
            )
            ir = await client.get(ins_url)
            ins_by_id: dict = {}
            if ir.status_code == 200:
                for row in ir.json().get("data", []):
                    aid = row.get("adset_id", "")
                    purchases = sum(int(a.get("value", 0)) for a in row.get("actions", []) if a.get("action_type") == "purchase")
                    roas_list = row.get("purchase_roas", [])
                    roas = float(roas_list[0].get("value", 0)) if roas_list else 0.0
                    spend = float(row.get("spend", 0))
                    impressions = int(row.get("impressions", 0))
                    clicks = int(row.get("clicks", 0))
                    reach = int(row.get("reach", 0))
                    frequency = float(row.get("frequency", 0))
                    cpm = float(row.get("cpm", 0))
                    ctr = round(clicks / impressions * 100, 2) if impressions > 0 else 0
                    ins_by_id[aid] = {
                        "spend_mxn": spend, "impressions": impressions,
                        "clicks": clicks, "reach": reach,
                        "frequency": round(frequency, 2), "cpm_mxn": round(cpm, 2),
                        "ctr_pct": ctr, "roas": round(roas, 2), "purchases": purchases,
                        "revenue_mxn": round(spend * roas, 2),
                    }

        _ins_defaults = {
            "spend_mxn": 0.0, "impressions": 0, "clicks": 0, "reach": 0,
            "frequency": 0.0, "cpm_mxn": 0.0, "ctr_pct": 0.0,
            "roas": 0.0, "purchases": 0, "revenue_mxn": 0.0,
        }

        adsets = []
        for a in adset_list:
            aid = a.get("id", "")
            ins = {**_ins_defaults, **ins_by_id.get(aid, {})}
            budget = int(a.get("daily_budget", 0)) / 100 if a.get("daily_budget") else 0
            score = _efficiency_score(ins.get("roas", 0), ins.get("ctr_pct", 0), ins.get("frequency", 0))
            adsets.append({
                "name": a.get("name", "—"),
                "status": a.get("effective_status", "—"),
                "budget_day": round(budget, 0),
                "efficiency_score": score,
                **ins,
            })

        result = {"adsets": adsets, "source": "live" if adsets else "empty"}
        set_cached("meta_adsets", result)
        return result
    except Exception:
        return {"adsets": [], "source": "error"}


def _efficiency_score(roas: float, ctr: float, frequency: float) -> int:
    """0-100 composite score: ROAS 50%, CTR 30%, Fatigue penalty 20%."""
    roas_score = min(roas / 3.0 * 50, 50)
    ctr_score = min(ctr / 3.0 * 30, 30)
    fatigue_penalty = max(0, (frequency - 2.0) * 5)
    return max(0, min(100, int(roas_score + ctr_score - fatigue_penalty)))


async def get_ads_insights() -> dict:
    """Ad-level insights for Creative Scorecard (Nacho Leo methodology)."""
    cached = get_cached("meta_ads_insights", ttl_seconds=120)
    if cached:
        return cached

    if not (META_TOKEN and META_ACCOUNT):
        return {"ads": [], "source": "unavailable"}

    ADS_FIELDS = ",".join([
        "ad_id", "ad_name", "spend", "impressions", "clicks",
        "actions", "purchase_roas", "ctr", "cpm",
        "video_play_actions", "video_thruplay_actions",
        "video_avg_time_watched_actions",
        "quality_ranking", "engagement_rate_ranking",
    ])

    url = (
        f"{GRAPH}/{META_ACCOUNT}/insights"
        f"?fields={ADS_FIELDS}"
        f"&date_preset=last_7d&level=ad&limit=50"
        f"&access_token={META_TOKEN}"
    )

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url)
            if r.status_code != 200:
                return {"ads": [], "source": "error"}
            data = r.json().get("data", [])

        ads = []
        for row in data:
            ins = _parse_insight_row(row)
            ads.append({
                "name": row.get("ad_name", "—"),
                "ad_id": row.get("ad_id", ""),
                **ins,
            })

        ads.sort(key=lambda x: x.get("hook_rate_pct", 0), reverse=True)
        result = {"ads": ads, "source": "live" if ads else "empty"}
        set_cached("meta_ads_insights", result)
        return result
    except Exception:
        return {"ads": [], "source": "error"}


async def get_placement_breakdown() -> dict:
    """Spend/performance by publisher platform and placement."""
    cached = get_cached("meta_placement_7d", ttl_seconds=1800)
    if cached:
        return cached

    if not (META_TOKEN and META_ACCOUNT):
        return {"placements": [], "source": "unavailable"}

    fields = "spend,impressions,clicks,ctr,cpm,actions,purchase_roas"
    url = (
        f"{GRAPH}/{META_ACCOUNT}/insights"
        f"?fields={fields}"
        f"&breakdowns=publisher_platform,platform_position"
        f"&date_preset=last_7d&level=account&limit=100"
        f"&access_token={META_TOKEN}"
    )

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url)
            if r.status_code != 200:
                return {"placements": [], "source": "error"}
            data = r.json().get("data", [])

        total_spend = sum(float(row.get("spend", 0)) for row in data)
        placements = []
        for row in data:
            spend = float(row.get("spend", 0))
            impressions = int(row.get("impressions", 0))
            clicks = int(row.get("clicks", 0))
            ctr = round(clicks / impressions * 100, 2) if impressions > 0 else 0
            cpm = float(row.get("cpm", 0))
            roas_list = row.get("purchase_roas", [])
            roas = float(roas_list[0].get("value", 0)) if roas_list else 0.0
            purchases = sum(int(a.get("value", 0)) for a in row.get("actions", []) if a.get("action_type") == "purchase")
            spend_pct = round(spend / total_spend * 100, 1) if total_spend > 0 else 0
            # red = burning money: high CPM low CTR
            signal = "red" if cpm > 80 and ctr < 1.0 else ("yellow" if cpm > 50 or ctr < 0.8 else "green")
            placements.append({
                "platform": row.get("publisher_platform", "—").title(),
                "position": row.get("platform_position", "—").replace("_", " ").title(),
                "spend_mxn": round(spend, 0),
                "spend_pct": spend_pct,
                "impressions": impressions,
                "ctr": ctr,
                "cpm_mxn": round(cpm, 0),
                "roas": round(roas, 2),
                "purchases": purchases,
                "signal": signal,
            })

        placements.sort(key=lambda x: x["spend_mxn"], reverse=True)
        result = {"placements": placements, "source": "live" if placements else "empty"}
        set_cached("meta_placement_7d", result)
        return result
    except Exception:
        return {"placements": [], "source": "error"}
