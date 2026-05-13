import os
from datetime import datetime, timedelta
import httpx
from services.cache import get_cached, set_cached

KLAVIYO_KEY = os.getenv("KLAVIYO_KEY", "")
KLAVIYO_BASE = "https://a.klaviyo.com/api"
KLAVIYO_REV = "2024-10-15"


def _headers() -> dict:
    return {
        "Authorization": f"Klaviyo-API-Key {KLAVIYO_KEY}",
        "revision": KLAVIYO_REV,
        "Accept": "application/json",
    }


async def get_klaviyo_overview() -> dict:
    cached = get_cached("klaviyo_overview", ttl_seconds=1800)
    if cached:
        return cached

    if not KLAVIYO_KEY:
        return _empty("no_key")

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            # Fetch lists (subscriber count)
            lists_r = await client.get(
                f"{KLAVIYO_BASE}/lists/",
                headers=_headers(),
                params={"fields[list]": "name,created,updated,profile_count"},
            )

            # Fetch metrics to find Placed Order metric ID
            metrics_r = await client.get(
                f"{KLAVIYO_BASE}/metrics/",
                headers=_headers(),
            )

            metrics_data = metrics_r.json().get("data", []) if metrics_r.status_code == 200 else []
            placed_order_id = next(
                (m["id"] for m in metrics_data if "placed order" in m.get("attributes", {}).get("name", "").lower()),
                None,
            )

            # Fetch flows
            flows_r = await client.get(
                f"{KLAVIYO_BASE}/flows/",
                headers=_headers(),
                params={"fields[flow]": "name,status,created,updated", "page[size]": "50"},
            )

            # Fetch metric aggregates for Placed Order (last 30d)
            revenue_30d = 0.0
            orders_30d = 0
            if placed_order_id:
                now = datetime.utcnow()
                start = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
                end = now.strftime("%Y-%m-%dT%H:%M:%SZ")
                agg_r = await client.post(
                    f"{KLAVIYO_BASE}/metric-aggregates/",
                    headers={**_headers(), "Content-Type": "application/json"},
                    json={
                        "data": {
                            "type": "metric-aggregate",
                            "attributes": {
                                "metric_id": placed_order_id,
                                "measurements": ["sum_value", "count"],
                                "interval": "month",
                                "filter": [
                                    f"greater-or-equal(datetime,{start})",
                                    f"less-than(datetime,{end})",
                                ],
                                "timezone": "America/Mexico_City",
                            },
                        }
                    },
                )
                if agg_r.status_code == 200:
                    agg_data = agg_r.json().get("data", {}).get("attributes", {})
                    dates = agg_data.get("dates", [])
                    values = agg_data.get("values", [[]])
                    if values:
                        revenue_30d = sum(float(v) for row in values for v in row)
                    counts = agg_data.get("data", [[]])
                    if counts:
                        orders_30d = sum(int(v) for row in counts for v in row)

        # Parse lists
        lists_data = lists_r.json().get("data", []) if lists_r.status_code == 200 else []
        total_subscribers = sum(
            int(l.get("attributes", {}).get("profile_count", 0) or 0) for l in lists_data
        )
        list_rows = [
            {
                "name": l.get("attributes", {}).get("name", "—"),
                "count": int(l.get("attributes", {}).get("profile_count", 0) or 0),
            }
            for l in lists_data
        ]

        # Parse flows
        flows_data = flows_r.json().get("data", []) if flows_r.status_code == 200 else []
        key_flows = ["welcome", "abandoned", "post-purchase", "post purchase", "browse", "winback"]
        flow_rows = []
        for f in flows_data:
            attrs = f.get("attributes", {})
            name = attrs.get("name", "—")
            status = attrs.get("status", "—")
            is_key = any(k in name.lower() for k in key_flows)
            flow_rows.append({"name": name, "status": status, "is_key": is_key})
        flow_rows.sort(key=lambda x: (not x["is_key"], x["name"]))

        result = {
            "revenue_30d": round(revenue_30d, 2),
            "orders_30d": orders_30d,
            "total_subscribers": total_subscribers,
            "lists": list_rows,
            "flows": flow_rows[:20],
            "metrics_count": len(metrics_data),
            "source": "live",
            "fetched_at": datetime.now().strftime("%H:%M"),
        }
        set_cached("klaviyo_overview", result)
        return result

    except Exception as e:
        return _empty(f"error: {e}")


def _empty(source: str) -> dict:
    return {
        "revenue_30d": 0,
        "orders_30d": 0,
        "total_subscribers": 0,
        "lists": [],
        "flows": [],
        "metrics_count": 0,
        "source": source,
        "fetched_at": datetime.now().strftime("%H:%M"),
    }
