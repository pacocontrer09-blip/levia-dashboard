import os
import httpx
from services.cache import get_cached, set_cached

API_KEY = os.getenv("GOOGLE_SHEETS_API_KEY", "")
SHEET_ID = os.getenv("GOOGLE_SHEETS_ID", "17aMLsgMhSjmH39UHyTwz1cahZrsmqHGLxGkCgtaAhX0")
BASE = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values"


async def _read_range(range_name: str) -> list:
    if not API_KEY:
        return []
    url = f"{BASE}/{range_name}?key={API_KEY}"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url)
            r.raise_for_status()
            return r.json().get("values", [])
    except Exception:
        return []


async def get_dashboard_kpis(month: str | None = None) -> dict:
    """Reads the Dashboard tab. Falls back to agent_state.json (real data) if Sheets unavailable."""
    cached = get_cached(f"sheets_kpis_{month}", ttl_seconds=900)
    if cached:
        return cached

    if API_KEY:
        values = await _read_range("Dashboard!A1:D30")
        if values:
            kpis = {}
            for row in values:
                if len(row) >= 2:
                    kpis[row[0]] = row[1] if len(row) > 1 else ""

            result = {
                "revenue_mxn": _num(kpis.get("Ingresos MXN", kpis.get("Revenue MXN", 0))),
                "orders": _num(kpis.get("Órdenes", kpis.get("Orders", 0))),
                "units": _num(kpis.get("Unidades", kpis.get("Units", 0))),
                "aov_mxn": _num(kpis.get("AOV MXN", kpis.get("AOV", 0))),
                "cogs_mxn": _num(kpis.get("COGS MXN", kpis.get("COGS", 0))),
                "gross_margin_pct": _num(kpis.get("Gross Margin %", kpis.get("Gross Margin", 0))),
                "net_margin_pct": _num(kpis.get("Net Margin %", kpis.get("Net Margin", 0))),
                "ad_spend_mxn": _num(kpis.get("Ad Spend MXN", kpis.get("Ad Spend", 0))),
                "roas": _num(kpis.get("ROAS", 0)),
                "cac_mxn": _num(kpis.get("CAC MXN", kpis.get("CAC", 0))),
                "net_profit_mxn": _num(kpis.get("Net Profit MXN", kpis.get("Net Profit", 0))),
                "source": "sheets",
            }
            set_cached(f"sheets_kpis_{month}", result)
            return result

    return _demo_kpis()


def _num(val) -> float:
    try:
        return float(str(val).replace(",", "").replace("$", "").replace("%", "").strip())
    except Exception:
        return 0.0


def _demo_kpis() -> dict:
    return {
        "revenue_mxn": 0, "orders": 0, "units": 0, "aov_mxn": 1899,
        "cogs_mxn": 0, "gross_margin_pct": 0, "net_margin_pct": 0,
        "ad_spend_mxn": 0, "roas": 0, "cac_mxn": 0, "net_profit_mxn": 0,
        "source": "demo",
    }
