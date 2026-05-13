"""
Cash position across all payment sources.
- Nu México: cached from CSV upload (no API)
- Shopify Payments: live via GraphQL
- MercadoPago: live via REST API (needs MP_ACCESS_TOKEN in .env)
"""
import json
import os
from datetime import datetime
from pathlib import Path
import httpx

SHOPIFY_STORE = os.getenv("SHOPIFY_STORE", "")
SHOPIFY_TOKEN = os.getenv("SHOPIFY_TOKEN", "")
MP_ACCESS_TOKEN = os.getenv("MP_ACCESS_TOKEN", "")

NU_CACHE = Path(__file__).parent.parent / "cache" / "nu_transactions.json"


def get_nu_balance() -> dict:
    """Latest balance from cached Nu CSV."""
    if not NU_CACHE.exists():
        return {"balance": None, "updated_at": None, "source": "no_data"}
    try:
        txs = json.loads(NU_CACHE.read_text())
        if not txs:
            return {"balance": None, "updated_at": None, "source": "no_data"}
        # transactions are sorted newest-first; first entry has latest balance
        latest = txs[0]
        return {
            "balance": latest.get("balance", 0),
            "updated_at": latest.get("date", ""),
            "source": "csv_cache",
        }
    except Exception as e:
        return {"balance": None, "updated_at": None, "source": "error", "error": str(e)}


async def get_shopify_payments_balance() -> dict:
    """Live balance from Shopify Payments GraphQL."""
    if not SHOPIFY_TOKEN:
        return {"balance": None, "currency": None, "source": "no_token"}
    query = """
    {
      shopifyPaymentsAccount {
        balance { currency amount }
      }
    }
    """
    url = f"https://{SHOPIFY_STORE}/admin/api/2024-01/graphql.json"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(url, json={"query": query},
                                  headers={"X-Shopify-Access-Token": SHOPIFY_TOKEN,
                                           "Content-Type": "application/json"})
        data = r.json()
        acct = (data.get("data") or {}).get("shopifyPaymentsAccount") or {}
        bal = acct.get("balance") or {}
        if bal.get("amount") is not None:
            return {"balance": float(bal["amount"]), "currency": bal.get("currency", "MXN"), "source": "live"}
        return {"balance": None, "currency": None, "source": "not_available"}
    except Exception as e:
        return {"balance": None, "currency": None, "source": "error", "error": str(e)}


async def get_mp_balance() -> dict:
    """Live balance from MercadoPago API."""
    if not MP_ACCESS_TOKEN:
        return {"balance": None, "source": "no_token"}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                "https://api.mercadopago.com/v1/account/balance",
                headers={"Authorization": f"Bearer {MP_ACCESS_TOKEN}"},
            )
        data = r.json()
        if r.status_code == 200:
            available = data.get("available_balance", data.get("total_amount", 0))
            return {"balance": float(available), "currency": "MXN", "source": "live"}
        return {"balance": None, "source": "error", "error": data.get("message", str(r.status_code))}
    except Exception as e:
        return {"balance": None, "source": "error", "error": str(e)}


async def get_treasury() -> dict:
    """Aggregate cash position from all sources."""
    import asyncio
    shopify_bal, mp_bal = await asyncio.gather(
        get_shopify_payments_balance(),
        get_mp_balance(),
    )
    nu_bal = get_nu_balance()

    sources = [
        {"name": "Nu México",          "icon": "🟣", "data": nu_bal,      "manual": True},
        {"name": "Shopify Payments",   "icon": "🟢", "data": shopify_bal, "manual": False},
        {"name": "MercadoPago",        "icon": "🔵", "data": mp_bal,      "manual": False},
    ]

    total = sum(
        s["data"]["balance"] for s in sources
        if s["data"].get("balance") is not None
    )

    any_missing = any(
        s["data"].get("source") in ("no_token", "no_data")
        for s in sources
    )

    return {
        "sources": sources,
        "total": total,
        "any_missing": any_missing,
        "fetched_at": datetime.now().strftime("%H:%M:%S"),
    }
