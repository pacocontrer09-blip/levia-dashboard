import os
import httpx
import calendar
from collections import defaultdict
from datetime import datetime, timedelta
from services.cache import get_cached, set_cached

STORE = os.getenv("SHOPIFY_STORE", "zwdhr1-e8.myshopify.com")
TOKEN = os.getenv("SHOPIFY_TOKEN", "")
BASE = f"https://{STORE}/admin/api/2024-10"
GQL = f"https://{STORE}/admin/api/2024-10/graphql.json"
HEADERS = {"X-Shopify-Access-Token": TOKEN}


async def get_orders_today() -> dict:
    cached = get_cached("shopify_today", ttl_seconds=30)  # 30s para tiempo real
    if cached:
        return cached

    if not TOKEN:
        return {"orders": [], "count": 0, "revenue_mxn": 0, "source": "no_token"}

    today = datetime.now().strftime("%Y-%m-%dT00:00:00-06:00")
    url = f"{BASE}/orders.json?status=any&created_at_min={today}&limit=250&fields=id,name,total_price,financial_status,created_at"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(url, headers=HEADERS)
            r.raise_for_status()
            orders = r.json().get("orders", [])
            paid = [o for o in orders if o.get("financial_status") == "paid"]
            revenue = sum(float(o.get("total_price", 0)) for o in paid)
            result = {"orders": orders[:10], "count": len(paid), "revenue_mxn": revenue, "source": "live"}
            set_cached("shopify_today", result)
            return result
    except Exception as e:
        return {"orders": [], "count": 0, "revenue_mxn": 0, "source": "error", "error": str(e)}


async def get_orders_month(month: str | None = None) -> dict:
    if not month:
        month = datetime.now().strftime("%Y-%m")

    cached = get_cached(f"shopify_month_{month}", ttl_seconds=600)
    if cached:
        return cached

    if not TOKEN:
        return {"orders": [], "count": 0, "revenue_mxn": 0, "units": 0, "source": "no_token"}

    year, mo = month.split("-")
    start = f"{year}-{mo}-01T00:00:00-06:00"
    last_day = calendar.monthrange(int(year), int(mo))[1]
    end = f"{year}-{mo}-{last_day:02d}T23:59:59-06:00"

    url = f"{BASE}/orders.json?status=any&created_at_min={start}&created_at_max={end}&limit=250&fields=id,name,total_price,financial_status,line_items,created_at"

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url, headers=HEADERS)
            r.raise_for_status()
            orders = r.json().get("orders", [])
            paid = [o for o in orders if o.get("financial_status") == "paid"]
            revenue = sum(float(o.get("total_price", 0)) for o in paid)
            units = sum(
                sum(int(li.get("quantity", 1)) for li in o.get("line_items", []))
                for o in paid
            )
            result = {"count": len(paid), "revenue_mxn": revenue, "units": units, "source": "live"}
            set_cached(f"shopify_month_{month}", result)
            return result
    except Exception as e:
        return {"count": 0, "revenue_mxn": 0, "units": 0, "source": "error", "error": str(e)}


def _parse_next_link(link_header: str) -> str | None:
    for part in link_header.split(","):
        part = part.strip()
        if 'rel="next"' in part:
            return part.split(";")[0].strip().strip("<>")
    return None


async def get_orders_historical(days: int = 90) -> dict:
    cache_key = f"shopify_historical_{days}"
    cached = get_cached(cache_key, ttl_seconds=1800)
    if cached:
        return cached

    if not TOKEN:
        return {"orders": [], "count": 0, "revenue_mxn": 0, "units": 0, "source": "no_token"}

    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00-06:00")
    first_url = f"{BASE}/orders.json?status=any&financial_status=paid&created_at_min={since}&limit=250&fields=id,name,total_price,financial_status,line_items,created_at,customer"

    try:
        all_orders = []
        next_url: str | None = first_url
        async with httpx.AsyncClient(timeout=20) as client:
            while next_url:
                r = await client.get(next_url, headers=HEADERS)
                r.raise_for_status()
                all_orders.extend(r.json().get("orders", []))
                next_url = _parse_next_link(r.headers.get("Link", ""))

        revenue = sum(float(o.get("total_price", 0)) for o in all_orders)
        units = sum(
            sum(int(li.get("quantity", 1)) for li in o.get("line_items", []))
            for o in all_orders
        )
        order_rows = [
            {
                "name": o.get("name", "—"),
                "date": o.get("created_at", "")[:10],
                "total": float(o.get("total_price", 0)),
                "status": o.get("financial_status", "—"),
            }
            for o in all_orders
        ]
        result = {
            "count": len(all_orders),
            "revenue_mxn": revenue,
            "units": units,
            "orders": order_rows,
            "source": "live",
            "period": f"Últimos {days} días",
        }
        set_cached(cache_key, result)
        return result
    except Exception as e:
        return {"count": 0, "revenue_mxn": 0, "units": 0, "orders": [], "source": "error", "error": str(e)}


async def get_orders_with_lineitems(days: int = 30) -> dict:
    """Returns daily revenue data + top products aggregated from line_items."""
    cache_key = f"shopify_lineitems_{days}"
    cached = get_cached(cache_key, ttl_seconds=1800)
    if cached:
        return cached

    if not TOKEN:
        return {"daily": [], "top_products": [], "count": 0, "revenue_mxn": 0, "source": "no_token"}

    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00-06:00")
    url = (
        f"{BASE}/orders.json?status=any&financial_status=paid&created_at_min={since}"
        f"&limit=250&fields=id,total_price,created_at,line_items"
    )

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, headers=HEADERS)
            r.raise_for_status()
            orders = r.json().get("orders", [])

        # Group by date for chart
        daily_map: dict = defaultdict(lambda: {"revenue": 0.0, "orders": 0})
        product_map: dict = defaultdict(lambda: {"units": 0, "revenue": 0.0})

        total_revenue = 0.0
        for o in orders:
            date = o.get("created_at", "")[:10]
            price = float(o.get("total_price", 0))
            daily_map[date]["revenue"] += price
            daily_map[date]["orders"] += 1
            total_revenue += price

            for li in o.get("line_items", []):
                name = li.get("title", "Producto")
                qty = int(li.get("quantity", 1))
                item_price = float(li.get("price", 0)) * qty
                product_map[name]["units"] += qty
                product_map[name]["revenue"] += item_price

        # Build daily array filling missing days with 0
        daily = []
        for i in range(days):
            date = (datetime.now() - timedelta(days=days - 1 - i)).strftime("%Y-%m-%d")
            d = daily_map.get(date, {"revenue": 0.0, "orders": 0})
            daily.append({"date": date, "revenue": round(d["revenue"], 2), "orders": d["orders"]})

        # Top products sorted by revenue
        top_products = sorted(
            [
                {
                    "name": name,
                    "units": data["units"],
                    "revenue": round(data["revenue"], 2),
                    "pct": round(data["revenue"] / total_revenue * 100, 1) if total_revenue > 0 else 0,
                }
                for name, data in product_map.items()
            ],
            key=lambda x: x["revenue"],
            reverse=True,
        )

        result = {
            "daily": daily,
            "top_products": top_products,
            "count": len(orders),
            "revenue_mxn": round(total_revenue, 2),
            "source": "live",
        }
        set_cached(cache_key, result)
        return result
    except Exception as e:
        return {"daily": [], "top_products": [], "count": 0, "revenue_mxn": 0, "source": "error", "error": str(e)}


async def get_customer_count() -> int:
    """Returns total number of customers in the store."""
    cached = get_cached("shopify_customer_count", ttl_seconds=3600)
    if cached:
        return cached

    if not TOKEN:
        return 0

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(f"{BASE}/customers/count.json", headers=HEADERS)
            r.raise_for_status()
            count = r.json().get("count", 0)
            set_cached("shopify_customer_count", count)
            return count
    except Exception:
        return 0


async def get_customers_detail(limit: int = 50) -> dict:
    """Returns top customers by total_spent with LTV metrics."""
    cached = get_cached("shopify_customers_detail", ttl_seconds=3600)
    if cached:
        return cached

    if not TOKEN:
        return {"customers": [], "total_count": 0, "avg_ltv": 0, "returning_pct": 0, "accepts_marketing_pct": 0, "source": "no_token"}

    url = (
        f"{BASE}/customers.json?limit={limit}&order=total_spent+desc"
        f"&fields=id,first_name,last_name,email,orders_count,total_spent,accepts_marketing,created_at,last_order_name,tags"
    )
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url, headers=HEADERS)
            r.raise_for_status()
            customers = r.json().get("customers", [])

        now = datetime.now()
        this_month = now.strftime("%Y-%m")
        new_this_month = sum(1 for c in customers if c.get("created_at", "")[:7] == this_month)
        returning = sum(1 for c in customers if int(c.get("orders_count", 0)) > 1)
        accepts_mkt = sum(1 for c in customers if c.get("accepts_marketing", False))
        total_ltv = sum(float(c.get("total_spent", 0)) for c in customers)
        avg_ltv = round(total_ltv / len(customers), 2) if customers else 0
        returning_pct = round(returning / len(customers) * 100, 1) if customers else 0
        accepts_pct = round(accepts_mkt / len(customers) * 100, 1) if customers else 0

        result = {
            "customers": customers,
            "total_count": len(customers),
            "new_this_month": new_this_month,
            "total_ltv": round(total_ltv, 2),
            "avg_ltv": avg_ltv,
            "returning_pct": returning_pct,
            "accepts_marketing_pct": accepts_pct,
            "source": "live",
        }
        set_cached("shopify_customers_detail", result)
        return result
    except Exception as e:
        return {"customers": [], "total_count": 0, "avg_ltv": 0, "returning_pct": 0, "accepts_marketing_pct": 0, "source": "error", "error": str(e)}


async def get_abandoned_checkouts(limit: int = 50) -> dict:
    """Returns open (abandoned) checkouts — revenue at risk."""
    cached = get_cached("shopify_abandoned", ttl_seconds=600)
    if cached:
        return cached

    if not TOKEN:
        return {"checkouts": [], "count": 0, "revenue_at_risk": 0, "source": "no_token"}

    url = f"{BASE}/checkouts.json?status=open&limit={limit}&fields=id,email,line_items,total_price,created_at,abandoned_checkout_url"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url, headers=HEADERS)
            r.raise_for_status()
            checkouts = r.json().get("checkouts", [])

        revenue_at_risk = sum(float(c.get("total_price", 0)) for c in checkouts)
        rows = [
            {
                "email": c.get("email") or "—",
                "items": sum(int(li.get("quantity", 1)) for li in c.get("line_items", [])),
                "total": float(c.get("total_price", 0)),
                "created_at": c.get("created_at", "")[:10],
                "url": c.get("abandoned_checkout_url", ""),
            }
            for c in checkouts
        ]
        result = {
            "checkouts": rows,
            "count": len(checkouts),
            "revenue_at_risk": round(revenue_at_risk, 2),
            "source": "live",
        }
        set_cached("shopify_abandoned", result)
        return result
    except Exception as e:
        return {"checkouts": [], "count": 0, "revenue_at_risk": 0, "source": "error", "error": str(e)}


async def get_draft_orders() -> dict:
    """Returns open draft orders (in-progress / manual sales)."""
    cached = get_cached("shopify_draft_orders", ttl_seconds=300)
    if cached:
        return cached

    if not TOKEN:
        return {"drafts": [], "count": 0, "total_value": 0, "source": "no_token"}

    url = f"{BASE}/draft_orders.json?status=open&limit=50&fields=id,name,email,total_price,status,created_at,line_items,customer"
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url, headers=HEADERS)
            r.raise_for_status()
            drafts = r.json().get("draft_orders", [])

        total_value = sum(float(d.get("total_price", 0)) for d in drafts)
        rows = [
            {
                "name": d.get("name", "—"),
                "email": d.get("email") or (d.get("customer") or {}).get("email", "—"),
                "items": sum(int(li.get("quantity", 1)) for li in d.get("line_items", [])),
                "total": float(d.get("total_price", 0)),
                "status": d.get("status", "—"),
                "created_at": d.get("created_at", "")[:10],
            }
            for d in drafts
        ]
        result = {"drafts": rows, "count": len(drafts), "total_value": round(total_value, 2), "source": "live"}
        set_cached("shopify_draft_orders", result)
        return result
    except Exception as e:
        return {"drafts": [], "count": 0, "total_value": 0, "source": "error", "error": str(e)}


async def get_discount_codes_stats(days: int = 30) -> dict:
    """Usage and revenue per discount code in the last N days."""
    cache_key = f"shopify_discounts_{days}"
    cached = get_cached(cache_key, ttl_seconds=1800)
    if cached:
        return cached

    if not TOKEN:
        return {"codes": [], "total_discounted_revenue": 0, "source": "no_token"}

    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00-06:00")
    url = (
        f"{BASE}/orders.json?status=any&financial_status=paid&created_at_min={since}"
        f"&limit=250&fields=id,total_price,discount_codes,created_at"
    )
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, headers=HEADERS)
            r.raise_for_status()
            orders = r.json().get("orders", [])

        code_map: dict = defaultdict(lambda: {"uses": 0, "revenue": 0.0})
        total_discounted = 0.0
        for o in orders:
            codes = o.get("discount_codes", [])
            price = float(o.get("total_price", 0))
            if codes:
                total_discounted += price
                for dc in codes:
                    code = dc.get("code", "—").upper()
                    code_map[code]["uses"] += 1
                    code_map[code]["revenue"] += price

        sorted_codes = sorted(
            [{"code": k, "uses": v["uses"], "revenue": round(v["revenue"], 2)} for k, v in code_map.items()],
            key=lambda x: x["revenue"],
            reverse=True,
        )
        result = {"codes": sorted_codes, "total_discounted_revenue": round(total_discounted, 2), "source": "live"}
        set_cached(cache_key, result)
        return result
    except Exception as e:
        return {"codes": [], "total_discounted_revenue": 0, "source": "error", "error": str(e)}


async def get_orders_with_geo(days: int = 90) -> dict:
    """Orders with shipping_address for geographic breakdown by state."""
    cache_key = f"shopify_geo_{days}"
    cached = get_cached(cache_key, ttl_seconds=1800)
    if cached:
        return cached

    if not TOKEN:
        return {"orders": [], "source": "no_token"}

    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00-06:00")
    url = (
        f"{BASE}/orders.json?status=any&financial_status=paid&created_at_min={since}"
        f"&limit=250&fields=id,total_price,created_at,shipping_address"
    )
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, headers=HEADERS)
            r.raise_for_status()
            orders = r.json().get("orders", [])

        result = {"orders": orders, "count": len(orders), "source": "live"}
        set_cached(cache_key, result)
        return result
    except Exception as e:
        return {"orders": [], "count": 0, "source": "error", "error": str(e)}


async def get_orders_with_refunds(days: int = 90) -> dict:
    """Orders with refund data + fulfillment status for revenue net calculation."""
    cache_key = f"shopify_refunds_{days}"
    cached = get_cached(cache_key, ttl_seconds=1800)
    if cached:
        return cached

    if not TOKEN:
        return {"count": 0, "revenue_gross": 0, "revenue_net": 0, "total_refunded": 0, "refund_count": 0, "refund_rate_pct": 0, "unfulfilled_count": 0, "unfulfilled_revenue": 0, "source": "no_token"}

    since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00-06:00")
    url = (
        f"{BASE}/orders.json?status=any&financial_status=paid&created_at_min={since}"
        f"&limit=250&fields=id,total_price,financial_status,fulfillment_status,created_at,refunds"
    )
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url, headers=HEADERS)
            r.raise_for_status()
            orders = r.json().get("orders", [])

        revenue_gross = sum(float(o.get("total_price", 0)) for o in orders)
        total_refunded = 0.0
        refund_count = 0
        for o in orders:
            for ref in o.get("refunds", []):
                for tx in ref.get("transactions", []):
                    if tx.get("kind") in ("refund", "void"):
                        total_refunded += float(tx.get("amount", 0))
                        refund_count += 1

        revenue_net = revenue_gross - total_refunded
        refund_rate = round(refund_count / len(orders) * 100, 1) if orders else 0

        unfulfilled = [o for o in orders if o.get("fulfillment_status") in (None, "unfulfilled", "partial")]
        unfulfilled_revenue = sum(float(o.get("total_price", 0)) for o in unfulfilled)

        result = {
            "count": len(orders),
            "revenue_gross": round(revenue_gross, 2),
            "revenue_net": round(revenue_net, 2),
            "total_refunded": round(total_refunded, 2),
            "refund_count": refund_count,
            "refund_rate_pct": refund_rate,
            "unfulfilled_count": len(unfulfilled),
            "unfulfilled_revenue": round(unfulfilled_revenue, 2),
            "source": "live",
        }
        set_cached(cache_key, result)
        return result
    except Exception as e:
        return {"count": 0, "revenue_gross": 0, "revenue_net": 0, "total_refunded": 0, "refund_count": 0, "refund_rate_pct": 0, "unfulfilled_count": 0, "unfulfilled_revenue": 0, "source": "error", "error": str(e)}


# ---------------------------------------------------------------------------
# GraphQL helper
# ---------------------------------------------------------------------------

async def _gql(query: str, variables: dict) -> dict:
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            GQL,
            json={"query": query, "variables": variables},
            headers={**HEADERS, "Content-Type": "application/json"},
        )
        r.raise_for_status()
        return r.json()


# ---------------------------------------------------------------------------
# Funda auto-bundle
# ---------------------------------------------------------------------------

async def create_funda_product() -> dict:
    """Creates LEVIA™ Funda as a hidden Shopify product. Run once, save the returned variant_id to .env."""
    if not TOKEN:
        return {"error": "no_token"}

    payload = {
        "product": {
            "title": "LEVIA™ Funda Cervical Suave y Fría",
            "vendor": "LEVIA™",
            "product_type": "Accesorio",
            "body_html": "Funda de repuesto suave y fría para almohada cervical LEVIA Align.",
            "status": "active",
            "published": False,
            "variants": [
                {
                    "title": "Default Title",
                    "price": "0.00",
                    "sku": "LEVIA-FUNDA-001",
                    "inventory_management": None,
                    "fulfillment_service": "manual",
                    "requires_shipping": True,
                }
            ],
        }
    }

    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            f"{BASE}/products.json",
            json=payload,
            headers={**HEADERS, "Content-Type": "application/json"},
        )
        r.raise_for_status()
        product = r.json().get("product", {})
        variants = product.get("variants", [])
        variant_id = variants[0]["id"] if variants else None
        return {
            "product_id": product.get("id"),
            "variant_id": variant_id,
            "title": product.get("title"),
            "status": "created",
            "next_step": f"Agrega LEVIA_FUNDA_VARIANT_ID={variant_id} a .env y reinicia el dashboard",
        }


async def add_funda_to_order(order_id: int, quantity: int) -> dict:
    """Adds funda line item (price $0) to a paid order via GraphQL orderEdit. Safe to call async."""
    import logging
    log = logging.getLogger("levia.funda")

    funda_variant_id = os.getenv("LEVIA_FUNDA_VARIANT_ID", "")
    if not funda_variant_id:
        log.warning("LEVIA_FUNDA_VARIANT_ID not set — funda auto-add skipped")
        return {"error": "no_variant_id"}
    if not TOKEN:
        return {"error": "no_token"}

    order_gid = f"gid://shopify/Order/{order_id}"
    variant_gid = f"gid://shopify/ProductVariant/{funda_variant_id}"

    try:
        # 1. Begin edit
        res = await _gql(
            """
            mutation beginEdit($id: ID!) {
              orderEditBegin(id: $id) {
                calculatedOrder { id }
                userErrors { field message }
              }
            }
            """,
            {"id": order_gid},
        )
        errors = (res.get("data") or {}).get("orderEditBegin", {}).get("userErrors", [])
        if errors:
            log.error("orderEditBegin errors order %s: %s", order_id, errors)
            return {"error": "begin_failed", "details": errors}

        calc_id = res["data"]["orderEditBegin"]["calculatedOrder"]["id"]

        # 2. Add variant
        res = await _gql(
            """
            mutation addVariant($id: ID!, $variantId: ID!, $qty: Int!) {
              orderEditAddVariant(id: $id, variantId: $variantId, quantity: $qty, allowDuplicates: false) {
                calculatedLineItem { id }
                userErrors { field message }
              }
            }
            """,
            {"id": calc_id, "variantId": variant_gid, "qty": quantity},
        )
        errors = (res.get("data") or {}).get("orderEditAddVariant", {}).get("userErrors", [])
        if errors:
            log.error("orderEditAddVariant errors order %s: %s", order_id, errors)
            return {"error": "add_failed", "details": errors}

        line_item_id = res["data"]["orderEditAddVariant"]["calculatedLineItem"]["id"]

        # 3. Set price to $0 — funda included free with pillow
        await _gql(
            """
            mutation setPrice($id: ID!, $lineItemId: ID!, $price: MoneyInput!) {
              orderEditSetLineItemPrice(id: $id, lineItemId: $lineItemId, price: $price) {
                calculatedOrder { id }
                userErrors { field message }
              }
            }
            """,
            {"id": calc_id, "lineItemId": line_item_id, "price": {"amount": "0.00", "currencyCode": "MXN"}},
        )

        # 4. Commit
        res = await _gql(
            """
            mutation commitEdit($id: ID!) {
              orderEditCommit(id: $id, notifyCustomer: false, staffNote: "Funda incluida automáticamente con tu almohada LEVIA™") {
                order { id name }
                userErrors { field message }
              }
            }
            """,
            {"id": calc_id},
        )
        errors = (res.get("data") or {}).get("orderEditCommit", {}).get("userErrors", [])
        if errors:
            log.error("orderEditCommit errors order %s: %s", order_id, errors)
            return {"error": "commit_failed", "details": errors}

        order_name = res["data"]["orderEditCommit"]["order"].get("name", "")
        log.info("Funda x%s added to order %s (%s)", quantity, order_name, order_id)
        return {"ok": True, "order_id": order_id, "quantity": quantity}

    except Exception as e:
        log.error("add_funda_to_order failed for order %s: %s", order_id, e)
        return {"error": str(e)}
