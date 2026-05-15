import asyncio
import base64
import hashlib
import hmac
import os
import time

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from services.cache import set_cached
from services import sse_service, shopify_service
from services.automation_service import (
    trigger_welcome_flow,
    trigger_abandoned_cart_flow,
    trigger_post_purchase_flow,
    cancel_abandoned_for_email,
    _cancel_pending_by_prefix,
)

PIXEL_ID = os.getenv("META_PIXEL_ID", "")
META_TOKEN = os.getenv("META_ACCESS_TOKEN", "")


async def _send_capi_purchase(order: dict):
    """Send Purchase event to Meta Conversions API (server-side, best-effort)."""
    if not PIXEL_ID or not META_TOKEN:
        return
    email = (order.get("customer") or {}).get("email") or order.get("email", "")
    email_hash = hashlib.sha256(email.lower().strip().encode()).hexdigest() if email else None
    value = float(order.get("total_price", 0))
    currency = order.get("currency", "MXN")
    order_id = str(order.get("id", ""))

    note_attrs = {a["name"]: a["value"] for a in order.get("note_attributes", [])}
    fbp = note_attrs.get("_fbp", "")
    fbc = note_attrs.get("_fbc", "")

    user_data: dict = {}
    if email_hash:
        user_data["em"] = [email_hash]
    if fbp:
        user_data["fbp"] = fbp
    if fbc:
        user_data["fbc"] = fbc

    payload = {
        "data": [{
            "event_name": "Purchase",
            "event_time": int(time.time()),
            "event_source_url": "https://levia.care/",
            "action_source": "website",
            "user_data": user_data,
            "custom_data": {
                "value": value,
                "currency": currency,
                "order_id": order_id,
                "content_type": "product",
            },
        }],
        "access_token": META_TOKEN,
    }
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://graph.facebook.com/v21.0/{PIXEL_ID}/events",
                json=payload,
                timeout=10,
            )
    except Exception:
        pass

router = APIRouter()

SHOPIFY_WEBHOOK_SECRET = os.getenv("SHOPIFY_WEBHOOK_SECRET", "")
LEVIA_ALIGN_PRODUCT_ID = int(os.getenv("LEVIA_ALIGN_PRODUCT_ID", "9015686267018"))


async def _valid_shopify_hmac(request: Request) -> bool:
    if not SHOPIFY_WEBHOOK_SECRET:
        return True  # Skip validation until secret is configured
    header = request.headers.get("X-Shopify-Hmac-Sha256", "")
    body = await request.body()
    digest = base64.b64encode(
        hmac.new(SHOPIFY_WEBHOOK_SECRET.encode(), body, hashlib.sha256).digest()
    ).decode()
    return hmac.compare_digest(digest, header)


@router.post("/shopify/orders/create")
async def shopify_order_create(request: Request):
    if not await _valid_shopify_hmac(request):
        return JSONResponse({"error": "invalid hmac"}, status_code=401)
    set_cached("shopify_today", None)
    set_cached("shopify_historical_90", None)
    for days in (7, 30, 90):
        set_cached(f"shopify_lineitems_{days}", None)
    try:
        data = await request.json()
        asyncio.create_task(sse_service.publish("order", {
            "name": data.get("name", ""),
            "total": str(data.get("total_price", "0")),
            "currency": data.get("currency", "MXN"),
        }))
    except Exception:
        pass
    return JSONResponse({"ok": True})


@router.post("/shopify/orders/paid")
async def shopify_order_paid(request: Request):
    if not await _valid_shopify_hmac(request):
        return JSONResponse({"error": "invalid hmac"}, status_code=401)

    order = await request.json()

    # Cancel any pending abandoned-cart emails for this customer
    customer = order.get("customer") or {}
    email = customer.get("email") or order.get("email", "")
    if email:
        cancel_abandoned_for_email(email)
        # Cancelar también welcome flow — primer comprador recibe ambos si no se cancela
        _cancel_pending_by_prefix(f"welcome_{email}_")

    trigger_post_purchase_flow(order)

    # Meta Conversions API — Purchase event (server-side, best-effort)
    asyncio.create_task(_send_capi_purchase(order))

    # Auto-bundle funda: count almohada units and add funda line item
    funda_qty = sum(
        li.get("quantity", 0)
        for li in order.get("line_items", [])
        if li.get("product_id") == LEVIA_ALIGN_PRODUCT_ID
    )
    if funda_qty > 0:
        asyncio.create_task(
            shopify_service.add_funda_to_order(order["id"], funda_qty)
        )

    # Invalidate order caches
    set_cached("shopify_today", None)
    set_cached("shopify_historical_90", None)
    for days in (7, 30, 90):
        set_cached(f"shopify_lineitems_{days}", None)

    asyncio.create_task(sse_service.publish("order", {
        "name": order.get("name", ""),
        "total": str(order.get("total_price", "0")),
        "currency": order.get("currency", "MXN"),
    }))

    return JSONResponse({"ok": True})


@router.post("/shopify/customers/create")
async def shopify_customer_create(request: Request):
    if not await _valid_shopify_hmac(request):
        return JSONResponse({"error": "invalid hmac"}, status_code=401)

    customer = await request.json()
    trigger_welcome_flow(customer)
    return JSONResponse({"ok": True})


@router.post("/shopify/checkouts/create")
async def shopify_checkout_create(request: Request):
    if not await _valid_shopify_hmac(request):
        return JSONResponse({"error": "invalid hmac"}, status_code=401)

    checkout = await request.json()
    # Only trigger if the checkout has an email (guest checkouts without email are skipped)
    if checkout.get("email"):
        trigger_abandoned_cart_flow(checkout)
    return JSONResponse({"ok": True})
