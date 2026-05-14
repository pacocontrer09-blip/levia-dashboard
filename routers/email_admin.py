import os
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from services.email_service import validate_unsubscribe_token, mark_unsubscribed, is_unsubscribed, get_email_log, _load_unsubscribed
from services.automation_service import (
    trigger_welcome_flow,
    trigger_abandoned_cart_flow,
    trigger_post_purchase_flow,
    _load_pending,
)

templates = Jinja2Templates(directory="templates")

router = APIRouter()


def _build_leads(log: list, pending: list, unsubs: set) -> list:
    """Agrega emails únicos con fecha de suscripción, step actual y estado."""
    from collections import defaultdict

    # Pasos enviados por email
    sent_steps: dict[str, list] = defaultdict(list)
    subscribed_at: dict[str, str] = {}
    for e in log:
        if e.get("status") != "sent":
            continue
        email = e.get("to", "")
        if not email:
            continue
        sent_steps[email].append(e.get("template", ""))
        # fecha más antigua = cuándo se suscribió
        ts = e.get("ts", "")
        if email not in subscribed_at or ts < subscribed_at[email]:
            subscribed_at[email] = ts

    # Pasos pendientes por email
    pending_steps: dict[str, list] = defaultdict(list)
    pending_next: dict[str, str] = {}
    for j in pending:
        email = j.get("email", "")
        if email:
            pending_steps[email].append(j)
            # próximo envío = el más cercano
            run_at = j.get("run_at", "")
            if email not in pending_next or run_at < pending_next[email]:
                pending_next[email] = run_at

    all_emails = set(sent_steps.keys()) | set(pending_steps.keys())

    leads = []
    for email in all_emails:
        steps_sent  = len(sent_steps[email])
        steps_queue = len(pending_steps[email])
        total_flow  = steps_sent + steps_queue

        if "welcome_03" in str(sent_steps[email]):
            flow_label = "Completo"
            flow_color = "#065f46"
        elif steps_sent > 0:
            flow_label = f"Paso {steps_sent}/{total_flow}"
            flow_color = "#6B8FB5"
        else:
            flow_label = "En cola"
            flow_color = "#C8A15A"

        leads.append({
            "email":         email,
            "subscribed_at": subscribed_at.get(email, "")[:16].replace("T", " "),
            "flow_label":    flow_label,
            "flow_color":    flow_color,
            "next_send":     pending_next.get(email, "")[:16].replace("T", " "),
            "unsubscribed":  email in unsubs,
        })

    leads.sort(key=lambda x: x["subscribed_at"], reverse=True)
    return leads


@router.get("/", response_class=HTMLResponse)
async def email_dashboard(request: Request):
    pending = _load_pending()
    unsubs  = _load_unsubscribed()
    log     = get_email_log(200)
    sent_total  = sum(1 for e in log if e.get("status") == "sent")
    error_total = sum(1 for e in log if e.get("status") == "error")
    leads = _build_leads(log, pending, unsubs)
    return templates.TemplateResponse("email.html", {
        "request": request,
        "page": "email",
        "fetched_at": "en vivo",
        "sent_total": sent_total,
        "error_total": error_total,
        "pending_total": len(pending),
        "unsubscribed_total": len(unsubs),
        "leads_total": len(leads),
        "leads": leads,
        "pending_jobs": sorted(pending, key=lambda j: j.get("run_at", "")),
        "recent_sends": log,
        "resend_missing": not os.getenv("RESEND_API_KEY"),
    })

_UNSUB_PAGE = """<!DOCTYPE html>
<html lang="es">
<head><meta charset="UTF-8"><title>Baja — LEVIA™</title></head>
<body style="font-family:'Inter',sans-serif;text-align:center;padding:80px 24px;background:#FAF8F3;">
  <p style="font-family:Georgia,serif;font-size:20px;letter-spacing:0.18em;color:#0A1F3D;">LEVIA™</p>
  <h1 style="font-size:22px;font-weight:400;color:#0A1F3D;">Dada de baja correctamente</h1>
  <p style="color:#4A5B76;font-size:15px;">Ya no recibirás emails de LEVIA™.<br>
  Si fue un error, responde cualquier email anterior y te reactivamos.</p>
</body></html>"""

_UNSUB_ERROR = """<!DOCTYPE html>
<html lang="es">
<head><meta charset="UTF-8"><title>Link inválido — LEVIA™</title></head>
<body style="font-family:'Inter',sans-serif;text-align:center;padding:80px 24px;background:#FAF8F3;">
  <p style="font-family:Georgia,serif;font-size:20px;letter-spacing:0.18em;color:#0A1F3D;">LEVIA™</p>
  <h1 style="font-size:22px;font-weight:400;color:#0A1F3D;">Link inválido</h1>
  <p style="color:#4A5B76;font-size:15px;">Este link ya expiró o no es válido.<br>
  Responde directamente al email para solicitar la baja.</p>
</body></html>"""


@router.get("/unsubscribe")
async def unsubscribe(token: str):
    email = validate_unsubscribe_token(token)
    if not email:
        return HTMLResponse(_UNSUB_ERROR, status_code=400)
    mark_unsubscribed(email)
    return HTMLResponse(_UNSUB_PAGE)


@router.get("/status")
async def email_status():
    try:
        pending = _load_pending()
        unsubs = _load_unsubscribed()
        log = get_email_log(200)
        sent_total = sum(1 for e in log if e.get("status") == "sent")
        error_total = sum(1 for e in log if e.get("status") == "error")
        return JSONResponse({
            "pending_jobs": len(pending),
            "unsubscribed_total": len(unsubs),
            "sent_total": sent_total,
            "error_total": error_total,
            "pending_detail": pending,
            "recent_sends": log[:50],
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/test")
async def test_flow(flow: str, email: str):
    if flow == "welcome":
        trigger_welcome_flow({"email": email, "first_name": "Test"})
    elif flow == "abandoned":
        trigger_abandoned_cart_flow({
            "email": email,
            "abandoned_checkout_url": "https://levia.care/cart",
            "line_items": [{"title": "LEVIA Align"}],
        })
    elif flow == "postpurchase":
        trigger_post_purchase_flow({
            "email": email,
            "customer": {"email": email, "first_name": "Test"},
            "name": "#TEST-001",
            "order_status_url": "https://levia.care/account",
        })
    else:
        return JSONResponse(
            {"error": "flow debe ser: welcome | abandoned | postpurchase"},
            status_code=400,
        )
    return JSONResponse({"ok": True, "flow": flow, "email": email})


@router.post("/subscribe")
async def subscribe_email(request: Request):
    """
    Endpoint público para captura de leads desde el storefront (popup + ecosystem waitlist).
    No requiere CSRF — valida origen por CORS y formato de email.
    Registra en Shopify vía Admin API y dispara el welcome flow.
    """
    import re, httpx

    try:
        body = await request.json()
    except Exception:
        form = await request.form()
        body = dict(form)

    email = str(body.get("email", "")).strip().lower()
    tags  = str(body.get("tags", "newsletter")).strip()

    if not email or not re.match(r"^[^@]+@[^@]+\.[^@]+$", email):
        return JSONResponse({"ok": False, "error": "email inválido"}, status_code=400)

    if is_unsubscribed(email):
        return JSONResponse({"ok": True, "note": "already_unsubscribed"})

    # Crear/actualizar customer en Shopify Admin API
    shopify_domain = os.getenv("SHOPIFY_STORE_DOMAIN", "zwdhr1-e8.myshopify.com")
    shopify_token  = os.getenv("SHOPIFY_ACCESS_TOKEN", "")

    shopify_ok = False
    if shopify_token:
        try:
            async with httpx.AsyncClient(timeout=8) as client:
                r = await client.post(
                    f"https://{shopify_domain}/admin/api/2024-01/customers.json",
                    headers={"X-Shopify-Access-Token": shopify_token, "Content-Type": "application/json"},
                    json={"customer": {
                        "email": email,
                        "tags": tags,
                        "email_marketing_consent": {
                            "state": "subscribed",
                            "opt_in_level": "single_opt_in",
                        },
                        "accepts_marketing": True,
                    }},
                )
                shopify_ok = r.status_code in (200, 201, 422)  # 422 = ya existe
        except Exception:
            shopify_ok = False

    # Disparar welcome flow siempre
    trigger_welcome_flow({"email": email, "first_name": ""})

    return JSONResponse({"ok": True, "shopify": shopify_ok}, headers={
        "Access-Control-Allow-Origin": "https://levia.care",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    })


@router.options("/subscribe")
async def subscribe_preflight():
    return JSONResponse({}, headers={
        "Access-Control-Allow-Origin": "https://levia.care",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
    })
