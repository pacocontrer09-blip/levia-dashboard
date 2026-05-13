from fastapi import APIRouter
from fastapi.responses import HTMLResponse, JSONResponse

from services.email_service import validate_unsubscribe_token, mark_unsubscribed, is_unsubscribed, get_email_log, _load_unsubscribed
from services.automation_service import (
    trigger_welcome_flow,
    trigger_abandoned_cart_flow,
    trigger_post_purchase_flow,
    _load_pending,
)

router = APIRouter()

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
