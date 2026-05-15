import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from services.email_service import send_email

import os
BASE_DIR = Path(__file__).parent.parent
DATA_DIR  = Path(os.getenv("DATA_DIR", str(BASE_DIR / "cache")))
DATA_DIR.mkdir(parents=True, exist_ok=True)
PENDING_JOBS_FILE = DATA_DIR / "pending_jobs.json"

scheduler = AsyncIOScheduler(timezone="America/Mexico_City")

# ---------------------------------------------------------------------------
# Flow definitions: (delay_hours, template_file, subject)
# ---------------------------------------------------------------------------

WELCOME_STEPS = [
    (0,       "welcome_01_bienvenida.html",   "Bienvenido a LEVIA™ — Ahora descansás distinto"),
    (3 * 24,  "welcome_02_mecanismo.html",    "Por qué tu almohada te traiciona — LEVIA™"),
    (7 * 24,  "welcome_03_ultimallamada.html","Tu código expira mañana — LEVIA™"),
]

ABANDONED_STEPS = [
    (0.5,     "abandoned_01_recordatorio.html", "Dejaste algo en tu carrito — LEVIA™"),
    (4,       "abandoned_02_testimonio.html",   "Ocho horas que cambian el día — LEVIA™"),
    (24,      "abandoned_03_faq.html",          "Las tres dudas que siempre nos hacen — LEVIA™"),
    (4 * 24,  "abandoned_04_descuento.html",    "Un 10% más, por si es el empujón — LEVIA™"),
    (5 * 24,  "abandoned_05_ultimallamada.html","Cerramos tu carrito mañana — LEVIA™"),
]

POSTPURCHASE_STEPS = [
    (0,       "postpurchase_01_confirmacion.html", "Tu LEVIA está en camino — LEVIA™"),
    (3 * 24,  "postpurchase_02_guiaUso.html",      "Cómo usar tu LEVIA las primeras noches — LEVIA™"),
    (14 * 24, "postpurchase_03_resena.html",       "¿Cambió tu mañana? — LEVIA™"),
]


# ---------------------------------------------------------------------------
# Pending jobs persistence
# ---------------------------------------------------------------------------

def _load_pending() -> list:
    if PENDING_JOBS_FILE.exists():
        try:
            return json.loads(PENDING_JOBS_FILE.read_text())
        except Exception:
            return []
    return []


def _save_pending(jobs: list):
    PENDING_JOBS_FILE.write_text(json.dumps(jobs, default=str))


def _add_pending(job_id: str, flow: str, step: int, email: str,
                 template: str, subject: str, context: dict, run_at: datetime):
    jobs = _load_pending()
    jobs = [j for j in jobs if j["job_id"] != job_id]
    jobs.append({
        "job_id": job_id,
        "flow": flow,
        "step": step,
        "email": email,
        "template": template,
        "subject": subject,
        "context": context,
        "run_at": run_at.isoformat(),
    })
    _save_pending(jobs)


def _remove_pending(job_id: str):
    jobs = [j for j in _load_pending() if j["job_id"] != job_id]
    _save_pending(jobs)


def _cancel_pending_by_prefix(prefix: str):
    jobs = _load_pending()
    for j in jobs:
        if j["job_id"].startswith(prefix):
            try:
                scheduler.remove_job(j["job_id"])
            except Exception:
                pass
    _save_pending([j for j in jobs if not j["job_id"].startswith(prefix)])


# ---------------------------------------------------------------------------
# Scheduling helpers
# ---------------------------------------------------------------------------

async def _send_and_cleanup(job_id: str, to: str, subject: str, template: str, context: dict):
    await send_email(to, subject, template, context)
    _remove_pending(job_id)


def _schedule_step(job_id: str, delay_hours: float, to: str,
                   subject: str, template: str, context: dict, flow: str, step: int):
    run_at = datetime.now(ZoneInfo("America/Mexico_City")) + timedelta(hours=delay_hours)
    _add_pending(job_id, flow, step, to, template, subject, context, run_at)
    scheduler.add_job(
        _send_and_cleanup,
        "date",
        run_date=run_at,
        id=job_id,
        replace_existing=True,
        args=[job_id, to, subject, template, context],
    )


# ---------------------------------------------------------------------------
# Public API: trigger flows
# ---------------------------------------------------------------------------

def trigger_welcome_flow(customer: dict):
    email = customer.get("email", "")
    if not email:
        return
    ctx = {"first_name": customer.get("first_name", "")}
    for i, (delay_h, template, subject) in enumerate(WELCOME_STEPS):
        _schedule_step(f"welcome_{email}_{i}", delay_h, email, subject, template, ctx, "welcome", i)
    print(f"[automation] Welcome flow queued for {email}")


def trigger_abandoned_cart_flow(checkout: dict):
    email = checkout.get("email", "")
    if not email:
        return
    checkout_url = checkout.get("abandoned_checkout_url", "https://levia.care/cart")
    product_title = "LEVIA Align"
    if checkout.get("line_items"):
        product_title = checkout["line_items"][0].get("title", product_title)
    ctx = {"checkout_url": checkout_url, "product_title": product_title}
    for i, (delay_h, template, subject) in enumerate(ABANDONED_STEPS):
        _schedule_step(f"abandoned_{email}_{i}", delay_h, email, subject, template, ctx, "abandoned", i)
    print(f"[automation] Abandoned cart flow queued for {email}")


def trigger_post_purchase_flow(order: dict):
    customer = order.get("customer") or {}
    email = customer.get("email") or order.get("email", "")
    if not email:
        return
    ctx = {
        "first_name": customer.get("first_name", ""),
        "order_name": order.get("name", ""),
        "order_status_url": order.get("order_status_url", "https://levia.care/account"),
    }
    for i, (delay_h, template, subject) in enumerate(POSTPURCHASE_STEPS):
        _schedule_step(f"postpurchase_{email}_{i}", delay_h, email, subject, template, ctx, "postpurchase", i)
    print(f"[automation] Post-purchase flow queued for {email}")


def cancel_abandoned_for_email(email: str):
    _cancel_pending_by_prefix(f"abandoned_{email}_")
    print(f"[automation] Abandoned cart flow cancelled for {email}")


# ---------------------------------------------------------------------------
# Scheduler lifecycle
# ---------------------------------------------------------------------------

def start_scheduler():
    scheduler.start()
    _restore_pending_jobs()
    _schedule_daily_cfdi()


def _schedule_daily_cfdi():
    from routers.sat import auto_generate_yesterday_cfdi
    scheduler.add_job(
        auto_generate_yesterday_cfdi,
        "cron",
        hour=8,
        minute=0,
        id="daily_cfdi_global",
        replace_existing=True,
        timezone="America/Mexico_City",
    )
    print("[automation] Job CFDI global diario programado a las 8:00 AM")


def _restore_pending_jobs():
    jobs = _load_pending()
    if not jobs:
        return
    now = datetime.now()
    restored = 0
    for j in jobs:
        run_at = datetime.fromisoformat(j["run_at"])
        # Past jobs fire 5 seconds from now instead of being dropped
        if run_at <= now:
            run_at = now + timedelta(seconds=5)
        try:
            scheduler.add_job(
                _send_and_cleanup,
                "date",
                run_date=run_at,
                id=j["job_id"],
                replace_existing=True,
                args=[j["job_id"], j["email"], j["subject"], j["template"], j["context"]],
            )
            restored += 1
        except Exception as e:
            print(f"[automation] Could not restore job {j['job_id']}: {e}")
    print(f"[automation] Restored {restored} pending jobs on startup")
