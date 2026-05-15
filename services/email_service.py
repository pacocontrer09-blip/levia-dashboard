import asyncio
import resend
import hmac
import hashlib
import base64
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from jinja2 import Environment, FileSystemLoader

BASE_DIR  = Path(__file__).parent.parent

def _resolve_data_dir() -> Path:
    """Usa /data (Railway Volume) si existe y es escribible, sino cache/ local."""
    volume = Path("/data")
    if volume.is_dir() and os.access(volume, os.W_OK):
        return volume
    fallback = BASE_DIR / "cache"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback

DATA_DIR  = _resolve_data_dir()
UNSUB_FILE = DATA_DIR / "unsubscribed.json"
LOG_FILE   = DATA_DIR / "email_log.json"
TEMPLATES_DIR = BASE_DIR / "templates" / "emails"

resend.api_key = os.getenv("RESEND_API_KEY", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "hola@levia.care")
EMAIL_FROM_NAME = os.getenv("EMAIL_FROM_NAME", "LEVIA™")
HMAC_SECRET = os.getenv("EMAIL_HMAC_SECRET", "levia-email-secret-change-me").encode()
DASHBOARD_BASE_URL = os.getenv("DASHBOARD_BASE_URL", "http://localhost:8000")

jinja_env = Environment(loader=FileSystemLoader(str(TEMPLATES_DIR)))


def _load_unsubscribed() -> set:
    if UNSUB_FILE.exists():
        try:
            return set(json.loads(UNSUB_FILE.read_text()))
        except Exception:
            return set()
    return set()


def _save_unsubscribed(emails: set):
    UNSUB_FILE.write_text(json.dumps(list(emails)))


def _append_log(entry: dict):
    try:
        log = json.loads(LOG_FILE.read_text()) if LOG_FILE.exists() else []
        log.append(entry)
        if len(log) > 5000:
            log = log[-5000:]
        LOG_FILE.write_text(json.dumps(log))
    except Exception:
        pass


def get_email_log(limit: int = 200) -> list:
    try:
        log = json.loads(LOG_FILE.read_text()) if LOG_FILE.exists() else []
        return list(reversed(log[-limit:]))
    except Exception:
        return []


def is_unsubscribed(email: str) -> bool:
    return email.lower() in _load_unsubscribed()


def mark_unsubscribed(email: str):
    emails = _load_unsubscribed()
    emails.add(email.lower())
    _save_unsubscribed(emails)


def generate_unsubscribe_token(email: str) -> str:
    sig = hmac.new(HMAC_SECRET, email.lower().encode(), hashlib.sha256).hexdigest()
    payload = f"{email.lower()}:{sig}"
    return base64.urlsafe_b64encode(payload.encode()).decode()


def validate_unsubscribe_token(token: str) -> str | None:
    try:
        payload = base64.urlsafe_b64decode(token.encode()).decode()
        email, sig = payload.rsplit(":", 1)
        expected = hmac.new(HMAC_SECRET, email.lower().encode(), hashlib.sha256).hexdigest()
        if hmac.compare_digest(sig, expected):
            return email
    except Exception:
        pass
    return None


def _generate_unsubscribe_url(email: str) -> str:
    token = generate_unsubscribe_token(email)
    return f"{DASHBOARD_BASE_URL}/email/unsubscribe?token={token}"


def render_template(template_name: str, context: dict) -> str:
    tpl = jinja_env.get_template(template_name)
    return tpl.render(**context)


async def send_email(to: str, subject: str, template_name: str, context: dict) -> bool:
    if is_unsubscribed(to):
        print(f"[email] Skipped (unsubscribed): {to}")
        return False

    context = {**context, "unsubscribe_url": _generate_unsubscribe_url(to)}
    html_body = render_template(template_name, context)

    ts = datetime.now(timezone.utc).isoformat()
    try:
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: resend.Emails.send({
            "from": f"{EMAIL_FROM_NAME} <{EMAIL_FROM}>",
            "to": [to],
            "subject": subject,
            "html": html_body,
        }))
        print(f"[email] Sent '{subject}' → {to}")
        _append_log({"ts": ts, "to": to, "subject": subject, "template": template_name, "status": "sent"})
        return True
    except Exception as e:
        print(f"[email] ERROR sending to {to}: {e}")
        _append_log({"ts": ts, "to": to, "subject": subject, "template": template_name, "status": "error", "error": str(e)})
        return False
