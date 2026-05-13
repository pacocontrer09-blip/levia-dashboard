"""
Microsoft Clarity — service layer
Project: LEVIA™ Align (wqjxxfweel)

Clarity data is accessed via its web dashboard; the Export API requires
Azure AD OAuth which can be added later (see CLARITY_API_NOTES below).

For now this module provides:
  - Project URLs for deep-links
  - Iframe-safe proxy status (Clarity blocks X-Frame-Options)
  - Placeholder for future Export API calls
"""
import os
from datetime import datetime, timedelta

CLARITY_PROJECT_ID = os.getenv("CLARITY_PROJECT_ID", "wqjxxfweel")
CLARITY_BASE_URL = "https://clarity.microsoft.com/projects/view"


def get_clarity_urls() -> dict:
    """Return deep-link URLs for the Clarity dashboard sections."""
    base = f"{CLARITY_BASE_URL}/{CLARITY_PROJECT_ID}"
    return {
        "dashboard":    f"{base}/dashboard",
        "recordings":   f"{base}/recordings",
        "heatmaps":     f"{base}/heatmaps",
        "settings":     f"{base}/settings",
        "setup":        f"{base}/settings#setup",
        "project_id":   CLARITY_PROJECT_ID,
    }


def get_clarity_status() -> dict:
    """
    Returns the current Clarity tracking status.
    Snippet was installed on {date} — data takes ~2h to appear.
    """
    install_date = datetime(2026, 5, 13, 20, 0, 0)  # approx install time
    now = datetime.utcnow()
    hours_since = (now - install_date).total_seconds() / 3600
    data_ready = hours_since >= 2

    return {
        "project_id": CLARITY_PROJECT_ID,
        "snippet_installed": True,
        "install_date": install_date.strftime("%d %b %Y %H:%M UTC"),
        "data_ready": data_ready,
        "hours_since_install": round(hours_since, 1),
        "status_label": "Activo — datos disponibles" if data_ready else "Instalado — esperando primeros datos (~2h)",
        "status_color": "green" if data_ready else "yellow",
        "urls": get_clarity_urls(),
    }


# ---------------------------------------------------------------------------
# CLARITY_API_NOTES — Future Export API integration
# ---------------------------------------------------------------------------
# The Clarity Data Export API requires Azure AD OAuth 2.0:
#   Endpoint:  GET https://www.clarity.ms/export/api/v1/
#   Auth:      Bearer token from Azure AD
#   App reg:   needs ClarityReadUser scope
#
# When ready to integrate:
# 1. Register app in Azure portal (portal.azure.com)
# 2. Add env vars: CLARITY_TENANT_ID, CLARITY_CLIENT_ID, CLARITY_CLIENT_SECRET
# 3. Use msal library to get Bearer token
# 4. Query: /projects/{projectId}/metrics?startDate=...&endDate=...
#
# Available metrics: sessions, users, pages_per_session, scroll_depth,
#                    rage_clicks, dead_clicks, excessive_scroll, quick_backs
# ---------------------------------------------------------------------------
