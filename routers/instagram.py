import json
import shutil
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

LEVIA_DIR = Path(__file__).parent.parent.parent
IG_DIR = LEVIA_DIR / "15_INSTAGRAM_AGENT"
CONTENT_DIR = IG_DIR / "content"
APPROVED_DIR = IG_DIR / "approved"
CALENDAR_PATH = IG_DIR / "calendar.json"

templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))
router = APIRouter()


def _load_drafts() -> list[dict]:
    drafts = []
    if not CONTENT_DIR.exists():
        return drafts
    for folder in sorted(CONTENT_DIR.iterdir(), reverse=True):
        draft_path = folder / "draft.json"
        if not draft_path.exists():
            continue
        try:
            d = json.loads(draft_path.read_text())
            # Attach asset URLs for preview
            assets_dir = folder / "assets"
            if assets_dir.exists():
                d["asset_urls"] = [
                    f"/ig_content/{folder.name}/assets/{f.name}"
                    for f in sorted(assets_dir.iterdir())
                    if f.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}
                ]
            else:
                d["asset_urls"] = []
            drafts.append(d)
        except Exception:
            pass
    return drafts


def _load_approved() -> list[dict]:
    approved = []
    if not APPROVED_DIR.exists():
        return approved
    for folder in sorted(APPROVED_DIR.iterdir(), reverse=True):
        draft_path = folder / "draft.json"
        if draft_path.exists():
            try:
                approved.append(json.loads(draft_path.read_text()))
            except Exception:
                pass
    return approved


def _load_calendar_week() -> list[dict]:
    if not CALENDAR_PATH.exists():
        return []
    calendar = json.loads(CALENDAR_PATH.read_text())
    today = date.today()
    # Find the Monday of current week
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)
    week_days = []
    for week in calendar:
        for day in week["days"]:
            d = date.fromisoformat(day["date"])
            if monday <= d <= sunday:
                day["is_today"] = d == today
                day["is_past"] = d < today
                week_days.append(day)
    return week_days


def _next_scheduled() -> dict | None:
    if not CALENDAR_PATH.exists():
        return None
    calendar = json.loads(CALENDAR_PATH.read_text())
    today = date.today()
    future = []
    for week in calendar:
        for day in week["days"]:
            d = date.fromisoformat(day["date"])
            if d >= today:
                future.append((d, day))
    if not future:
        return None
    future.sort(key=lambda x: x[0])
    return future[0][1]


@router.get("/", response_class=HTMLResponse)
async def instagram_page(request: Request):
    drafts = _load_drafts()
    approved = _load_approved()
    calendar_week = _load_calendar_week()
    next_post = _next_scheduled()

    # Stats
    published_this_week = sum(
        1 for a in approved
        if a.get("published_at") and
        (datetime.now() - datetime.fromisoformat(a["published_at"])).days < 7
    )

    return templates.TemplateResponse("instagram.html", {
        "request": request,
        "page": "instagram",
        "drafts": drafts,
        "drafts_count": len(drafts),
        "approved": approved[:5],
        "published_this_week": published_this_week,
        "calendar_week": calendar_week,
        "next_post": next_post,
    })


@router.post("/generate", response_class=HTMLResponse)
async def generate_content(
    request: Request,
    content_type: str = Form(...),
    topic: str = Form(...),
):
    orchestrator = IG_DIR / "orchestrator.py"
    env_path = LEVIA_DIR / "13_DASHBOARD" / ".env"

    cmd = [sys.executable, str(orchestrator), "--type", content_type, "--topic", topic]
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=str(IG_DIR))

    if result.returncode != 0:
        return HTMLResponse(
            f'<div class="alert red">Error al generar: {result.stderr[:300]}</div>',
            status_code=500,
        )

    drafts = _load_drafts()
    # Return updated draft queue partial
    return templates.TemplateResponse("instagram.html", {
        "request": request,
        "page": "instagram",
        "drafts": drafts,
        "drafts_count": len(drafts),
        "approved": _load_approved()[:5],
        "published_this_week": 0,
        "calendar_week": _load_calendar_week(),
        "next_post": _next_scheduled(),
        "flash": f"Draft generado: {drafts[0]['id'] if drafts else '?'}",
    })


@router.post("/approve/{content_id}", response_class=JSONResponse)
async def approve_content(content_id: str):
    src = CONTENT_DIR / content_id
    draft_path = src / "draft.json"
    if not draft_path.exists():
        return JSONResponse({"success": False, "error": "Draft no encontrado"}, status_code=404)

    draft = json.loads(draft_path.read_text())

    # Move to approved/
    dst = APPROVED_DIR / content_id
    APPROVED_DIR.mkdir(exist_ok=True)
    shutil.move(str(src), str(dst))

    # Build public asset URLs (must be accessible from internet for Meta API)
    # In local dev, assets can't be published automatically — flag it
    draft["status"] = "approved"
    draft["published_at"] = datetime.now().isoformat()

    # Attempt publish only if assets exist and have public URLs
    assets_dir = dst / "assets"
    asset_files = sorted(assets_dir.iterdir()) if assets_dir.exists() else []
    has_assets = len(asset_files) > 0

    pub_result = {"success": False, "ig_post_id": None, "error": "Sin assets para publicar"}

    if has_assets:
        # Assets need to be publicly reachable URLs — skip auto-publish locally
        pub_result["error"] = (
            "Assets locales detectados. Sube las imágenes a una URL pública "
            "y actualiza el draft.json para publicar vía Meta API."
        )
    else:
        pub_result["error"] = "No hay imágenes en assets/. Agrega imágenes y vuelve a aprobar."

    draft["ig_post_id"] = pub_result.get("ig_post_id")
    (dst / "draft.json").write_text(json.dumps(draft, ensure_ascii=False, indent=2))

    return JSONResponse({
        "success": True,
        "publish_result": pub_result,
        "message": "Movido a approved/. " + (pub_result.get("error") or "Publicado."),
    })


@router.post("/reject/{content_id}", response_class=JSONResponse)
async def reject_content(content_id: str):
    src = CONTENT_DIR / content_id
    if not src.exists():
        return JSONResponse({"success": False, "error": "Draft no encontrado"}, status_code=404)
    shutil.rmtree(str(src))
    return JSONResponse({"success": True, "message": f"Draft {content_id} eliminado"})
