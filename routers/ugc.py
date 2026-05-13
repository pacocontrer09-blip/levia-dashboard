import json
from pathlib import Path
from datetime import datetime
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse

LEVIA_DIR = Path(__file__).parent.parent.parent
UGC_DIR = LEVIA_DIR / "12_CREATIVOS_UGC"
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))
router = APIRouter()


def _load_json(path: Path) -> list | dict:
    try:
        return json.loads(path.read_text())
    except Exception:
        return [] if not path.exists() else {}


def _get_ugc_weeks() -> list[dict]:
    output_dir = UGC_DIR / "output"
    if not output_dir.exists():
        return []
    weeks = []
    for folder in sorted(output_dir.iterdir(), reverse=True):
        if not folder.is_dir():
            continue
        mp4s = list(folder.glob("*.mp4"))
        mp3s = list(folder.glob("**/*.mp3"))
        weeks.append({
            "name": folder.name,
            "mp4_count": len(mp4s),
            "mp3_count": len(mp3s),
            "videos": [{"name": v.name, "url": f"/ugc_output/{folder.name}/{v.name}"} for v in mp4s],
            "size_mb": round(sum(v.stat().st_size for v in mp4s) / 1_048_576, 1),
        })
    return weeks


@router.get("/", response_class=HTMLResponse)
async def ugc_page(request: Request):
    pending = _load_json(UGC_DIR / "pending_manual.json")
    if not isinstance(pending, list):
        pending = []

    quota_path = UGC_DIR / "opal_quota.json"
    quota = _load_json(quota_path) if quota_path.exists() else {}
    quota_used = quota.get("clips_generated_today", 0)
    quota_date = quota.get("date", "—")
    quota_limit = quota.get("daily_limit", 10)

    weeks = _get_ugc_weeks()
    current_week = weeks[0] if weeks else None

    # Load hooks library count
    hooks_path = UGC_DIR / "hooks_library.json"
    hooks_count = 0
    if hooks_path.exists():
        try:
            hooks_data = json.loads(hooks_path.read_text())
            hooks_count = len(hooks_data) if isinstance(hooks_data, list) else sum(
                len(v) for v in hooks_data.values() if isinstance(v, list)
            )
        except Exception:
            pass

    # Creative matrix count
    matrix_path = UGC_DIR / "creative_matrix.json"
    matrix_count = 0
    if matrix_path.exists():
        try:
            matrix_data = json.loads(matrix_path.read_text())
            matrix_count = len(matrix_data) if isinstance(matrix_data, list) else 0
        except Exception:
            pass

    return templates.TemplateResponse("ugc.html", {
        "request": request,
        "page": "ugc",
        "pending": pending,
        "pending_count": len(pending),
        "quota_used": quota_used,
        "quota_limit": quota_limit,
        "quota_date": quota_date,
        "weeks": weeks,
        "current_week": current_week,
        "total_weeks": len(weeks),
        "hooks_count": hooks_count,
        "matrix_count": matrix_count,
    })
