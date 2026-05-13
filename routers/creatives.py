import json
import re
from datetime import date
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse

LEVIA_DIR = Path(__file__).parent.parent.parent
CREATIVOS_DIR = LEVIA_DIR / "03_ADS_Y_COPY" / "creativos"
UGC_DIR = LEVIA_DIR / "12_CREATIVOS_UGC"
AGENCY_DIR = LEVIA_DIR / "11_MARKETING_AGENCY"
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))
router = APIRouter()


# ─── Metadata de ángulos — fuente única de verdad ─────────────────────────────
# Cada entrada define: angle, avatar, cold/warm, priority, market_gap
ANGLE_META: dict[str, dict] = {
    # F49–F54 (serie actual)
    "F49": {"angle": "Almohada Enemigo",       "avatar": "Mujer 30-45",      "cold": True,  "priority": "alta",  "gap": "Replicar pokevision88 (85K likes)"},
    "F50": {"angle": "Primera Noche",          "avatar": "Mujer 30-45",      "cold": True,  "priority": "alta",  "gap": "Especificidad de resultado — primera noche"},
    "F51": {"angle": "Pareja / Snoring",       "avatar": "Pareja 30-50",     "cold": True,  "priority": "alta",  "gap": "Ángulo virgen en español"},
    "F52": {"angle": "Prueba sin Riesgo",      "avatar": "Mujer 30-45",      "cold": False, "priority": "media", "gap": "Mejor en retarget / warm audience"},
    "F53": {"angle": "3 Zonas Cervicales",     "avatar": "Educacional",      "cold": True,  "priority": "media", "gap": "Replicar CloudAlign '3 zonas' en español"},
    "F54": {"angle": "Mañanas Diferentes",     "avatar": "Mujer 30-45",      "cold": False, "priority": "media", "gap": "Aspiracional — mejor en warm"},
    # F55–F59 (nuevos ángulos)
    "F55": {"angle": "Avatar Masculino",       "avatar": "Hombre 38-50",     "cold": True,  "priority": "alta",  "gap": "NADIE en MX usa avatar masculino en cervicales"},
    "F56": {"angle": "Home Office",            "avatar": "Mujer profesional","cold": True,  "priority": "alta",  "gap": "Ángulo virgen en español — millones en home office MX"},
    "F57": {"angle": "Comparación Directa",   "avatar": "Todos",            "cold": True,  "priority": "alta",  "gap": "vs almohada genérica — nobody en español"},
    "F58": {"angle": "Pareja / Alineación",   "avatar": "Pareja 30-50",     "cold": True,  "priority": "alta",  "gap": "Ronquidos + pareja — CloudAlign 'saved my marriage'"},
    "F59": {"angle": "UGC Testimonial",        "avatar": "Mujer 28-35",      "cold": True,  "priority": "alta",  "gap": "Especificidad de tiempo: '3 años / 3ª noche'"},
    # Series anteriores (labels de ángulo)
    "A1":  {"angle": "Dolor Matutino",         "avatar": "Mujer 30-45",      "cold": True,  "priority": "alta",  "gap": "Hero creativo — mañana"},
    "A2":  {"angle": "Reptil (27kg)",          "avatar": "Mujer 30-45",      "cold": True,  "priority": "alta",  "gap": "Dato alarma — presión cervical"},
    "A3":  {"angle": "Autoridad Clínica",      "avatar": "Mujer +40",        "cold": True,  "priority": "alta",  "gap": "Neocórtex — credencial"},
    "A11": {"angle": "Clínica +40",            "avatar": "Mujer +40",        "cold": True,  "priority": "media", "gap": "Autoridad para avatar maduro"},
    "F47": {"angle": "Clinical Trust",        "avatar": "Todos",            "cold": False, "priority": "baja",  "gap": "Mejor en retarget — posicionamiento premium"},
    "F40": {"angle": "Primera Noche",          "avatar": "Mujer 30-45",      "cold": True,  "priority": "media", "gap": "Lifestyle — habitación cálida"},
    "F28": {"angle": "Llamado al Descanso",    "avatar": "Mujer 30-45",      "cold": False, "priority": "baja",  "gap": "Aspiracional copy — mejor warm"},
}

# Avatares que queremos cubrir y si existen en el inventario
AVATAR_COVERAGE = [
    {"avatar": "Mujer 30-45",      "icon": "👩",  "status": "covered",   "frames": ["F49","F50","F54","F59","A1","A2","F40"]},
    {"avatar": "Hombre 38-50",     "icon": "🧑",  "status": "covered",   "frames": ["F55"]},
    {"avatar": "Pareja 30-50",     "icon": "👫",  "status": "covered",   "frames": ["F51","F58"]},
    {"avatar": "Mujer profesional","icon": "💼",  "status": "covered",   "frames": ["F56"]},
    {"avatar": "Mujer +40",        "icon": "👩‍⚕️", "status": "partial",   "frames": ["A3","A11"]},
    {"avatar": "Fisio / Quiro",    "icon": "🩺",  "status": "missing",   "frames": []},
    {"avatar": "Postparto",        "icon": "🤱",  "status": "missing",   "frames": []},
]

# Referencias de competencia (de videos_referencia/INDEX.md)
COMPETITOR_REFS = [
    {
        "handle": "pokevision88",
        "title": "Tu almohada vieja está arruinando tu cuello",
        "url": "https://www.tiktok.com/@pokevision88/video/7588184249828773142",
        "likes": 85500,
        "lang": "ES",
        "brain": "Reptil",
        "angle": "Almohada Enemigo",
        "levia_fit": 95,
        "adaptation": "F49 ya captura este ángulo — testear primero",
    },
    {
        "handle": "vipfree_erica01",
        "title": "UGC Testimonial — resultado personal",
        "url": "https://www.tiktok.com/@vipfree_erica01/video/7523297755708656909",
        "likes": 17100,
        "lang": "EN",
        "brain": "Límbico + Social Proof",
        "angle": "UGC Auténtico",
        "levia_fit": 88,
        "adaptation": "F59 replica este formato — micro-influencer MX 30-45",
    },
    {
        "handle": "marandaleann",
        "title": "Neck Pain Review — dolor crónico",
        "url": "https://www.tiktok.com/@marandaleann/video/7384817935627537710",
        "likes": 13800,
        "lang": "EN",
        "brain": "Límbico",
        "angle": "Despertador",
        "levia_fit": 82,
        "adaptation": "F59 — especificidad de tiempo ('2 años', 'tercera noche')",
    },
    {
        "handle": "steverecomienda",
        "title": "Si te despiertas con dolor de cuello",
        "url": "https://www.tiktok.com/@steverecomienda/video/7580835870954114312",
        "likes": 500,
        "lang": "ES",
        "brain": "Límbico",
        "angle": "Dolor Matutino UGC",
        "levia_fit": 78,
        "adaptation": "Muestra que el formato funciona con bajo presupuesto en español",
    },
    {
        "handle": "jamiehealthcoach",
        "title": "Health Coach recomienda almohada cervical",
        "url": "https://www.tiktok.com/@jamiehealthcoach/video/7550878773667826999",
        "likes": None,
        "lang": "EN",
        "brain": "Neocórtex",
        "angle": "Autoridad de Salud",
        "levia_fit": 75,
        "adaptation": "Buscar fisio CDMX/GDL — selfie style, 20s",
    },
    {
        "handle": "dr.ravelo.trauma",
        "title": "Doctor explica mecanismo dolor cervical",
        "url": "https://www.tiktok.com/@dr..ravelo.trauma/video/7633186932020743444",
        "likes": None,
        "lang": "ES",
        "brain": "Neocórtex",
        "angle": "Autoridad Médica",
        "levia_fit": 72,
        "adaptation": "Ángulo 1 (Quiropráctico) — mercado hispano acepta este formato",
    },
]


TESTING_JSON = AGENCY_DIR / "testing_creatives.json"

# Directorios de video candidatos para testing (url_prefix, directorio_local)
VIDEO_SOURCES = [
    ("videos_ref/_levia_hooked",   LEVIA_DIR / "03_ADS_Y_COPY/creativos/videos_referencia/_levia_hooked"),
    ("videos_ref/_levia_adapted",  LEVIA_DIR / "03_ADS_Y_COPY/creativos/videos_referencia/_levia_adapted"),
    ("videos_ref",                 LEVIA_DIR / "03_ADS_Y_COPY/creativos/videos_referencia"),
    ("agency_clips/originals",     LEVIA_DIR / "11_MARKETING_AGENCY/clips/originals"),
]

STATUS_LABELS = {
    "queued":       ("EN COLA",     "#e0e7ff", "#3730a3"),
    "pending_edit": ("PENDIENTE",   "#fef3c7", "#92400e"),
    "testing":      ("EN TEST",     "#d1fae5", "#065f46"),
    "winner":       ("WINNER",      "#bbf7d0", "#14532d"),
    "loser":        ("LOSER",       "#fee2e2", "#991b1b"),
}


def _load_json(path: Path):
    try:
        return json.loads(path.read_text())
    except Exception:
        return {}


def _save_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False))


def _source_path_to_url(sp: str) -> str:
    sp = sp.replace("\\", "/")
    if "_levia_hooked/" in sp:
        return "/videos_ref/_levia_hooked/" + sp.split("_levia_hooked/")[-1]
    if "_levia_adapted/" in sp:
        return "/videos_ref/_levia_adapted/" + sp.split("_levia_adapted/")[-1]
    if "videos_referencia/" in sp:
        return "/videos_ref/" + sp.split("videos_referencia/")[-1]
    if "clips/originals/" in sp:
        return "/agency_clips/originals/" + sp.split("originals/")[-1]
    if "11_MARKETING_AGENCY/out/" in sp:
        return "/agency_out/" + sp.split("/out/")[-1]
    if "12_CREATIVOS_UGC/output/" in sp:
        return "/ugc_output/" + sp.split("output/")[-1]
    if "meta_ads_mayo2026/" in sp:
        return "/creativos/meta_ads_mayo2026/" + sp.split("meta_ads_mayo2026/")[-1]
    return ""


def _get_testing_creatives() -> tuple[list[dict], dict]:
    items = _load_json(TESTING_JSON)
    if not isinstance(items, list):
        items = []

    # Enrich with urls and status labels
    for item in items:
        item["url"] = _source_path_to_url(item.get("source_path", ""))
        sl = STATUS_LABELS.get(item.get("status", "queued"), ("—", "#f3f4f6", "#6b7280"))
        item["status_label"], item["status_bg"], item["status_fg"] = sl

    stats = {
        "queued":       sum(1 for i in items if i.get("status") == "queued"),
        "pending_edit": sum(1 for i in items if i.get("status") == "pending_edit"),
        "testing":      sum(1 for i in items if i.get("status") == "testing"),
        "winner":       sum(1 for i in items if i.get("status") == "winner"),
        "loser":        sum(1 for i in items if i.get("status") == "loser"),
        "total":        len(items),
    }
    return items, stats


def _scan_candidate_videos(existing_paths: set) -> list[dict]:
    candidates = []
    for url_prefix, directory in VIDEO_SOURCES:
        if not directory.exists():
            continue
        for ext in ("*.mp4", "*.mov"):
            for vid in sorted(directory.glob(ext)):
                rel = str(vid.relative_to(LEVIA_DIR)).replace("\\", "/")
                if rel in existing_paths:
                    continue
                name = vid.stem.replace("_CON_HOOK", "").replace("_", " ").title()
                candidates.append({
                    "source_path": rel,
                    "name": name,
                    "url": f"/{url_prefix}/{vid.name}",
                    "size_mb": round(vid.stat().st_size / 1_048_576, 1),
                    "type": "hooked" if "_levia_hooked" in url_prefix else
                            "adapted" if "_levia_adapted" in url_prefix else "original",
                })
    return candidates


def _get_creatives() -> list[dict]:
    if not CREATIVOS_DIR.exists():
        return []

    # Load live ads from creatives.json and test_results.json
    live_creatives = _load_json(AGENCY_DIR / "creatives.json")
    test_results = _load_json(AGENCY_DIR / "test_results.json")

    live_names: set = set()
    if isinstance(live_creatives, list):
        live_names = {c.get("name", "") for c in live_creatives}
    elif isinstance(live_creatives, dict):
        live_names = set(live_creatives.keys())

    perf_by_name: dict = {}
    if isinstance(test_results, list):
        for t in test_results:
            perf_by_name[t.get("name", "")] = t
    elif isinstance(test_results, dict):
        perf_by_name = test_results

    creatives = []
    for png in sorted(CREATIVOS_DIR.glob("*.png")):
        name = png.stem
        match = re.match(r"^(F\d+|A\d+)", name, re.IGNORECASE)
        frame_id = match.group(1).upper() if match else name.upper()

        label = name.replace("_final", "").replace("_", " ")
        label = re.sub(r"^[FA]\d+ ?", "", label, flags=re.IGNORECASE).title()

        is_live = any(frame_id.lower() in ln.lower() or name.lower() in ln.lower() for ln in live_names)
        perf = perf_by_name.get(name, perf_by_name.get(frame_id, {}))

        # Enrich with angle metadata
        meta = ANGLE_META.get(frame_id, {})

        # Derive format tag from filename
        fmt = "1:1"
        if "9x16" in name or "916" in name:
            fmt = "9:16"
        elif "1x1" in name or "11" in name:
            fmt = "1:1"

        creatives.append({
            "name": name,
            "frame_id": frame_id,
            "label": label or frame_id,
            "url": f"/creativos/{png.name}",
            "status": "live" if is_live else "ready",
            "roas": perf.get("roas"),
            "ctr_pct": perf.get("ctr_pct"),
            "platform": perf.get("platform", "—"),
            # Angle metadata
            "angle": meta.get("angle", "—"),
            "avatar": meta.get("avatar", "—"),
            "cold": meta.get("cold", True),
            "priority": meta.get("priority", "—"),
            "gap": meta.get("gap", ""),
            "format": fmt,
        })

    return creatives


@router.get("/", response_class=HTMLResponse)
async def creatives_page(request: Request):
    creatives = _get_creatives()
    live = [c for c in creatives if c["status"] == "live"]
    ready = [c for c in creatives if c["status"] == "ready"]
    cold = [c for c in creatives if c.get("cold")]
    high_priority = [c for c in creatives if c.get("priority") == "alta"]

    # UGC videos
    ugc_output = UGC_DIR / "output"
    ugc_videos = []
    if ugc_output.exists():
        for folder in sorted(ugc_output.iterdir(), reverse=True):
            for mp4 in folder.glob("*.mp4"):
                ugc_videos.append({
                    "name": mp4.stem,
                    "url": f"/ugc_output/{folder.name}/{mp4.name}",
                    "week": folder.name,
                })

    testing_items, testing_stats = _get_testing_creatives()
    existing_paths = {i["source_path"] for i in testing_items}
    testing_candidates = _scan_candidate_videos(existing_paths)

    return templates.TemplateResponse("creatives.html", {
        "request": request,
        "page": "creatives",
        "creatives": creatives,
        "live": live,
        "ready": ready,
        "total": len(creatives),
        "cold_count": len(cold),
        "high_priority_count": len(high_priority),
        "ugc_videos": ugc_videos[:12],
        "avatar_coverage": AVATAR_COVERAGE,
        "competitor_refs": sorted(COMPETITOR_REFS, key=lambda x: x["levia_fit"], reverse=True),
        "angle_meta": ANGLE_META,
        "testing_items": testing_items,
        "testing_stats": testing_stats,
        "testing_candidates": testing_candidates,
    })


@router.get("/testing", response_class=JSONResponse)
async def testing_data():
    """JSON: lista de creativos en testing + candidatos disponibles + stats."""
    items, stats = _get_testing_creatives()
    existing = {i["source_path"] for i in items}
    candidates = _scan_candidate_videos(existing)
    return {"items": items, "stats": stats, "candidates": candidates}


@router.post("/testing/add", response_class=JSONResponse)
async def testing_add(request: Request):
    """Agrega un video candidato al pipeline de testing."""
    body = await request.json()
    source_path = body.get("source_path", "")
    if not source_path:
        return JSONResponse({"ok": False, "error": "source_path requerido"}, status_code=400)

    items, _ = _get_testing_creatives()
    if any(i["source_path"] == source_path for i in items):
        return JSONResponse({"ok": False, "error": "Ya existe en testing"}, status_code=400)

    new_id = f"tc_{len(items) + 1:03d}"
    items.append({
        "id": new_id,
        "source_path": source_path,
        "name": body.get("name", Path(source_path).stem.replace("_", " ").title()),
        "type": body.get("type", "referencia"),
        "origin": body.get("origin", "competidor"),
        "status": "queued",
        "audit": {
            "hook_score": None,
            "pain_point": None,
            "product_showcase": None,
            "cta_clarity": None,
            "total": None,
            "emotion": "",
            "angle": body.get("angle", ""),
            "avatar": body.get("avatar", ""),
            "vic_notes": "",
        },
        "meta_ad_id": None,
        "roas": None,
        "ctr_pct": None,
        "spend": None,
        "added_date": str(date.today()),
    })
    _save_json(TESTING_JSON, [
        {k: v for k, v in i.items() if k not in ("url", "status_label", "status_bg", "status_fg")}
        for i in items
    ])
    return {"ok": True, "id": new_id}


@router.patch("/testing/{item_id}", response_class=JSONResponse)
async def testing_update(item_id: str, request: Request):
    """Actualiza status o audit de un creativo de testing."""
    body = await request.json()
    raw = _load_json(TESTING_JSON)
    if not isinstance(raw, list):
        return JSONResponse({"ok": False, "error": "JSON corrupto"}, status_code=500)

    for item in raw:
        if item["id"] == item_id:
            if "status" in body:
                item["status"] = body["status"]
            if "name" in body:
                item["name"] = body["name"]
            if "meta_ad_id" in body:
                item["meta_ad_id"] = body["meta_ad_id"]
            if "vic_notes" in body:
                item.setdefault("audit", {})["vic_notes"] = body["vic_notes"]
            if "audit" in body:
                item.setdefault("audit", {}).update(body["audit"])
                scores = [item["audit"].get(k) for k in ("hook_score", "pain_point", "product_showcase", "cta_clarity")]
                if all(s is not None for s in scores):
                    item["audit"]["total"] = sum(scores)
            _save_json(TESTING_JSON, raw)
            return {"ok": True}

    return JSONResponse({"ok": False, "error": "No encontrado"}, status_code=404)


@router.get("/analysis", response_class=JSONResponse)
async def creatives_analysis():
    """JSON endpoint: angle coverage, avatar matrix, priority distribution."""
    creatives = _get_creatives()

    # Angle distribution
    angle_dist: dict = {}
    for c in creatives:
        a = c.get("angle", "—")
        angle_dist[a] = angle_dist.get(a, 0) + 1

    # Avatar distribution
    avatar_dist: dict = {}
    for c in creatives:
        av = c.get("avatar", "—")
        avatar_dist[av] = avatar_dist.get(av, 0) + 1

    # Priority distribution
    prio_dist: dict = {}
    for c in creatives:
        p = c.get("priority", "—")
        prio_dist[p] = prio_dist.get(p, 0) + 1

    # High priority cold creatives (candidates to test)
    test_candidates = [
        {"frame_id": c["frame_id"], "label": c["label"], "angle": c["angle"],
         "avatar": c["avatar"], "gap": c["gap"]}
        for c in creatives
        if c.get("priority") == "alta" and c.get("cold") and c.get("status") == "ready"
    ][:10]

    return {
        "total": len(creatives),
        "angle_distribution": angle_dist,
        "avatar_distribution": avatar_dist,
        "priority_distribution": prio_dist,
        "avatar_coverage": AVATAR_COVERAGE,
        "competitor_refs": COMPETITOR_REFS,
        "test_candidates": test_candidates,
    }
