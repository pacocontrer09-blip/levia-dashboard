import os
import sys
import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env", override=True)

BASE_DIR = Path(__file__).parent
LEVIA_DIR = BASE_DIR.parent  # 01_Proyectos/Levia/

logger = logging.getLogger("levia.startup")
logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")


def _validate_env():
    PLACEHOLDER = "levia-email-secret-change-me"
    hmac_secret = os.getenv("EMAIL_HMAC_SECRET", "")
    if hmac_secret == PLACEHOLDER or len(hmac_secret) < 32:
        logger.error("EMAIL_HMAC_SECRET no está configurado o es el placeholder por defecto. "
                     "Genera uno con: python3 -c \"import secrets; print(secrets.token_hex(32))\"")
        sys.exit(1)

    warnings = []
    if not os.getenv("SHOPIFY_TOKEN"):
        warnings.append("SHOPIFY_TOKEN no definido — datos de Shopify no disponibles")
    if not os.getenv("META_ACCESS_TOKEN"):
        warnings.append("META_ACCESS_TOKEN no definido — datos de Meta Ads no disponibles")
    if not os.getenv("RESEND_API_KEY"):
        warnings.append("RESEND_API_KEY no definido — emails de automatización desactivados")
    if not os.getenv("KLAVIYO_KEY"):
        warnings.append("KLAVIYO_KEY no definido — datos de Klaviyo no disponibles")

    for w in warnings:
        logger.warning(w)


_validate_env()


@asynccontextmanager
async def lifespan(app: FastAPI):
    from services.automation_service import start_scheduler
    start_scheduler()
    yield


app = FastAPI(title="LEVIA™ Ops Dashboard", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# Static + local-only mounts — conditional for cloud compatibility
def _mount_if_exists(path: Path, route: str, name: str):
    if path.exists():
        app.mount(route, StaticFiles(directory=str(path)), name=name)
    else:
        logger.info(f"Directorio local no disponible (cloud mode): {path}")

_mount_if_exists(BASE_DIR / "static", "/static", "static")

CREATIVOS_DIR    = LEVIA_DIR / "03_ADS_Y_COPY" / "creativos"
UGC_OUTPUT_DIR   = LEVIA_DIR / "12_CREATIVOS_UGC" / "output"
VIDEOS_REF_DIR   = LEVIA_DIR / "03_ADS_Y_COPY" / "creativos" / "videos_referencia"
AGENCY_CLIPS_DIR = LEVIA_DIR / "11_MARKETING_AGENCY" / "clips"
AGENCY_OUT_DIR   = LEVIA_DIR / "11_MARKETING_AGENCY" / "out"
META_ADS_DIR     = LEVIA_DIR / "03_ADS_Y_COPY" / "creativos" / "meta_ads_mayo2026"

_mount_if_exists(CREATIVOS_DIR,    "/creativos",   "creativos")
_mount_if_exists(UGC_OUTPUT_DIR,   "/ugc_output",  "ugc_output")
_mount_if_exists(VIDEOS_REF_DIR,   "/videos_ref",  "videos_ref")
_mount_if_exists(AGENCY_CLIPS_DIR, "/agency_clips","agency_clips")
_mount_if_exists(AGENCY_OUT_DIR,   "/agency_out",  "agency_out")
_mount_if_exists(META_ADS_DIR,     "/meta_ads",    "meta_ads")

templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# Register routers
from routers import overview, finance, ads, creatives, ugc, bank, ventas, inventario, webhooks, clientes, analitica, email_admin, klaviyo, sat, clarity

app.include_router(overview.router)
app.include_router(ventas.router, prefix="/ventas")
app.include_router(finance.router, prefix="/finance")
app.include_router(ads.router, prefix="/ads")
app.include_router(inventario.router, prefix="/inventario")
app.include_router(creatives.router, prefix="/creatives")
app.include_router(ugc.router, prefix="/ugc")
app.include_router(bank.router, prefix="/bank")
app.include_router(clientes.router, prefix="/clientes")
app.include_router(analitica.router, prefix="/analitica")
app.include_router(webhooks.router, prefix="/webhooks")
app.include_router(email_admin.router, prefix="/email")
app.include_router(klaviyo.router, prefix="/klaviyo")
app.include_router(sat.router)
app.include_router(clarity.router, prefix="/clarity")


@app.get("/admin/setup-funda")
async def setup_funda():
    """One-time endpoint: creates LEVIA™ Funda product in Shopify and returns variant_id."""
    from services import shopify_service
    result = await shopify_service.create_funda_product()
    return JSONResponse(result)


@app.get("/health")
async def health():
    checks = {
        "agent_state": (LEVIA_DIR / "11_MARKETING_AGENCY" / "agent_state.json").exists(),
        "pending_manual": (LEVIA_DIR / "12_CREATIVOS_UGC" / "pending_manual.json").exists(),
        "creativos_dir": CREATIVOS_DIR.exists(),
        "ugc_output_dir": UGC_OUTPUT_DIR.exists(),
    }
    status = "ok" if all(checks.values()) else "degraded"
    return JSONResponse({"status": status, "checks": checks})
