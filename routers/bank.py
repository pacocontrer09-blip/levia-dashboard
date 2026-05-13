"""
Nu México bank integration via CSV import.

Nu México → app → Estados de cuenta → Exportar CSV
El CSV de Nu tiene formato:
  Fecha,Descripción,Tipo,Monto,Saldo
  2026-05-01,OXXO PAGO,Cargo,-150.00,4850.00
"""
import csv
import io
import json
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, Request, UploadFile, File
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse

LEVIA_DIR = Path(__file__).parent.parent.parent
BANK_DATA_PATH = Path(__file__).parent.parent / "cache" / "nu_transactions.json"
templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))
router = APIRouter()

# Categorías por palabras clave en descripción
CATEGORIES = {
    "Meta": ["META", "FACEBOOK", "FB ADS"],
    "TikTok": ["TIKTOK", "BYTEDANCE"],
    "Shopify": ["SHOPIFY", "STRIPE"],
    "Proveedor": ["ALIBABA", "ALIEXPRESS", "PROVEEDOR", "CARGO CHINA"],
    "Envíos": ["FEDEX", "DHL", "ESTAFETA", "REDPACK", "SENDEX", "J&T"],
    "Cobro LEVIA": ["LEVIA", "DEPOSITO", "TRANSFERENCIA RECIBIDA"],
    "Servicios": ["TELMEX", "TELCEL", "CFE", "IZZI"],
    "Retiro": ["RETIRO", "CAJERO", "ATM"],
}


def _categorize(description: str) -> str:
    desc_upper = description.upper()
    for category, keywords in CATEGORIES.items():
        if any(kw in desc_upper for kw in keywords):
            return category
    return "Otro"


def _parse_nu_csv(content: str) -> list[dict]:
    """Parses Nu México CSV export."""
    transactions = []
    reader = csv.DictReader(io.StringIO(content))

    for row in reader:
        # Nu CSV headers vary slightly — handle both Spanish variants
        date_val = row.get("Fecha") or row.get("Date") or ""
        desc = row.get("Descripción") or row.get("Descripcion") or row.get("Description") or ""
        amount_str = row.get("Monto") or row.get("Amount") or row.get("Importe") or "0"
        balance_str = row.get("Saldo") or row.get("Balance") or "0"
        tx_type = row.get("Tipo") or row.get("Type") or ""

        # Clean amount
        amount = float(str(amount_str).replace(",", "").replace("$", "").strip() or 0)
        balance = float(str(balance_str).replace(",", "").replace("$", "").strip() or 0)

        transactions.append({
            "date": date_val,
            "description": desc,
            "type": tx_type,
            "amount": amount,
            "balance": balance,
            "category": _categorize(desc),
            "is_income": amount > 0,
        })

    # Sort newest first
    try:
        transactions.sort(key=lambda x: x["date"], reverse=True)
    except Exception:
        pass

    return transactions


def _load_transactions() -> list[dict]:
    if BANK_DATA_PATH.exists():
        try:
            return json.loads(BANK_DATA_PATH.read_text())
        except Exception:
            pass
    return []


def _save_transactions(txs: list[dict]):
    BANK_DATA_PATH.parent.mkdir(exist_ok=True)
    BANK_DATA_PATH.write_text(json.dumps(txs, ensure_ascii=False, indent=2))


def _summary(txs: list[dict]) -> dict:
    if not txs:
        return {"income": 0, "expenses": 0, "balance": 0, "by_category": {}}

    income = sum(t["amount"] for t in txs if t["amount"] > 0)
    expenses = abs(sum(t["amount"] for t in txs if t["amount"] < 0))
    latest_balance = txs[0]["balance"] if txs else 0

    by_cat: dict[str, float] = {}
    for t in txs:
        if t["amount"] < 0:
            cat = t["category"]
            by_cat[cat] = by_cat.get(cat, 0) + abs(t["amount"])

    return {
        "income": income,
        "expenses": expenses,
        "balance": latest_balance,
        "by_category": dict(sorted(by_cat.items(), key=lambda x: x[1], reverse=True)),
    }


@router.get("/", response_class=HTMLResponse)
async def bank_page(request: Request):
    txs = _load_transactions()
    summary = _summary(txs)
    return templates.TemplateResponse("bank.html", {
        "request": request,
        "page": "bank",
        "transactions": txs[:100],
        "total_count": len(txs),
        "summary": summary,
        "has_data": len(txs) > 0,
    })


@router.post("/upload", response_class=HTMLResponse)
async def upload_csv(request: Request, file: UploadFile = File(...)):
    content = (await file.read()).decode("utf-8-sig")  # utf-8-sig handles BOM
    txs = _parse_nu_csv(content)
    if not txs:
        return HTMLResponse("<div class='alert red'>No se pudieron leer transacciones del CSV. Verifica el formato.</div>")

    # Merge with existing (deduplicate by date+description+amount)
    existing = _load_transactions()
    existing_keys = {(t["date"], t["description"], t["amount"]) for t in existing}
    new_txs = [t for t in txs if (t["date"], t["description"], t["amount"]) not in existing_keys]
    merged = txs + [t for t in existing if (t["date"], t["description"], t["amount"]) not in {(n["date"], n["description"], n["amount"]) for n in txs}]
    merged.sort(key=lambda x: x["date"], reverse=True)
    _save_transactions(merged)

    summary = _summary(merged)
    return templates.TemplateResponse("bank.html", {
        "request": request,
        "page": "bank",
        "transactions": merged[:100],
        "total_count": len(merged),
        "summary": summary,
        "has_data": True,
        "upload_success": f"✓ {len(txs)} transacciones importadas ({len(new_txs)} nuevas)",
    })
