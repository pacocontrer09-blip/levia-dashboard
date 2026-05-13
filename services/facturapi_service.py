import os
import json
import httpx
from datetime import date, datetime
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
CFDI_LOG = BASE_DIR / "cache" / "cfdi_log.json"

FACTURAPI_BASE = "https://www.facturapi.io/v2"
SANDBOX_BASE = "https://www.facturapi.io/v2"

# XAXX010101000 = RFC público en general (ventas sin datos del cliente)
RFC_PUBLICO_EN_GENERAL = "XAXX010101000"
# RESICO persona física = clave SAT 612
REGIMEN_RESICO = "612"


def _api_key() -> str:
    return os.getenv("FACTURAPI_API_KEY", "")


def _is_live() -> bool:
    return os.getenv("FACTURAPI_LIVE", "false").lower() == "true"


def _headers() -> dict:
    key = _api_key()
    if not key:
        return {}
    return {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}


# ---------------------------------------------------------------------------
# CFDI log (local JSON)
# ---------------------------------------------------------------------------

def _load_log() -> list:
    if CFDI_LOG.exists():
        try:
            return json.loads(CFDI_LOG.read_text())
        except Exception:
            return []
    return []


def _append_log(entry: dict):
    log = _load_log()
    log.append(entry)
    CFDI_LOG.write_text(json.dumps(log, default=str, indent=2))


# ---------------------------------------------------------------------------
# Crear CFDI global diaria
# ---------------------------------------------------------------------------

async def create_global_invoice(invoice_date: date, orders: list) -> dict:
    """
    Construye y timbra un CFDI global con todas las ventas del día.
    `orders` es la lista de órdenes pagadas de Shopify para ese día.
    Retorna el objeto de Facturapi o un dict con error.
    """
    key = _api_key()
    if not key:
        return {"ok": False, "error": "FACTURAPI_API_KEY no configurada"}

    if not orders:
        return {"ok": False, "error": "Sin ventas ese día — no se genera CFDI"}

    # Agrupar totales
    subtotal = sum(float(o.get("subtotal_price", 0)) for o in orders)
    total_tax = sum(float(o.get("total_tax", 0)) for o in orders)
    total_mxn = sum(float(o.get("total_price", 0)) for o in orders)

    rfc = os.getenv("SAT_RFC", "")
    razon_social = os.getenv("SAT_RAZON_SOCIAL", "PERSONA FISICA LEVIA")

    payload = {
        "type": "I",  # Ingreso
        "customer": {
            "legal_name": "PUBLICO EN GENERAL",
            "tax_id": RFC_PUBLICO_EN_GENERAL,
            "tax_system": "616",  # Sin obligaciones fiscales
            "zip": os.getenv("SAT_CP", "01000"),
        },
        "global": {
            "periodicity": "01",  # Diaria
            "months": str(invoice_date.month).zfill(2),
            "year": invoice_date.year,
        },
        "date": invoice_date.isoformat() + "T12:00:00",
        "items": [
            {
                "quantity": 1,
                "product": {
                    "description": f"Ventas del día {invoice_date.strftime('%d/%m/%Y')} — LEVIA™",
                    "product_key": "43211500",  # Almohadas y cojines (SAT)
                    "unit_key": "E48",  # Unidad de servicio
                    "price": subtotal,
                    "taxes": [{"type": "IVA", "rate": 0.16}],
                },
            }
        ],
        "payment_form": "28",  # Tarjeta de débito/crédito
        "use": "G03",  # Gastos en general
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{FACTURAPI_BASE}/invoices",
                headers=_headers(),
                json=payload,
            )
        data = resp.json()
        if resp.status_code in (200, 201):
            entry = {
                "id": data.get("id"),
                "date": invoice_date.isoformat(),
                "total": total_mxn,
                "subtotal": subtotal,
                "tax": total_tax,
                "orders_count": len(orders),
                "status": data.get("status", "valid"),
                "folio": data.get("folio_number"),
                "created_at": datetime.now().isoformat(),
                "live": _is_live(),
            }
            _append_log(entry)
            return {"ok": True, "invoice": entry, "raw": data}
        else:
            return {"ok": False, "error": data.get("message", str(data)), "status_code": resp.status_code}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ---------------------------------------------------------------------------
# Listar CFDIs del log local
# ---------------------------------------------------------------------------

def list_invoices(month: int | None = None, year: int | None = None) -> list:
    log = _load_log()
    if month and year:
        log = [e for e in log if e.get("date", "").startswith(f"{year}-{str(month).zfill(2)}")]
    return sorted(log, key=lambda x: x.get("date", ""), reverse=True)


# ---------------------------------------------------------------------------
# Descargar PDF de un CFDI (por ID de Facturapi)
# ---------------------------------------------------------------------------

async def get_invoice_pdf_url(invoice_id: str) -> str:
    """Retorna la URL del PDF en Facturapi (requiere key de API)."""
    return f"{FACTURAPI_BASE}/invoices/{invoice_id}/pdf"


# ---------------------------------------------------------------------------
# Cancelar CFDI
# ---------------------------------------------------------------------------

async def cancel_invoice(invoice_id: str, motive: str = "02") -> dict:
    """
    motive: 01=comprobante emitido con errores con relación, 02=comprobante emitido
    con errores sin relación, 03=no se llevó a cabo la operación, 04=operación nominativa
    relacionada en factura global.
    """
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.delete(
                f"{FACTURAPI_BASE}/invoices/{invoice_id}",
                headers=_headers(),
                params={"motive": motive},
            )
        return {"ok": resp.status_code in (200, 204), "status_code": resp.status_code}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ---------------------------------------------------------------------------
# KPIs fiscales del mes (calculados localmente desde el log)
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# IVA acreditable automático desde transacciones Nu
# ---------------------------------------------------------------------------

# Categorías del banco que sí causan IVA (proveedores mexicanos/internacionales con IVA)
IVA_CATEGORIES = {"Meta", "TikTok", "Shopify", "Envíos", "Servicios"}
# Alibaba/AliExpress son extranjeros → sin IVA acreditable
NO_IVA_CATEGORIES = {"Proveedor", "Retiro", "Cobro LEVIA", "Otro"}

IVA_RATE = 0.16


def get_iva_acreditable_from_bank(month: str | None = None) -> dict:
    """
    Calcula IVA acreditable desde las transacciones del banco Nu.
    `month` formato 'YYYY-MM'. Si None, usa el mes actual.
    """
    from pathlib import Path
    import json as _json

    bank_path = BASE_DIR / "cache" / "nu_transactions.json"
    if not bank_path.exists():
        return {"iva_acreditable": 0, "gastos_con_iva": 0, "detalle": {}, "sin_datos_banco": True}

    try:
        txs = _json.loads(bank_path.read_text())
    except Exception:
        return {"iva_acreditable": 0, "gastos_con_iva": 0, "detalle": {}, "sin_datos_banco": True}

    if not month:
        month = datetime.now().strftime("%Y-%m")

    # Solo gastos (amount < 0) del mes objetivo, con IVA
    month_txs = [
        t for t in txs
        if t.get("date", "").startswith(month)
        and t.get("amount", 0) < 0
        and t.get("category") in IVA_CATEGORIES
    ]

    detalle: dict[str, float] = {}
    for t in month_txs:
        cat = t.get("category", "Otro")
        detalle[cat] = detalle.get(cat, 0) + abs(t.get("amount", 0))

    gastos_con_iva = sum(detalle.values())
    # IVA acreditable = gasto_base × 16% (asumiendo precio ya incluye IVA → base = gasto/1.16 × 0.16)
    iva_acreditable = round(gastos_con_iva / 1.16 * IVA_RATE, 2)

    return {
        "iva_acreditable": iva_acreditable,
        "gastos_con_iva": round(gastos_con_iva, 2),
        "detalle": {k: round(v, 2) for k, v in sorted(detalle.items(), key=lambda x: x[1], reverse=True)},
        "sin_datos_banco": False,
        "mes": month,
        "txs_count": len(month_txs),
    }


RESICO_RATES = [
    (84_570,    0.0190),
    (168_636,   0.0380),
    (253_426,   0.0570),
    (337_995,   0.0850),
    (527_099,   0.1023),
    (842_560,   0.1290),
    (1_315_873, 0.1600),
    (2_007_347, 0.1792),
    (float("inf"), 0.1984),
]

LIMITE_RESICO = 3_500_000.0


def calcular_isr_resico(ingresos: float) -> float:
    for limite, tasa in RESICO_RATES:
        if ingresos <= limite:
            return round(ingresos * tasa, 2)
    return round(ingresos * 0.1984, 2)


def get_fiscal_kpis(year: int) -> dict:
    """KPIs fiscales acumulados del año."""
    log = _load_log()
    year_entries = [e for e in log if e.get("date", "").startswith(str(year))]

    ingresos_año = sum(e.get("total", 0) for e in year_entries)
    isr_estimado = calcular_isr_resico(ingresos_año)
    pct_limite = round((ingresos_año / LIMITE_RESICO) * 100, 1)

    # Mes actual
    mes_actual = datetime.now().month
    mes_entries = [e for e in year_entries if e.get("date", "").startswith(f"{year}-{str(mes_actual).zfill(2)}")]
    ingresos_mes = sum(e.get("total", 0) for e in mes_entries)
    isr_mes = calcular_isr_resico(ingresos_mes)
    iva_mes = round(ingresos_mes * 0.16 / 1.16, 2)  # IVA implícito si precio incluye IVA

    # Próxima declaración (día 17 del mes siguiente)
    now = datetime.now()
    if now.month == 12:
        next_decl = datetime(now.year + 1, 1, 17)
    else:
        next_decl = datetime(now.year, now.month + 1, 17)
    dias_para_decl = (next_decl.date() - now.date()).days

    return {
        "ingresos_año": round(ingresos_año, 2),
        "isr_estimado_año": isr_estimado,
        "pct_limite_resico": pct_limite,
        "ingresos_mes": round(ingresos_mes, 2),
        "isr_mes": isr_mes,
        "iva_mes": iva_mes,
        "cfdi_mes": len(mes_entries),
        "cfdi_año": len(year_entries),
        "proxima_decl": next_decl.strftime("%d/%m/%Y"),
        "dias_para_decl": dias_para_decl,
        "alerta_decl": dias_para_decl <= 5,
        "alerta_limite": pct_limite >= 80,
        "api_configurada": bool(_api_key()),
        "modo_live": _is_live(),
    }
