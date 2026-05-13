import asyncio
from datetime import datetime
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from services.sheets_service import get_dashboard_kpis
from services.shopify_service import get_orders_month, get_orders_historical, get_orders_with_refunds, get_customers_detail
from services.analytics_service import compute_ltv_cac_metrics
from services.meta_service import get_campaigns
from services.facturapi_service import calcular_isr_resico, get_iva_acreditable_from_bank

templates = Jinja2Templates(directory=str(Path(__file__).parent.parent / "templates"))
router = APIRouter()

COGS_USD = 26.0
FX_DEFAULT = 17.3
UNIT_PRICE_AVG = 1299
SHOPIFY_TX_FEE_PCT = 0.02       # 2% Shopify transaction fee (no Shopify Payments MX)
PAYMENT_PROCESSING_PCT = 0.025  # 2.5% Conekta/PayPal average
PAYMENT_PROCESSING_FIXED = 10   # MXN per order


@router.get("/", response_class=HTMLResponse)
async def finance_page(request: Request, month: str | None = None):
    if not month:
        month = datetime.now().strftime("%Y-%m")

    try:
        kpis, shopify_month, shopify_hist, refunds_data, customers_data, meta_data = await asyncio.gather(
            get_dashboard_kpis(month),
            get_orders_month(month),
            get_orders_historical(days=90),
            get_orders_with_refunds(days=90),
            get_customers_detail(limit=100),
            get_campaigns(),
        )
    except Exception:
        kpis = {"source": "error"}
        shopify_month = {}
        shopify_hist = {"orders": [], "revenue_mxn": 0, "count": 0, "period": "Últimos 90 días"}
        refunds_data = {"total_refunded": 0, "refund_rate_pct": 0, "revenue_net": 0, "unfulfilled_count": 0}
        customers_data = {"customers": [], "total_count": 0}
        meta_data = {"campaigns": []}

    # Revenue: Sheets first, then Shopify
    revenue = kpis.get("revenue_mxn") or shopify_month.get("revenue_mxn", 0)
    units = kpis.get("units") or shopify_month.get("units", 0)
    orders = kpis.get("orders") or shopify_month.get("count", 0)

    total_refunded = refunds_data.get("total_refunded", 0)
    refund_rate_pct = refunds_data.get("refund_rate_pct", 0)
    revenue_net = revenue - total_refunded

    cogs = kpis.get("cogs_mxn") or (units * COGS_USD * FX_DEFAULT)
    ad_spend = kpis.get("ad_spend_mxn", 0)
    gross_profit = revenue_net - cogs
    gross_margin = (gross_profit / revenue_net * 100) if revenue_net > 0 else 0
    net_profit = gross_profit - ad_spend
    net_margin = (net_profit / revenue_net * 100) if revenue_net > 0 else 0
    roas = kpis.get("roas") or ((revenue / ad_spend) if ad_spend > 0 else 0)
    cac = kpis.get("cac_mxn") or ((ad_spend / orders) if orders > 0 else 0)

    # True Contribution Margin (Gross Profit minus transaction/payment fees)
    shopify_fees = revenue * SHOPIFY_TX_FEE_PCT
    payment_fees = revenue * PAYMENT_PROCESSING_PCT + orders * PAYMENT_PROCESSING_FIXED
    true_contribution = gross_profit - shopify_fees - payment_fees
    true_contribution_margin_pct = (true_contribution / revenue * 100) if revenue > 0 else 0

    # SAT / RESICO — impacto fiscal real
    isr_provisional = calcular_isr_resico(revenue_net)
    # IVA sobre ventas (precio incluye IVA → IVA = precio / 1.16 * 0.16)
    iva_ventas = round(revenue_net / 1.16 * 0.16, 2)
    iva_data = get_iva_acreditable_from_bank(month)
    iva_acreditable = iva_data.get("iva_acreditable", 0)
    iva_neto = max(0, iva_ventas - iva_acreditable)
    total_sat = isr_provisional + iva_neto
    real_cash_after_tax = true_contribution - total_sat
    real_cash_margin_pct = (real_cash_after_tax / revenue_net * 100) if revenue_net > 0 else 0

    roas_color = "green" if roas >= 1.2 else ("yellow" if roas >= 0.8 else "red")
    cac_color = "green" if cac <= 180 else ("yellow" if cac <= 250 else "red")
    margin_color = "green" if net_margin >= 20 else ("yellow" if net_margin >= 10 else "red")

    unit_profit = UNIT_PRICE_AVG - COGS_USD * FX_DEFAULT
    breakeven_units = int(ad_spend / unit_profit) if unit_profit > 0 and ad_spend > 0 else 0

    shopify_orders = shopify_hist.get("orders", [])
    hist_revenue = shopify_hist.get("revenue_mxn", 0)
    hist_count = shopify_hist.get("count", 0)

    # LTV:CAC metrics
    live_campaigns = meta_data.get("campaigns", [])
    total_ad_spend = sum(c.get("spend_mxn", 0) for c in live_campaigns)
    customers = customers_data.get("customers", [])
    ltv_cac = compute_ltv_cac_metrics(customers, total_ad_spend)

    # MER vs ROAS Meta gap (A4)
    total_meta_attributed_revenue = sum(c.get("revenue_mxn", 0) for c in live_campaigns)
    roas_meta_attributed = round(total_meta_attributed_revenue / total_ad_spend, 2) if total_ad_spend > 0 else 0
    mer_finance = round(revenue / ad_spend, 2) if ad_spend > 0 else 0
    attribution_gap_pct = round((roas_meta_attributed - mer_finance) / roas_meta_attributed * 100, 1) if roas_meta_attributed > 0 else 0
    attribution_gap_color = "red" if attribution_gap_pct > 30 else ("yellow" if attribution_gap_pct > 15 else "green")

    # MoM trend — last 6 months
    mom_months = _last_n_months(6)
    mom_data = await asyncio.gather(*[get_orders_month(m) for m in mom_months])
    mom_labels = [m[5:] for m in mom_months]  # MM
    mom_revenue = [round(d.get("revenue_mxn", 0), 0) for d in mom_data]
    # Estimate net profit per month (simplified: revenue - COGS*units - assumed ad spend)
    mom_profit = []
    for d in mom_data:
        rev = d.get("revenue_mxn", 0)
        u = d.get("units", 0)
        cogs_est = u * COGS_USD * FX_DEFAULT
        profit_est = max(0, rev - cogs_est)
        mom_profit.append(round(profit_est, 0))

    data_source = kpis.get("source", "—")
    source_note = kpis.get("note", "")

    return templates.TemplateResponse("finance.html", {
        "request": request,
        "page": "finance",
        "month": month,
        "revenue_mxn": revenue,
        "units": int(units),
        "orders": int(orders),
        "aov_mxn": (revenue / orders) if orders > 0 else 0,
        "cogs_mxn": cogs,
        "gross_profit_mxn": gross_profit,
        "gross_margin_pct": gross_margin,
        "ad_spend_mxn": ad_spend,
        "net_profit_mxn": net_profit,
        "net_margin_pct": net_margin,
        "roas": roas,
        "roas_color": roas_color,
        "cac_mxn": cac,
        "cac_color": cac_color,
        "margin_color": margin_color,
        "breakeven_units": breakeven_units,
        "data_source": data_source,
        "source_note": source_note,
        "total_refunded": total_refunded,
        "refund_rate_pct": refund_rate_pct,
        "revenue_net": revenue_net,
        "shopify_orders": shopify_orders,
        "hist_revenue": hist_revenue,
        "hist_count": hist_count,
        "hist_period": shopify_hist.get("period", "Últimos 90 días"),
        "fetched_at": datetime.now().strftime("%H:%M"),
        # MER vs ROAS gap (A4)
        "mer_finance": mer_finance,
        "roas_meta_attributed": roas_meta_attributed,
        "attribution_gap_pct": attribution_gap_pct,
        "attribution_gap_color": attribution_gap_color,
        # LTV:CAC
        "ltv": ltv_cac.get("ltv", 0),
        "cac_ltv": ltv_cac.get("cac", 0),
        "ltv_cac_ratio": ltv_cac.get("ltv_cac_ratio", 0),
        "payback_days": ltv_cac.get("payback_days", 0),
        "ratio_color": ltv_cac.get("ratio_color", "red"),
        "payback_color": ltv_cac.get("payback_color", "red"),
        "ratio_label": ltv_cac.get("ratio_label", "—"),
        # True Contribution Margin
        "shopify_fees": shopify_fees,
        "payment_fees": payment_fees,
        "true_contribution": true_contribution,
        "true_contribution_margin_pct": true_contribution_margin_pct,
        # SAT / RESICO
        "isr_provisional": isr_provisional,
        "iva_ventas": iva_ventas,
        "iva_acreditable": iva_acreditable,
        "iva_neto": iva_neto,
        "total_sat": total_sat,
        "real_cash_after_tax": real_cash_after_tax,
        "real_cash_margin_pct": real_cash_margin_pct,
        "iva_sin_datos_banco": iva_data.get("sin_datos_banco", True),
        # MoM chart
        "mom_labels": mom_labels,
        "mom_revenue": mom_revenue,
        "mom_profit": mom_profit,
    })


def _last_n_months(n: int) -> list[str]:
    from datetime import date
    result = []
    now = date.today()
    year, month = now.year, now.month
    for _ in range(n):
        result.append(f"{year}-{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return list(reversed(result))
