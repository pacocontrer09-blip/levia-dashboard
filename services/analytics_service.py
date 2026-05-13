"""
Pure computation functions — no async, no API calls.
Receives already-fetched data from shopify_service/meta_service and derives analytics.
"""
from collections import defaultdict
from datetime import datetime


def compute_day_of_week(orders: list) -> dict:
    """Revenue and order count grouped by day of week (Mon=0 … Sun=6)."""
    day_names = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"]
    revenue_by_day = defaultdict(float)
    orders_by_day = defaultdict(int)
    week_count_by_day = defaultdict(set)  # track which weeks had data per weekday

    for o in orders:
        date_str = o.get("created_at", o.get("date", ""))[:10]
        if not date_str:
            continue
        try:
            dt = datetime.fromisoformat(date_str)
        except ValueError:
            continue
        dow = dt.weekday()  # 0=Mon
        price = float(o.get("total_price", o.get("revenue", 0)))
        revenue_by_day[dow] += price
        orders_by_day[dow] += 1
        week_count_by_day[dow].add(dt.isocalendar()[:2])  # (year, week)

    labels, revenue, order_counts, avg_revenue, avg_orders = [], [], [], [], []
    for i in range(7):
        labels.append(day_names[i])
        rev = round(revenue_by_day[i], 2)
        cnt = orders_by_day[i]
        weeks = max(len(week_count_by_day[i]), 1)
        revenue.append(rev)
        order_counts.append(cnt)
        avg_revenue.append(round(rev / weeks, 2))
        avg_orders.append(round(cnt / weeks, 1))

    best_day_idx = avg_revenue.index(max(avg_revenue)) if any(avg_revenue) else 0
    return {
        "labels": labels,
        "revenue": revenue,
        "orders": order_counts,
        "avg_revenue": avg_revenue,
        "avg_orders": avg_orders,
        "best_day": day_names[best_day_idx],
        "best_day_revenue": avg_revenue[best_day_idx],
    }


def compute_forecasting(orders: list, days_ahead: int = 30) -> dict:
    """Simple linear regression on daily revenue to project next N days."""
    if not orders:
        return {
            "forecast_revenue": 0, "forecast_orders": 0,
            "trend": "flat", "confidence": "low",
            "daily_avg": 0, "daily_avg_trend": 0,
            "hist_labels": [], "hist_revenue": [],
            "proj_labels": [], "proj_revenue": [],
        }

    # Aggregate by date
    daily: dict = defaultdict(lambda: {"revenue": 0.0, "orders": 0})
    for o in orders:
        date_str = o.get("created_at", o.get("date", ""))[:10]
        price = float(o.get("total_price", o.get("revenue", 0)))
        if date_str:
            daily[date_str]["revenue"] += price
            daily[date_str]["orders"] += 1

    sorted_dates = sorted(daily.keys())
    if len(sorted_dates) < 5:
        total_rev = sum(d["revenue"] for d in daily.values())
        daily_avg = total_rev / max(len(sorted_dates), 1)
        return {
            "forecast_revenue": round(daily_avg * days_ahead, 0),
            "forecast_orders": len(orders),
            "trend": "flat", "confidence": "low",
            "daily_avg": round(daily_avg, 0), "daily_avg_trend": 0,
            "hist_labels": [d[5:] for d in sorted_dates],
            "hist_revenue": [round(daily[d]["revenue"], 0) for d in sorted_dates],
            "proj_labels": [], "proj_revenue": [],
        }

    # Use last 60 days max for regression
    sorted_dates = sorted_dates[-60:]
    n = len(sorted_dates)
    x_vals = list(range(n))
    y_vals = [daily[d]["revenue"] for d in sorted_dates]

    # Linear regression: y = m*x + b
    x_mean = sum(x_vals) / n
    y_mean = sum(y_vals) / n
    numerator = sum((x_vals[i] - x_mean) * (y_vals[i] - y_mean) for i in range(n))
    denominator = sum((x_vals[i] - x_mean) ** 2 for i in range(n))
    slope = numerator / denominator if denominator != 0 else 0
    intercept = y_mean - slope * x_mean

    # Project forward
    proj_labels, proj_revenue = [], []
    last_date = datetime.fromisoformat(sorted_dates[-1])
    for i in range(1, days_ahead + 1):
        from datetime import timedelta
        proj_date = last_date + timedelta(days=i)
        proj_labels.append(proj_date.strftime("%m-%d"))
        proj_y = max(0, intercept + slope * (n - 1 + i))
        proj_revenue.append(round(proj_y, 0))

    forecast_revenue = round(sum(proj_revenue), 0)
    forecast_orders = round(len(orders) / n * days_ahead, 0)

    # Trend classification
    slope_pct = slope / y_mean * 100 if y_mean > 0 else 0
    if slope_pct > 5:
        trend = "up_strong"
    elif slope_pct > 1:
        trend = "up"
    elif slope_pct < -5:
        trend = "down_strong"
    elif slope_pct < -1:
        trend = "down"
    else:
        trend = "flat"

    confidence = "high" if n >= 30 else ("medium" if n >= 14 else "low")

    return {
        "forecast_revenue": forecast_revenue,
        "forecast_orders": int(forecast_orders),
        "trend": trend,
        "confidence": confidence,
        "daily_avg": round(y_mean, 0),
        "daily_avg_trend": round(slope, 0),
        "hist_labels": [d[5:] for d in sorted_dates[-30:]],
        "hist_revenue": [round(daily[d]["revenue"], 0) for d in sorted_dates[-30:]],
        "proj_labels": proj_labels[:30],
        "proj_revenue": proj_revenue[:30],
    }


def compute_cohort_retention(customers: list) -> dict:
    """Group customers by acquisition month and compute retention (repeat purchasers)."""
    cohorts: dict = defaultdict(lambda: {"acquired": 0, "returned": 0})

    for c in customers:
        created = c.get("created_at", "")[:7]  # YYYY-MM
        if not created:
            continue
        cohorts[created]["acquired"] += 1
        if int(c.get("orders_count", 0)) > 1:
            cohorts[created]["returned"] += 1

    rows = []
    for month in sorted(cohorts.keys())[-12:]:  # last 12 months
        data = cohorts[month]
        acquired = data["acquired"]
        returned = data["returned"]
        retention = round(returned / acquired * 100, 1) if acquired > 0 else 0
        rows.append({
            "month": month,
            "acquired": acquired,
            "returned": returned,
            "retention_pct": retention,
            "color": "green" if retention >= 20 else ("yellow" if retention >= 10 else "red"),
        })

    overall_acquired = sum(r["acquired"] for r in rows)
    overall_returned = sum(r["returned"] for r in rows)
    overall_retention = round(overall_returned / overall_acquired * 100, 1) if overall_acquired > 0 else 0

    return {
        "cohorts": rows,
        "overall_retention_pct": overall_retention,
        "overall_acquired": overall_acquired,
        "overall_returned": overall_returned,
    }


def compute_ltv_cac_metrics(customers: list, total_ad_spend: float) -> dict:
    """LTV:CAC ratio, payback period, and related strategic metrics."""
    if not customers:
        return {"ltv": 0, "cac": 0, "ltv_cac_ratio": 0, "payback_days": 0, "ratio_color": "red", "payback_color": "red"}

    total_ltv = sum(float(c.get("total_spent", 0)) for c in customers)
    count = len(customers)
    avg_ltv = round(total_ltv / count, 2)

    new_customers = sum(1 for c in customers if int(c.get("orders_count", 0)) == 1)
    cac = round(total_ad_spend / new_customers, 2) if new_customers > 0 and total_ad_spend > 0 else 0

    ltv_cac_ratio = round(avg_ltv / cac, 2) if cac > 0 else 0

    # Payback period: how many days to recover CAC at current daily revenue pace
    daily_revenue_per_customer = avg_ltv / 365 if avg_ltv > 0 else 0
    payback_days = int(cac / daily_revenue_per_customer) if daily_revenue_per_customer > 0 else 0

    ratio_color = "green" if ltv_cac_ratio >= 3 else ("yellow" if ltv_cac_ratio >= 1.5 else "red")
    payback_color = "green" if 0 < payback_days <= 60 else ("yellow" if payback_days <= 120 else "red")

    return {
        "ltv": avg_ltv,
        "cac": cac,
        "ltv_cac_ratio": ltv_cac_ratio,
        "payback_days": payback_days,
        "new_customers": new_customers,
        "ratio_color": ratio_color,
        "payback_color": payback_color,
        "ratio_label": "Excelente" if ltv_cac_ratio >= 3 else ("Aceptable" if ltv_cac_ratio >= 1.5 else "Crítico"),
    }


def compute_geographic_breakdown(orders: list) -> dict:
    """Revenue and order count grouped by Mexican state from shipping_address.province."""
    state_map: dict = defaultdict(lambda: {"orders": 0, "revenue": 0.0})

    for o in orders:
        shipping = o.get("shipping_address") or {}
        province = shipping.get("province") or shipping.get("province_code") or "Desconocido"
        price = float(o.get("total_price", 0))
        state_map[province]["orders"] += 1
        state_map[province]["revenue"] += price

    total_revenue = sum(v["revenue"] for v in state_map.values())
    total_orders = sum(v["orders"] for v in state_map.values())

    states = sorted(
        [
            {
                "name": state,
                "orders": data["orders"],
                "revenue": round(data["revenue"], 2),
                "pct_revenue": round(data["revenue"] / total_revenue * 100, 1) if total_revenue > 0 else 0,
                "pct_orders": round(data["orders"] / total_orders * 100, 1) if total_orders > 0 else 0,
            }
            for state, data in state_map.items()
        ],
        key=lambda x: x["revenue"],
        reverse=True,
    )

    top_state = states[0]["name"] if states else "—"
    return {
        "states": states,
        "top_state": top_state,
        "total_states": len(states),
        "total_revenue": round(total_revenue, 2),
        "total_orders": total_orders,
    }


def compute_rfm_segments(customers: list) -> dict:
    """RFM segmentation: Champion / Leal / En Riesgo / Dormido / Nuevo."""
    from datetime import date as date_cls
    today = date_cls.today()

    segments: dict = {"champion": [], "leal": [], "en_riesgo": [], "dormido": [], "nuevo": []}

    for c in customers:
        last_date = c.get("last_order_date", "")
        orders = int(c.get("orders_count", 0))
        spent = float(c.get("total_spent", 0))

        if last_date:
            try:
                days_ago = (today - date_cls.fromisoformat(last_date[:10])).days
            except ValueError:
                days_ago = 999
        else:
            days_ago = 999

        r = 1 if days_ago < 30 else (2 if days_ago < 60 else 3)
        f = 1 if orders > 3 else (2 if orders > 1 else 3)
        m = 1 if spent > 5000 else (2 if spent > 2000 else 3)

        row = {**c, "days_since_order": days_ago}

        if r == 1 and f == 1:
            segments["champion"].append(row)
        elif r == 1 and f == 3:
            segments["nuevo"].append(row)
        elif r <= 2 and f <= 2:
            segments["leal"].append(row)
        elif r >= 2 and f <= 2:
            segments["en_riesgo"].append(row)
        elif r == 3 and f == 3:
            segments["dormido"].append(row)
        else:
            segments["leal"].append(row)

    total = len(customers)
    dormidos = len(segments["dormido"])
    churn_rate = round(dormidos / total * 100, 1) if total > 0 else 0

    at_risk = sorted(segments["en_riesgo"], key=lambda x: x["days_since_order"], reverse=True)[:10]

    return {
        "segments": segments,
        "counts": {k: len(v) for k, v in segments.items()},
        "customers_at_risk": at_risk,
        "churn_rate": churn_rate,
    }
