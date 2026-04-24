# export_metrics.py — repo root
# Runs after dbt build via GitHub Actions
# Queries Snowflake ONCE, writes docs/metrics.json
# GitHub Pages serves the static file — no live Snowflake hits on page load

import json
import os
from datetime import datetime, timezone
import snowflake.connector

def get_connection():
    return snowflake.connector.connect(
        account   = os.environ['SNOWFLAKE_ACCOUNT'],
        user      = os.environ['SNOWFLAKE_USER'],
        password  = os.environ['SNOWFLAKE_PASSWORD'],
        warehouse = 'COMPUTE_WH',
        database  = 'CUSTOMER_PLATFORM',
        schema    = 'DBT_SRIRAM_PROD',
        role      = 'ACCOUNTADMIN'
    )

def q(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()

def breakdown(cursor, sql):
    return [{"label": str(r[0]), "value": r[1]} for r in q(cursor, sql)]

def build_metrics():
    conn = get_connection()
    cur  = conn.cursor()
    m    = {}

    m['updated_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    # ── headline KPIs from DIM_CUSTOMERS ──────────────────────────────────
    m['total_customers']          = q(cur, "SELECT COUNT(*) FROM DIM_CUSTOMERS")[0][0]
    m['active_customers']         = q(cur, "SELECT COUNT(*) FROM DIM_CUSTOMERS WHERE IS_ACTIVE = TRUE")[0][0]
    m['churn_rate']               = q(cur, "SELECT ROUND(AVG(CASE WHEN IS_ACTIVE=FALSE THEN 1.0 ELSE 0 END),3) FROM DIM_CUSTOMERS")[0][0]

    # ── revenue from FCT_CUSTOMER_VALUE (pre-aggregated per customer) ──────
    m['total_revenue']            = q(cur, "SELECT ROUND(SUM(TOTAL_REVENUE)) FROM FCT_CUSTOMER_VALUE")[0][0]
    m['revenue_last_30d']         = q(cur, "SELECT ROUND(SUM(REVENUE_LAST_30D)) FROM FCT_CUSTOMER_VALUE")[0][0]
    m['revenue_last_90d']         = q(cur, "SELECT ROUND(SUM(REVENUE_LAST_90D)) FROM FCT_CUSTOMER_VALUE")[0][0]
    m['avg_order_value']          = q(cur, "SELECT ROUND(AVG(AVG_ORDER_VALUE)) FROM FCT_CUSTOMER_VALUE WHERE AVG_ORDER_VALUE > 0")[0][0]
    m['revenue_per_active_customer'] = q(cur, "SELECT ROUND(SUM(TOTAL_REVENUE)/NULLIF(COUNT(CASE WHEN IS_ACTIVE=TRUE THEN 1 END),0),1) FROM FCT_CUSTOMER_VALUE")[0][0]
    m['avg_customer_value_score'] = q(cur, "SELECT ROUND(AVG(CUSTOMER_VALUE_SCORE),1) FROM FCT_CUSTOMER_VALUE")[0][0]

    # ── risk metrics from FCT_CUSTOMER_VALUE ──────────────────────────────
    m['high_risk_customer_count'] = q(cur, "SELECT COUNT(*) FROM FCT_CUSTOMER_VALUE WHERE RISK_TIER = 'high'")[0][0]
    m['avg_churn_risk_score']     = q(cur, "SELECT ROUND(AVG(CHURN_RISK_SCORE),1) FROM FCT_CUSTOMER_VALUE")[0][0]
    m['avg_failed_txn_rate']      = q(cur, "SELECT ROUND(AVG(FAILED_TXN_RATE),3) FROM FCT_CUSTOMER_VALUE WHERE FAILED_TXN_RATE IS NOT NULL")[0][0]
    m['avg_chargeback_rate']      = q(cur, "SELECT ROUND(AVG(CHARGEBACK_RATE),3) FROM FCT_CUSTOMER_VALUE WHERE CHARGEBACK_RATE IS NOT NULL")[0][0]
    m['avg_cart_abandon_rate']    = q(cur, "SELECT ROUND(AVG(CART_ABANDON_RATE),3) FROM FCT_CUSTOMER_VALUE WHERE CART_ABANDON_RATE IS NOT NULL")[0][0]

    # ── engagement from FCT_CUSTOMER_VALUE ────────────────────────────────
    m['avg_email_open_rate']      = q(cur, "SELECT ROUND(AVG(EMAIL_OPEN_RATE),3) FROM FCT_CUSTOMER_VALUE WHERE EMAIL_OPEN_RATE IS NOT NULL")[0][0]
    m['avg_email_click_rate']     = q(cur, "SELECT ROUND(AVG(EMAIL_CLICK_RATE),3) FROM FCT_CUSTOMER_VALUE WHERE EMAIL_CLICK_RATE IS NOT NULL")[0][0]
    m['avg_personalization_score']= q(cur, "SELECT ROUND(AVG(PERSONALIZATION_SCORE),1) FROM FCT_CUSTOMER_VALUE WHERE PERSONALIZATION_SCORE IS NOT NULL")[0][0]
    m['avg_recency_days']         = q(cur, "SELECT ROUND(AVG(RECENCY_DAYS),1) FROM FCT_CUSTOMER_VALUE WHERE RECENCY_DAYS IS NOT NULL")[0][0]

    # ── breakdowns for bar charts ──────────────────────────────────────────

    # Customer tab
    m['total_customers_by_loyalty_tier'] = breakdown(cur,
        "SELECT LOYALTY_TIER, COUNT(*) FROM DIM_CUSTOMERS GROUP BY 1 ORDER BY 2 DESC")

    m['active_customers_by_age_band'] = breakdown(cur,
        "SELECT AGE_BAND, COUNT(*) FROM DIM_CUSTOMERS WHERE IS_ACTIVE=TRUE GROUP BY 1 ORDER BY 1")

    m['avg_recency_days_by_loyalty_tier'] = breakdown(cur,
        "SELECT LOYALTY_TIER, ROUND(AVG(RECENCY_DAYS),1) FROM FCT_CUSTOMER_VALUE GROUP BY 1 ORDER BY 2")

    # Revenue tab
    m['total_revenue_by_loyalty_tier'] = breakdown(cur,
        "SELECT LOYALTY_TIER, ROUND(SUM(TOTAL_REVENUE)) FROM FCT_CUSTOMER_VALUE GROUP BY 1 ORDER BY 2 DESC")

    m['revenue_last_30d_by_age_band'] = breakdown(cur,
        "SELECT AGE_BAND, ROUND(SUM(REVENUE_LAST_30D)) FROM FCT_CUSTOMER_VALUE GROUP BY 1 ORDER BY 1")

    m['avg_order_value_by_loyalty_tier'] = breakdown(cur,
        "SELECT LOYALTY_TIER, ROUND(AVG(AVG_ORDER_VALUE)) FROM FCT_CUSTOMER_VALUE WHERE AVG_ORDER_VALUE > 0 GROUP BY 1 ORDER BY 2 DESC")

    # Risk tab
    m['churn_rate_by_acquisition_channel'] = breakdown(cur,
        "SELECT ACQUISITION_CHANNEL, ROUND(AVG(CASE WHEN IS_ACTIVE=FALSE THEN 1.0 ELSE 0 END),3) FROM DIM_CUSTOMERS GROUP BY 1 ORDER BY 2 DESC")

    m['high_risk_by_loyalty_tier'] = breakdown(cur,
        "SELECT LOYALTY_TIER, COUNT(*) FROM FCT_CUSTOMER_VALUE WHERE RISK_TIER='high' GROUP BY 1 ORDER BY 2 DESC")

    m['avg_churn_risk_score_by_loyalty_tier'] = breakdown(cur,
        "SELECT LOYALTY_TIER, ROUND(AVG(CHURN_RISK_SCORE),1) FROM FCT_CUSTOMER_VALUE GROUP BY 1 ORDER BY 2 DESC")

    # Engagement tab
    m['avg_email_open_rate_by_loyalty_tier'] = breakdown(cur,
        "SELECT LOYALTY_TIER, ROUND(AVG(EMAIL_OPEN_RATE),3) FROM FCT_CUSTOMER_VALUE WHERE EMAIL_OPEN_RATE IS NOT NULL GROUP BY 1 ORDER BY 2 DESC")

    m['avg_email_click_rate_by_loyalty_tier'] = breakdown(cur,
        "SELECT LOYALTY_TIER, ROUND(AVG(EMAIL_CLICK_RATE),3) FROM FCT_CUSTOMER_VALUE WHERE EMAIL_CLICK_RATE IS NOT NULL GROUP BY 1 ORDER BY 2 DESC")

    m['avg_personalization_score_by_age_band'] = breakdown(cur,
        "SELECT AGE_BAND, ROUND(AVG(PERSONALIZATION_SCORE),1) FROM FCT_CUSTOMER_VALUE WHERE PERSONALIZATION_SCORE IS NOT NULL GROUP BY 1 ORDER BY 1")

    cur.close()
    conn.close()
    return m

if __name__ == '__main__':
    metrics = build_metrics()
    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'docs', 'metrics.json')
    with open(output_path, 'w') as f:
        json.dump(metrics, f, indent=2)
    print(f"metrics.json written — {metrics['updated_at']}")
    print(f"total_customers        : {metrics['total_customers']}")
    print(f"active_customers       : {metrics['active_customers']}")
    print(f"churn_rate             : {metrics['churn_rate']}")
    print(f"total_revenue          : {metrics['total_revenue']}")
    print(f"high_risk_customers    : {metrics['high_risk_customer_count']}")
    print(f"avg_order_value        : {metrics['avg_order_value']}")
