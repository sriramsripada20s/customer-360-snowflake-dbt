# export_metrics.py
# Runs after dbt build in GitHub Actions
# Queries Snowflake once and writes docs/metrics.json
# GitHub Pages serves the static file — no live Snowflake hits

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

def build_metrics():
    conn = get_connection()
    cur  = conn.cursor()
    m    = {}

    m['updated_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    # --- headline KPIs ---
    m['total_customers']         = q(cur, "SELECT COUNT(*) FROM DIM_CUSTOMERS")[0][0]
    m['active_customers']        = q(cur, "SELECT COUNT(*) FROM DIM_CUSTOMERS WHERE IS_ACTIVE = TRUE")[0][0]
    m['total_revenue']           = q(cur, "SELECT ROUND(SUM(ORDER_AMOUNT)) FROM FCT_ORDERS")[0][0]
    m['avg_order_value']         = q(cur, "SELECT ROUND(AVG(ORDER_AMOUNT)) FROM FCT_ORDERS")[0][0]
    m['high_risk_customer_count']= q(cur, "SELECT COUNT(*) FROM DIM_CUSTOMERS WHERE CHURN_RISK_SCORE > 7")[0][0]
    m['avg_recency_days']        = q(cur, "SELECT ROUND(AVG(RECENCY_DAYS),1) FROM DIM_CUSTOMERS")[0][0]
    m['revenue_last_30d']        = q(cur, "SELECT ROUND(SUM(ORDER_AMOUNT)) FROM FCT_ORDERS WHERE ORDER_DATE >= DATEADD('day',-30,CURRENT_DATE)")[0][0]
    m['revenue_last_90d']        = q(cur, "SELECT ROUND(SUM(ORDER_AMOUNT)) FROM FCT_ORDERS WHERE ORDER_DATE >= DATEADD('day',-90,CURRENT_DATE)")[0][0]
    m['avg_customer_value_score']= q(cur, "SELECT ROUND(AVG(CUSTOMER_VALUE_SCORE),1) FROM FCT_CUSTOMER_VALUE")[0][0]
    m['avg_churn_risk_score']    = q(cur, "SELECT ROUND(AVG(CHURN_RISK_SCORE),1) FROM DIM_CUSTOMERS")[0][0]
    m['avg_failed_txn_rate']     = q(cur, "SELECT ROUND(AVG(CASE WHEN STATUS='failed' THEN 1.0 ELSE 0 END),3) FROM FCT_TRANSACTIONS")[0][0]
    m['avg_chargeback_rate']     = q(cur, "SELECT ROUND(AVG(CASE WHEN IS_CHARGEBACK=TRUE THEN 1.0 ELSE 0 END),3) FROM FCT_TRANSACTIONS")[0][0]
    m['avg_cart_abandon_rate']   = q(cur, "SELECT ROUND(AVG(CART_ABANDON_RATE),3) FROM FCT_CUSTOMER_VALUE")[0][0]
    m['avg_email_open_rate']     = q(cur, "SELECT ROUND(AVG(EMAIL_OPEN_RATE),3) FROM FCT_CAMPAIGNS")[0][0]
    m['avg_email_click_rate']    = q(cur, "SELECT ROUND(AVG(EMAIL_CLICK_RATE),3) FROM FCT_CAMPAIGNS")[0][0]
    m['avg_personalization_score']= q(cur,"SELECT ROUND(AVG(PERSONALIZATION_SCORE),1) FROM FEAT_PERSONALIZATION")[0][0]

    if m['total_customers'] and m['active_customers']:
        m['revenue_per_active_customer'] = round(m['total_revenue'] / m['active_customers'], 1) if m['active_customers'] else 0

    churn_rows = q(cur, "SELECT COUNT(*) churned, COUNT(*) total FROM DIM_CUSTOMERS")
    m['churn_rate'] = q(cur, "SELECT ROUND(SUM(CASE WHEN IS_CHURNED=TRUE THEN 1.0 ELSE 0 END)/COUNT(*),3) FROM DIM_CUSTOMERS")[0][0]

    # --- breakdowns for bar charts ---
    def breakdown(sql, label_col=0, val_col=1):
        return [{"label": str(r[label_col]), "value": r[val_col]} for r in q(cur, sql)]

    m['total_customers_by_loyalty_tier'] = breakdown(
        "SELECT LOYALTY_TIER, COUNT(*) FROM DIM_CUSTOMERS GROUP BY 1 ORDER BY 2 DESC")

    m['active_customers_by_age_band'] = breakdown(
        "SELECT AGE_BAND, COUNT(*) FROM DIM_CUSTOMERS WHERE IS_ACTIVE=TRUE GROUP BY 1 ORDER BY 1")

    m['avg_recency_days_by_loyalty_tier'] = breakdown(
        "SELECT LOYALTY_TIER, ROUND(AVG(RECENCY_DAYS),1) FROM DIM_CUSTOMERS GROUP BY 1 ORDER BY 2")

    m['total_revenue_by_loyalty_tier'] = breakdown(
        "SELECT D.LOYALTY_TIER, ROUND(SUM(O.ORDER_AMOUNT)) FROM FCT_ORDERS O JOIN DIM_CUSTOMERS D ON O.CUSTOMER_ID=D.CUSTOMER_ID GROUP BY 1 ORDER BY 2 DESC")

    m['revenue_last_30d_by_age_band'] = breakdown(
        "SELECT D.AGE_BAND, ROUND(SUM(O.ORDER_AMOUNT)) FROM FCT_ORDERS O JOIN DIM_CUSTOMERS D ON O.CUSTOMER_ID=D.CUSTOMER_ID WHERE O.ORDER_DATE >= DATEADD('day',-30,CURRENT_DATE) GROUP BY 1 ORDER BY 1")

    m['avg_order_value_by_loyalty_tier'] = breakdown(
        "SELECT D.LOYALTY_TIER, ROUND(AVG(O.ORDER_AMOUNT)) FROM FCT_ORDERS O JOIN DIM_CUSTOMERS D ON O.CUSTOMER_ID=D.CUSTOMER_ID GROUP BY 1 ORDER BY 2 DESC")

    m['churn_rate_by_acquisition_channel'] = breakdown(
        "SELECT ACQUISITION_CHANNEL, ROUND(SUM(CASE WHEN IS_CHURNED=TRUE THEN 1.0 ELSE 0 END)/COUNT(*),3) FROM DIM_CUSTOMERS GROUP BY 1 ORDER BY 2 DESC")

    m['high_risk_by_loyalty_tier'] = breakdown(
        "SELECT LOYALTY_TIER, COUNT(*) FROM DIM_CUSTOMERS WHERE CHURN_RISK_SCORE > 7 GROUP BY 1 ORDER BY 2 DESC")

    m['avg_churn_risk_score_by_loyalty_tier'] = breakdown(
        "SELECT LOYALTY_TIER, ROUND(AVG(CHURN_RISK_SCORE),1) FROM DIM_CUSTOMERS GROUP BY 1 ORDER BY 2 DESC")

    m['avg_email_open_rate_by_loyalty_tier'] = breakdown(
        "SELECT D.LOYALTY_TIER, ROUND(AVG(C.EMAIL_OPEN_RATE),3) FROM FCT_CAMPAIGNS C JOIN DIM_CUSTOMERS D ON C.CUSTOMER_ID=D.CUSTOMER_ID GROUP BY 1 ORDER BY 2 DESC")

    m['avg_email_click_rate_by_loyalty_tier'] = breakdown(
        "SELECT D.LOYALTY_TIER, ROUND(AVG(C.EMAIL_CLICK_RATE),3) FROM FCT_CAMPAIGNS C JOIN DIM_CUSTOMERS D ON C.CUSTOMER_ID=D.CUSTOMER_ID GROUP BY 1 ORDER BY 2 DESC")

    m['avg_personalization_score_by_age_band'] = breakdown(
        "SELECT D.AGE_BAND, ROUND(AVG(P.PERSONALIZATION_SCORE),1) FROM FEAT_PERSONALIZATION P JOIN DIM_CUSTOMERS D ON P.CUSTOMER_ID=D.CUSTOMER_ID GROUP BY 1 ORDER BY 1")

    cur.close()
    conn.close()
    return m

if __name__ == '__main__':
    metrics = build_metrics()
    output_path = os.path.join(os.path.dirname(__file__), '..', 'docs', 'metrics.json')
    with open(output_path, 'w') as f:
        json.dump(metrics, f, indent=2)
    print(f"metrics.json written — {metrics['updated_at']}")
    print(f"total_customers: {metrics['total_customers']}")
    print(f"churn_rate: {metrics['churn_rate']}")
    print(f"total_revenue: {metrics['total_revenue']}")
