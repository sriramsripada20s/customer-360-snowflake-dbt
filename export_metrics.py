import json
import os
from decimal import Decimal
from datetime import datetime, timezone
import snowflake.connector

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super().default(o)

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
    return [{"label": str(r[0]), "value": float(r[1]) if r[1] is not None else 0} for r in q(cursor, sql)]

def build_metrics():
    conn = get_connection()
    cur  = conn.cursor()
    m    = {}

    m['updated_at'] = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    def scalar(sql):
        result = q(cur, sql)[0][0]
        if result is None: return None
        return float(result) if isinstance(result, Decimal) else result

    m['total_customers']             = scalar("SELECT COUNT(*) FROM DIM_CUSTOMERS")
    m['active_customers']            = scalar("SELECT COUNT(*) FROM DIM_CUSTOMERS WHERE IS_ACTIVE = TRUE")
    m['churn_rate']                  = scalar("SELECT ROUND(AVG(CASE WHEN IS_ACTIVE=FALSE THEN 1.0 ELSE 0 END),3) FROM DIM_CUSTOMERS")
    m['total_revenue']               = scalar("SELECT ROUND(SUM(TOTAL_REVENUE)) FROM FCT_CUSTOMER_VALUE")
    m['revenue_last_30d']            = scalar("SELECT ROUND(SUM(REVENUE_LAST_30D)) FROM FCT_CUSTOMER_VALUE")
    m['revenue_last_90d']            = scalar("SELECT ROUND(SUM(REVENUE_LAST_90D)) FROM FCT_CUSTOMER_VALUE")
    m['avg_order_value']             = scalar("SELECT ROUND(AVG(AVG_ORDER_VALUE)) FROM FCT_CUSTOMER_VALUE WHERE AVG_ORDER_VALUE > 0")
    m['revenue_per_active_customer'] = scalar("SELECT ROUND(SUM(TOTAL_REVENUE)/NULLIF(COUNT(CASE WHEN IS_ACTIVE=TRUE THEN 1 END),0),1) FROM FCT_CUSTOMER_VALUE")
    m['avg_customer_value_score']    = scalar("SELECT ROUND(AVG(CUSTOMER_VALUE_SCORE),1) FROM FCT_CUSTOMER_VALUE")
    m['high_risk_customer_count']    = scalar("SELECT COUNT(*) FROM FCT_CUSTOMER_VALUE WHERE RISK_TIER = 'high'")
    m['avg_churn_risk_score']        = scalar("SELECT ROUND(AVG(CHURN_RISK_SCORE),1) FROM FCT_CUSTOMER_VALUE")
    m['avg_failed_txn_rate']         = scalar("SELECT ROUND(AVG(FAILED_TXN_RATE),3) FROM FCT_CUSTOMER_VALUE WHERE FAILED_TXN_RATE IS NOT NULL")
    m['avg_chargeback_rate']         = scalar("SELECT ROUND(AVG(CHARGEBACK_RATE),3) FROM FCT_CUSTOMER_VALUE WHERE CHARGEBACK_RATE IS NOT NULL")
    m['avg_cart_abandon_rate']       = scalar("SELECT ROUND(AVG(CART_ABANDON_RATE),3) FROM FCT_CUSTOMER_VALUE WHERE CART_ABANDON_RATE IS NOT NULL")
    m['avg_email_open_rate']         = scalar("SELECT ROUND(AVG(EMAIL_OPEN_RATE),3) FROM FCT_CUSTOMER_VALUE WHERE EMAIL_OPEN_RATE IS NOT NULL")
    m['avg_email_click_rate']        = scalar("SELECT ROUND(AVG(EMAIL_CLICK_RATE),3) FROM FCT_CUSTOMER_VALUE WHERE EMAIL_CLICK_RATE IS NOT NULL")
    m['avg_personalization_score']   = scalar("SELECT ROUND(AVG(PERSONALIZATION_SCORE),1) FROM FCT_CUSTOMER_VALUE WHERE PERSONALIZATION_SCORE IS NOT NULL")
    m['avg_recency_days']            = scalar("SELECT ROUND(AVG(RECENCY_DAYS),1) FROM FCT_CUSTOMER_VALUE WHERE RECENCY_DAYS IS NOT NULL")

    m['total_customers_by_loyalty_tier'] = breakdown(cur,
        "SELECT LOYALTY_TIER, COUNT(*) FROM DIM_CUSTOMERS GROUP BY 1 ORDER BY 2 DESC")
    m['active_customers_by_age_band'] = breakdown(cur,
        "SELECT AGE_BAND, COUNT(*) FROM DIM_CUSTOMERS WHERE IS_ACTIVE=TRUE GROUP BY 1 ORDER BY 1")
    m['avg_recency_days_by_loyalty_tier'] = breakdown(cur,
        "SELECT LOYALTY_TIER, ROUND(AVG(RECENCY_DAYS),1) FROM FCT_CUSTOMER_VALUE GROUP BY 1 ORDER BY 2")
    m['total_revenue_by_loyalty_tier'] = breakdown(cur,
        "SELECT LOYALTY_TIER, ROUND(SUM(TOTAL_REVENUE)) FROM FCT_CUSTOMER_VALUE GROUP BY 1 ORDER BY 2 DESC")
    m['revenue_last_30d_by_age_band'] = breakdown(cur,
        "SELECT AGE_BAND, ROUND(SUM(REVENUE_LAST_30D)) FROM FCT_CUSTOMER_VALUE GROUP BY 1 ORDER BY 1")
    m['avg_order_value_by_loyalty_tier'] = breakdown(cur,
        "SELECT LOYALTY_TIER, ROUND(AVG(AVG_ORDER_VALUE)) FROM FCT_CUSTOMER_VALUE WHERE AVG_ORDER_VALUE > 0 GROUP BY 1 ORDER BY 2 DESC")
    m['churn_rate_by_acquisition_channel'] = breakdown(cur,
        "SELECT ACQUISITION_CHANNEL, ROUND(AVG(CASE WHEN IS_ACTIVE=FALSE THEN 1.0 ELSE 0 END),3) FROM DIM_CUSTOMERS GROUP BY 1 ORDER BY 2 DESC")
    m['high_risk_by_loyalty_tier'] = breakdown(cur,
        "SELECT LOYALTY_TIER, COUNT(*) FROM FCT_CUSTOMER_VALUE WHERE RISK_TIER='high' GROUP BY 1 ORDER BY 2 DESC")
    m['avg_churn_risk_score_by_loyalty_tier'] = breakdown(cur,
        "SELECT LOYALTY_TIER, ROUND(AVG(CHURN_RISK_SCORE),1) FROM FCT_CUSTOMER_VALUE GROUP BY 1 ORDER BY 2 DESC")
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
        json.dump(metrics, f, indent=2, cls=DecimalEncoder)
    print(f"metrics.json written — {metrics['updated_at']}")
    print(f"total_customers     : {metrics['total_customers']}")
    print(f"active_customers    : {metrics['active_customers']}")
    print(f"churn_rate          : {metrics['churn_rate']}")
    print(f"total_revenue       : {metrics['total_revenue']}")
    print(f"high_risk_customers : {metrics['high_risk_customer_count']}")
    print(f"avg_order_value     : {metrics['avg_order_value']}")
