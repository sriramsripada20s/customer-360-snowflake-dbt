-- Your account identifier
SELECT CURRENT_ACCOUNT();

-- Your username  
SELECT CURRENT_USER();

USE ROLE SYSADMIN;
USE DATABASE CUSTOMER_PLATFORM;
USE SCHEMA MART;


-- Snowflake Semantic View.
CREATE OR REPLACE SEMANTIC VIEW CUSTOMER_PLATFORM.MART.CUSTOMER_METRICS
  TABLES (
    fct AS CUSTOMER_PLATFORM.MART.FCT_CUSTOMER_VALUE
      PRIMARY KEY (customer_id),
    dim AS CUSTOMER_PLATFORM.MART.DIM_CUSTOMERS
      PRIMARY KEY (customer_id)
  )

  RELATIONSHIPS (
    dim_to_fct AS dim (customer_id) REFERENCES fct (customer_id)
  )

  FACTS (
    -- Revenue
    fct.total_revenue AS total_revenue,
    fct.revenue_last_30d AS revenue_last_30d,
    fct.revenue_last_90d AS revenue_last_90d,
    fct.avg_order_value AS avg_order_value,
    fct.max_order_value AS max_order_value,

    -- Orders
    fct.total_orders AS total_orders,
    fct.orders_last_30d AS orders_last_30d,
    fct.orders_last_90d AS orders_last_90d,
    fct.recency_days AS recency_days,
    fct.return_rate AS return_rate,
    fct.discount_ratio AS discount_ratio,

    -- Sessions
    fct.total_sessions AS total_sessions,
    fct.sessions_last_30d AS sessions_last_30d,
    fct.cart_abandon_rate AS cart_abandon_rate,
    fct.cart_to_purchase_rate AS cart_to_purchase_rate,
    fct.avg_session_duration_seconds AS avg_session_duration_seconds,

    -- Campaign engagement
    fct.email_open_rate AS email_open_rate,
    fct.email_click_rate AS email_click_rate,
    fct.email_conversion_rate AS email_conversion_rate,
    fct.total_campaigns_received AS total_campaigns_received,
    fct.campaigns_converted AS campaigns_converted,

    -- Payment signals
    fct.failed_txn_rate AS failed_txn_rate,
    fct.chargeback_rate AS chargeback_rate,
    fct.chargeback_count AS chargeback_count,
    fct.refund_rate AS refund_rate,

    -- Fraud signals
    fct.fraud_signal_count AS fraud_signal_count,

    -- Composite scores
    fct.customer_value_score AS customer_value_score,
    fct.churn_risk_score AS churn_risk_score,
    fct.personalization_score AS personalization_score
  )

  DIMENSIONS (
    -- Customer identity
    dim.loyalty_tier AS loyalty_tier,
    dim.tenure_band AS tenure_band,
    dim.age_band AS age_band,
    dim.gender AS gender,
    dim.region AS region,
    dim.state AS state,
    dim.city AS city,
    dim.acquisition_channel AS acquisition_channel,
    dim.is_active AS is_active,

    -- Risk and fraud
    fct.risk_tier AS risk_tier,
    fct.high_risk_device_flag AS high_risk_device_flag,
    fct.multiple_cards_flag AS multiple_cards_flag,
    fct.has_chargeback_flag AS has_chargeback_flag,

    -- Time dimensions
    fct.first_order_date AS first_order_date,
    fct.last_order_date AS last_order_date,
    fct.signup_date AS signup_date
  )

  METRICS (
    -- Volume
    fct.total_customers AS COUNT(DISTINCT fct.customer_id),

    fct.active_customers AS COUNT(DISTINCT 
      CASE WHEN dim.is_active = TRUE THEN fct.customer_id END
    ),

    -- Revenue
    fct.total_revenue_metric AS SUM(fct.total_revenue),
    fct.avg_order_value_metric AS AVG(fct.avg_order_value),
    fct.revenue_last_30d_metric AS SUM(fct.revenue_last_30d),
    fct.revenue_last_90d_metric AS SUM(fct.revenue_last_90d),
    fct.revenue_per_customer AS SUM(fct.total_revenue)
      / NULLIF(COUNT(DISTINCT fct.customer_id), 0),

    -- Scoring
    fct.avg_customer_value_score AS AVG(fct.customer_value_score),
    fct.avg_churn_risk_score AS AVG(fct.churn_risk_score),
    fct.avg_personalization_score AS AVG(fct.personalization_score),

    -- Churn proxy
    fct.high_risk_customers AS COUNT(DISTINCT 
      CASE WHEN fct.risk_tier = 'HIGH' THEN fct.customer_id END
    ),
    fct.churn_rate AS COUNT(DISTINCT 
      CASE WHEN fct.risk_tier = 'HIGH' THEN fct.customer_id END
    ) / NULLIF(COUNT(DISTINCT fct.customer_id), 0),

    -- Engagement
    fct.avg_email_open_rate AS AVG(fct.email_open_rate),
    fct.avg_email_click_rate AS AVG(fct.email_click_rate),
    fct.avg_cart_abandon_rate AS AVG(fct.cart_abandon_rate),

    -- Payment health
    fct.avg_failed_txn_rate AS AVG(fct.failed_txn_rate),
    fct.avg_chargeback_rate AS AVG(fct.chargeback_rate),

    -- Fraud
    fct.avg_fraud_signal_count AS AVG(fct.fraud_signal_count)
  );

SELECT
    loyalty_tier,
    AGG(total_customers)                          AS total_customers,
    ROUND(AGG(avg_customer_value_score), 2)       AS avg_value_score,
    ROUND(AGG(avg_churn_risk_score), 2)           AS avg_churn_risk,
    ROUND(AGG(churn_rate) * 100, 2)               AS churn_rate_pct,
    ROUND(AGG(total_revenue_metric), 2)           AS total_revenue
FROM CUSTOMER_PLATFORM.MART.CUSTOMER_METRICS
GROUP BY loyalty_tier
ORDER BY avg_value_score DESC;
