WITH customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),

-- ── ALL 5 INTERMEDIATE MODELS ─────────────────────────────────
orders AS (
    SELECT * FROM {{ ref('int_customer_orders') }}
),

transactions AS (
    SELECT * FROM {{ ref('int_customer_transactions') }}
),

fraud AS (
    SELECT * FROM {{ ref('int_customer_fraud_signals') }}
),

sessions AS (
    SELECT * FROM {{ ref('int_session_features') }}
),

affinity AS (
    SELECT * FROM {{ ref('int_product_affinity') }}
),

-- ── CAMPAIGN AGGREGATIONS ─────────────────────────────────────
campaigns AS (
    SELECT
        customer_id,

        COUNT(campaign_response_id)                 AS total_campaigns_received,

        SUM(CASE WHEN is_opened
            THEN 1 ELSE 0 END)                      AS campaigns_opened,

        SUM(CASE WHEN is_clicked
            THEN 1 ELSE 0 END)                      AS campaigns_clicked,

        SUM(CASE WHEN is_converted
            THEN 1 ELSE 0 END)                      AS campaigns_converted,

        SUM(CASE WHEN is_unsubscribed
            THEN 1 ELSE 0 END)                      AS campaigns_unsubscribed,

        CASE WHEN COUNT(campaign_response_id) > 0
            THEN SUM(CASE WHEN is_opened
                     THEN 1 ELSE 0 END)::FLOAT
                 / COUNT(campaign_response_id)
            ELSE 0
        END                                         AS email_open_rate,

        CASE WHEN COUNT(campaign_response_id) > 0
            THEN SUM(CASE WHEN is_clicked
                     THEN 1 ELSE 0 END)::FLOAT
                 / COUNT(campaign_response_id)
            ELSE 0
        END                                         AS email_click_rate,

        CASE WHEN COUNT(campaign_response_id) > 0
            THEN SUM(CASE WHEN is_converted
                     THEN 1 ELSE 0 END)::FLOAT
                 / COUNT(campaign_response_id)
            ELSE 0
        END                                         AS email_conversion_rate,

        SUM(conversion_amount)                      AS total_campaign_revenue,
        MODE(campaign_type)                         AS preferred_campaign_type

    FROM {{ ref('stg_campaigns') }}
    GROUP BY 1
)

SELECT
    -- ── IDENTITY ──────────────────────────────────────────────
    c.customer_sk,
    c.customer_id,
    c.full_name,
    c.email,
    c.age_band,
    c.gender,
    c.region,
    c.city,
    c.state,
    c.acquisition_channel,
    c.loyalty_tier,
    c.loyalty_tier_rank,
    c.tenure_days,
    c.tenure_band,
    c.signup_date,
    c.is_active,

    -- ── RFM METRICS ───────────────────────────────────────────
    COALESCE(o.recency_days, 999)                   AS recency_days,
    COALESCE(o.total_orders, 0)                     AS total_orders,
    COALESCE(o.total_revenue, 0)                    AS total_revenue,
    COALESCE(o.avg_order_value, 0)                  AS avg_order_value,
    COALESCE(o.max_order_value, 0)                  AS max_order_value,
    COALESCE(o.orders_last_90d, 0)                  AS orders_last_90d,
    COALESCE(o.orders_last_30d, 0)                  AS orders_last_30d,
    COALESCE(o.revenue_last_90d, 0)                 AS revenue_last_90d,
    COALESCE(o.revenue_last_30d, 0)                 AS revenue_last_30d,
    o.first_order_date,
    o.last_order_date,

    -- ── PURCHASE BEHAVIOR ─────────────────────────────────────
    COALESCE(o.discount_ratio, 0)                   AS discount_ratio,
    COALESCE(o.return_rate, 0)                      AS return_rate,
    COALESCE(o.returned_orders, 0)                  AS returned_orders,
    COALESCE(o.promo_orders, 0)                     AS promo_orders,
    COALESCE(o.weekend_order_ratio, 0)              AS weekend_order_ratio,
    COALESCE(o.distinct_categories_purchased, 0)    AS distinct_categories_purchased,
    o.preferred_channel,
    o.preferred_payment_method,

    -- ── DIGITAL BEHAVIOR ──────────────────────────────────────
    COALESCE(s.total_sessions, 0)                   AS total_sessions,
    COALESCE(s.sessions_last_30d, 0)                AS sessions_last_30d,
    COALESCE(s.sessions_last_7d, 0)                 AS sessions_last_7d,
    COALESCE(s.avg_session_duration_seconds, 0)     AS avg_session_duration_seconds,
    COALESCE(s.avg_product_views_per_session, 0)    AS avg_product_views_per_session,
    COALESCE(s.total_product_views, 0)              AS total_product_views,
    COALESCE(s.total_searches, 0)                   AS total_searches,
    COALESCE(s.cart_abandon_rate, 0)                AS cart_abandon_rate,
    COALESCE(s.cart_to_purchase_rate, 0)            AS cart_to_purchase_rate,
    s.preferred_device,
    s.time_of_day_preference,
    s.weekday_preference,
    s.most_browsed_category,
    s.last_session_date,
    COALESCE(s.days_since_last_session, 999)        AS days_since_last_session,

    -- ── PRODUCT AFFINITY ──────────────────────────────────────
    a.top_category_1,
    a.top_category_2,
    a.top_category_3,
    COALESCE(a.top_category_1_score, 0)             AS top_category_1_score,
    COALESCE(a.top_category_2_score, 0)             AS top_category_2_score,
    COALESCE(a.distinct_categories_purchased, 0)    AS distinct_categories_browsed,
    COALESCE(a.category_entropy, 0)                 AS category_entropy,
    COALESCE(a.overall_discount_item_ratio, 0)      AS overall_discount_item_ratio,
    COALESCE(a.avg_price_point, 0)                  AS avg_price_point,

    -- ── CAMPAIGN ENGAGEMENT ───────────────────────────────────
    COALESCE(camp.total_campaigns_received, 0)      AS total_campaigns_received,
    COALESCE(camp.campaigns_opened, 0)              AS campaigns_opened,
    COALESCE(camp.campaigns_clicked, 0)             AS campaigns_clicked,
    COALESCE(camp.campaigns_converted, 0)           AS campaigns_converted,
    COALESCE(camp.email_open_rate, 0)               AS email_open_rate,
    COALESCE(camp.email_click_rate, 0)              AS email_click_rate,
    COALESCE(camp.email_conversion_rate, 0)         AS email_conversion_rate,
    COALESCE(camp.total_campaign_revenue, 0)        AS total_campaign_revenue,
    camp.preferred_campaign_type,

    -- ── PAYMENT SIGNALS ───────────────────────────────────────
    COALESCE(t.total_transactions, 0)               AS total_transactions,
    COALESCE(t.total_txn_amount, 0)                 AS total_txn_amount,
    COALESCE(t.avg_txn_amount, 0)                   AS avg_txn_amount,
    COALESCE(t.failed_txn_rate, 0)                  AS failed_txn_rate,
    COALESCE(t.chargeback_rate, 0)                  AS chargeback_rate,
    COALESCE(t.chargeback_count, 0)                 AS chargeback_count,
    COALESCE(t.refund_count, 0)                     AS refund_count,
    COALESCE(t.refund_rate, 0)                      AS refund_rate,
    COALESCE(t.failed_txn_count_30d, 0)             AS failed_txn_count_30d,
    COALESCE(t.unique_cards_30d, 0)                 AS unique_cards_30d,
    COALESCE(t.unique_devices_30d, 0)               AS unique_devices_30d,
    COALESCE(t.unique_countries_30d, 0)             AS unique_countries_30d,
    COALESCE(t.night_activity_ratio, 0)             AS night_activity_ratio,
    COALESCE(t.international_txn_count, 0)          AS international_txn_count,
    t.preferred_payment_method                      AS preferred_payment_method_txn,

    -- ── FRAUD SIGNALS ─────────────────────────────────────────
    COALESCE(f.fraud_signal_count, 0)               AS fraud_signal_count,
    COALESCE(f.risk_tier, 'NORMAL')                 AS risk_tier,
    COALESCE(f.high_risk_device_flag, FALSE)        AS high_risk_device_flag,
    COALESCE(f.multiple_cards_flag, FALSE)          AS multiple_cards_flag,
    COALESCE(f.geo_velocity_flag, FALSE)            AS geo_velocity_flag,
    COALESCE(f.rapid_txn_flag, FALSE)               AS rapid_txn_flag,
    COALESCE(f.has_chargeback_flag, FALSE)          AS has_chargeback_flag,
    COALESCE(f.high_failure_rate_flag, FALSE)       AS high_failure_rate_flag,
    COALESCE(f.high_night_activity_flag, FALSE)     AS high_night_activity_flag,

    -- ── COMPOSITE SCORES ──────────────────────────────────────

    -- Customer Value Score (0-100)
    -- Recency 25pts + Frequency 25pts + Monetary 30pts + Engagement 20pts
    LEAST(100, GREATEST(0,
        -- Recency (25pts)
        25 * CASE
            WHEN COALESCE(o.recency_days, 999) <= 7   THEN 1.0
            WHEN COALESCE(o.recency_days, 999) <= 30  THEN 0.8
            WHEN COALESCE(o.recency_days, 999) <= 90  THEN 0.5
            WHEN COALESCE(o.recency_days, 999) <= 180 THEN 0.2
            ELSE 0
        END
        +
        -- Frequency (25pts)
        25 * LEAST(
            COALESCE(o.orders_last_90d, 0)::FLOAT / 10,
            1.0
        )
        +
        -- Monetary (30pts)
        30 * LEAST(
            COALESCE(o.revenue_last_90d, 0)::FLOAT / 5000,
            1.0
        )
        +
        -- Email engagement (10pts)
        10 * COALESCE(camp.email_click_rate, 0)
        +
        -- Session activity (10pts)
        10 * LEAST(
            COALESCE(s.sessions_last_30d, 0)::FLOAT / 20,
            1.0
        )
    ))                                              AS customer_value_score,

    -- Churn Risk Score (0-100)
    -- Higher score = higher risk of churning
    LEAST(100, GREATEST(0,
        -- Recency decline (30pts)
        30 * CASE
            WHEN COALESCE(o.recency_days, 999) > 180 THEN 1.0
            WHEN COALESCE(o.recency_days, 999) > 90  THEN 0.6
            WHEN COALESCE(o.recency_days, 999) > 30  THEN 0.2
            ELSE 0
        END
        +
        -- Frequency decline (20pts)
        20 * GREATEST(0,
            1 - LEAST(
                COALESCE(o.orders_last_90d, 0)::FLOAT / 5,
                1.0
            )
        )
        +
        -- Discount dependency (20pts)
        20 * COALESCE(o.discount_ratio, 0)
        +
        -- Payment failures (15pts)
        15 * LEAST(COALESCE(t.failed_txn_rate, 0), 1.0)
        +
        -- Cart abandonment (15pts)
        15 * LEAST(COALESCE(s.cart_abandon_rate, 0), 1.0)
    ))                                              AS churn_risk_score,

    -- Personalization Score (0-100)
    -- How well do we know this customer for personalization
    LEAST(100, GREATEST(0,
        -- Has category affinity (30pts)
        CASE WHEN a.top_category_1 IS NOT NULL THEN 30 ELSE 0 END
        +
        -- Has session data (25pts)
        CASE WHEN COALESCE(s.total_sessions, 0) > 5 THEN 25
             WHEN COALESCE(s.total_sessions, 0) > 0 THEN 10
             ELSE 0
        END
        +
        -- Has campaign response data (25pts)
        CASE WHEN COALESCE(camp.total_campaigns_received, 0) > 3 THEN 25
             WHEN COALESCE(camp.total_campaigns_received, 0) > 0 THEN 10
             ELSE 0
        END
        +
        -- Has purchase history (20pts)
        CASE WHEN COALESCE(o.total_orders, 0) > 5 THEN 20
             WHEN COALESCE(o.total_orders, 0) > 0 THEN 10
             ELSE 0
        END
    ))                                              AS personalization_score,

    CURRENT_TIMESTAMP()                             AS refreshed_at

FROM customers c
LEFT JOIN orders       o    ON c.customer_id = o.customer_id
LEFT JOIN transactions t    ON c.customer_id = t.customer_id
LEFT JOIN fraud        f    ON c.customer_id = f.customer_id
LEFT JOIN sessions     s    ON c.customer_id = s.customer_id
LEFT JOIN affinity     a    ON c.customer_id = a.customer_id
LEFT JOIN campaigns    camp ON c.customer_id = camp.customer_id