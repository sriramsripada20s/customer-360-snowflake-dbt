{{
  config(
    materialized   = 'incremental',
    unique_key     = 'customer_id',
    on_schema_change = 'sync_all_columns'
  )
}}

WITH base AS (
    SELECT * FROM {{ ref('fct_customer_value') }}
    WHERE is_active = TRUE
    {% if is_incremental() %}
        AND refreshed_at > (
            SELECT MAX(feature_created_at)
            FROM {{ this }}
        )
    {% endif %}
),

-- Global min/max for normalization
stats AS (
    SELECT
        MIN(recency_days)               AS min_recency,
        MAX(recency_days)               AS max_recency,
        MIN(total_orders)               AS min_orders,
        MAX(total_orders)               AS max_orders,
        MIN(total_revenue)              AS min_revenue,
        MAX(total_revenue)              AS max_revenue,
        MIN(avg_order_value)            AS min_aov,
        MAX(avg_order_value)            AS max_aov,
        MIN(email_click_rate)           AS min_click_rate,
        MAX(email_click_rate)           AS max_click_rate,
        MIN(sessions_last_30d)          AS min_sessions,
        MAX(sessions_last_30d)          AS max_sessions,
        MIN(cart_abandon_rate)          AS min_abandon,
        MAX(cart_abandon_rate)          AS max_abandon,
        MIN(discount_ratio)             AS min_discount,
        MAX(discount_ratio)             AS max_discount,
        MIN(category_entropy)           AS min_entropy,
        MAX(category_entropy)           AS max_entropy
    FROM {{ ref('fct_customer_value') }}
    WHERE is_active = TRUE
),

normalized AS (
    SELECT
        b.customer_id,
        b.customer_sk,

        -- ── RAW FEATURES (kept for interpretability) ──────────
        b.recency_days,
        b.total_orders,
        b.total_revenue,
        b.avg_order_value,
        b.orders_last_90d,
        b.revenue_last_90d,
        b.email_open_rate,
        b.email_click_rate,
        b.sessions_last_30d,
        b.cart_abandon_rate,
        b.discount_ratio,
        b.category_entropy,
        b.customer_value_score,
        b.churn_risk_score,
        b.personalization_score,
        b.top_category_1,
        b.preferred_channel,
        b.time_of_day_preference,
        b.loyalty_tier_rank,
        b.tenure_days,
        b.risk_tier,
        b.fraud_signal_count,

        -- ── NORMALIZED FEATURES [0-1] for KMeans ──────────────

        -- Recency: inverted so higher = more recent = better
        CASE WHEN (s.max_recency - s.min_recency) > 0
            THEN 1 - ((b.recency_days - s.min_recency)::FLOAT
                 / (s.max_recency - s.min_recency))
            ELSE 0
        END                             AS recency_norm,

        -- Frequency
        CASE WHEN (s.max_orders - s.min_orders) > 0
            THEN (b.total_orders - s.min_orders)::FLOAT
                 / (s.max_orders - s.min_orders)
            ELSE 0
        END                             AS frequency_norm,

        -- Monetary
        CASE WHEN (s.max_revenue - s.min_revenue) > 0
            THEN (b.total_revenue - s.min_revenue)::FLOAT
                 / (s.max_revenue - s.min_revenue)
            ELSE 0
        END                             AS monetary_norm,

        -- Average order value
        CASE WHEN (s.max_aov - s.min_aov) > 0
            THEN (b.avg_order_value - s.min_aov)::FLOAT
                 / (s.max_aov - s.min_aov)
            ELSE 0
        END                             AS avg_order_value_norm,

        -- Email engagement
        CASE WHEN (s.max_click_rate - s.min_click_rate) > 0
            THEN (b.email_click_rate - s.min_click_rate)::FLOAT
                 / (s.max_click_rate - s.min_click_rate)
            ELSE 0
        END                             AS email_engagement_norm,

        -- Session activity
        CASE WHEN (s.max_sessions - s.min_sessions) > 0
            THEN (b.sessions_last_30d - s.min_sessions)::FLOAT
                 / (s.max_sessions - s.min_sessions)
            ELSE 0
        END                             AS session_activity_norm,

        -- Conversion propensity: inverted abandon rate
        CASE WHEN (s.max_abandon - s.min_abandon) > 0
            THEN 1 - ((b.cart_abandon_rate - s.min_abandon)::FLOAT
                 / (s.max_abandon - s.min_abandon))
            ELSE 0
        END                             AS conversion_propensity_norm,

        -- Price sensitivity: inverted discount ratio
        CASE WHEN (s.max_discount - s.min_discount) > 0
            THEN 1 - ((b.discount_ratio - s.min_discount)::FLOAT
                 / (s.max_discount - s.min_discount))
            ELSE 0
        END                             AS price_sensitivity_norm,

        -- Category diversity
        CASE WHEN (s.max_entropy - s.min_entropy) > 0
            THEN (b.category_entropy - s.min_entropy)::FLOAT
                 / (s.max_entropy - s.min_entropy)
            ELSE 0
        END                             AS category_diversity_norm,

        -- Loyalty
        b.loyalty_tier_rank::FLOAT / 4  AS loyalty_norm,

        CURRENT_TIMESTAMP()             AS feature_created_at

    FROM base b
    CROSS JOIN stats s
)

SELECT * FROM normalized