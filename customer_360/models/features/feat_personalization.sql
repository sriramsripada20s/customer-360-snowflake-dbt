{{
  config(
    materialized   = 'incremental',
    unique_key     = 'customer_id',
    on_schema_change = 'sync_all_columns'
  )
}}

WITH base AS (
    SELECT
        cv.customer_id,
        cv.customer_sk,

        -- Identity
        cv.age_band,
        cv.gender,
        cv.region,
        cv.loyalty_tier,
        cv.loyalty_tier_rank,
        cv.tenure_days,
        cv.tenure_band,
        cv.acquisition_channel,

        -- Product affinity
        cv.top_category_1,
        cv.top_category_2,
        cv.top_category_3,
        cv.top_category_1_score,
        cv.top_category_2_score,
        cv.category_entropy,
        cv.most_browsed_category,
        cv.avg_price_point,
        cv.overall_discount_item_ratio,

        -- Digital behavior
        cv.sessions_last_30d,
        cv.sessions_last_7d,
        cv.avg_session_duration_seconds,
        cv.avg_product_views_per_session,
        cv.total_searches,
        cv.cart_abandon_rate,
        cv.cart_to_purchase_rate,
        cv.preferred_device,
        cv.time_of_day_preference,
        cv.weekday_preference,

        -- Purchase behavior
        cv.total_orders,
        cv.avg_order_value,
        cv.discount_ratio,
        cv.preferred_channel,
        cv.preferred_payment_method,
        cv.orders_last_90d,
        cv.revenue_last_90d,

        -- Campaign engagement
        cv.email_open_rate,
        cv.email_click_rate,
        cv.email_conversion_rate,
        cv.preferred_campaign_type,
        cv.total_campaigns_received,

        -- Scores
        cv.customer_value_score,
        cv.churn_risk_score,
        cv.personalization_score,

        -- Product affinity details
        pa.top_category_1_spend,
        pa.top_category_2_spend,
        pa.distinct_categories_purchased,

        cv.refreshed_at

    FROM {{ ref('fct_customer_value') }} cv
    LEFT JOIN {{ ref('int_product_affinity') }} pa
        ON cv.customer_id = pa.customer_id

    {% if is_incremental() %}
    WHERE cv.refreshed_at > (
        SELECT MAX(feature_created_at)
        FROM {{ this }}
    )
    {% endif %}
),

personalization AS (
    SELECT
        customer_id,
        customer_sk,

        -- ── CUSTOMER PROFILE ──────────────────────────────────
        age_band,
        gender,
        region,
        loyalty_tier,
        loyalty_tier_rank,
        tenure_days,
        tenure_band,
        acquisition_channel,

        -- ── PRODUCT AFFINITY ──────────────────────────────────
        top_category_1,
        top_category_2,
        top_category_3,
        top_category_1_score,
        top_category_2_score,
        category_entropy,
        most_browsed_category,
        COALESCE(distinct_categories_purchased, 0)  AS distinct_categories_purchased,
        COALESCE(top_category_1_spend, 0)           AS top_category_1_spend,
        COALESCE(top_category_2_spend, 0)           AS top_category_2_spend,

        -- ── PRICE SENSITIVITY ─────────────────────────────────
        avg_price_point,
        overall_discount_item_ratio,
        discount_ratio,

        -- Price segment
        CASE
            WHEN avg_price_point >= 200 THEN 'premium'
            WHEN avg_price_point >= 100 THEN 'mid'
            WHEN avg_price_point >= 50  THEN 'value'
            ELSE 'budget'
        END                                         AS price_segment,

        -- Discount sensitivity
        CASE
            WHEN discount_ratio >= 0.7 THEN 'high'
            WHEN discount_ratio >= 0.4 THEN 'medium'
            WHEN discount_ratio >= 0.1 THEN 'low'
            ELSE 'none'
        END                                         AS discount_sensitivity,

        -- ── CHANNEL PREFERENCE ────────────────────────────────
        preferred_channel,
        preferred_device,
        preferred_campaign_type,
        preferred_payment_method,

        -- ── TIMING PREFERENCE ─────────────────────────────────
        time_of_day_preference,
        weekday_preference,

        -- ── ENGAGEMENT SIGNALS ────────────────────────────────
        sessions_last_30d,
        sessions_last_7d,
        avg_session_duration_seconds,
        avg_product_views_per_session,
        total_searches,
        cart_abandon_rate,
        cart_to_purchase_rate,
        email_open_rate,
        email_click_rate,
        email_conversion_rate,
        total_campaigns_received,

        -- Email engagement tier
        CASE
            WHEN email_click_rate >= 0.3  THEN 'highly_engaged'
            WHEN email_click_rate >= 0.1  THEN 'engaged'
            WHEN email_click_rate >= 0.05 THEN 'low_engaged'
            ELSE 'unengaged'
        END                                         AS email_engagement_tier,

        -- ── PURCHASE SIGNALS ──────────────────────────────────
        total_orders,
        avg_order_value,
        orders_last_90d,
        revenue_last_90d,

        -- Purchase frequency tier
        CASE
            WHEN orders_last_90d >= 10 THEN 'very_frequent'
            WHEN orders_last_90d >= 5  THEN 'frequent'
            WHEN orders_last_90d >= 2  THEN 'occasional'
            WHEN orders_last_90d >= 1  THEN 'rare'
            ELSE 'inactive'
        END                                         AS purchase_frequency_tier,

        -- ── COMPOSITE SCORES ──────────────────────────────────
        customer_value_score,
        churn_risk_score,
        personalization_score,

        -- ── RECOMMENDED ACTIONS ───────────────────────────────
        -- Best channel to reach this customer
        CASE
            WHEN preferred_campaign_type IS NOT NULL
                THEN preferred_campaign_type
            WHEN email_click_rate >= 0.1
                THEN 'email'
            WHEN sessions_last_7d >= 5
                THEN 'push'
            ELSE 'email'
        END                                         AS recommended_channel,

        -- Best time to reach this customer
        CASE
            WHEN time_of_day_preference = 'morning'
                THEN '07:00-10:00'
            WHEN time_of_day_preference = 'afternoon'
                THEN '12:00-15:00'
            WHEN time_of_day_preference = 'evening'
                THEN '18:00-21:00'
            ELSE '10:00-12:00'
        END                                         AS recommended_send_time,

        -- Best offer type for this customer
        CASE
            WHEN discount_ratio >= 0.7
                THEN 'discount_offer'
            WHEN top_category_1_score >= 0.5
                THEN 'category_recommendation'
            WHEN cart_abandon_rate >= 0.6
                THEN 'cart_recovery'
            WHEN churn_risk_score >= 70
                THEN 'winback_offer'
            WHEN customer_value_score >= 75
                THEN 'loyalty_reward'
            ELSE 'new_arrival'
        END                                         AS recommended_offer_type,

        CURRENT_TIMESTAMP()                         AS feature_created_at

    FROM base
)

SELECT * FROM personalization