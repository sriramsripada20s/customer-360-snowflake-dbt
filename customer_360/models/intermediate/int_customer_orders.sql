WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

customer_orders AS (
    SELECT
        o.customer_id,

        -- ── ORDER COUNTS ─────────────────────────────────────
        COUNT(DISTINCT o.order_id)                          AS total_orders,

        COUNT(DISTINCT CASE
            WHEN o.order_date >= DATEADD('day', -90, CURRENT_DATE())
            THEN o.order_id END)                            AS orders_last_90d,

        COUNT(DISTINCT CASE
            WHEN o.order_date >= DATEADD('day', -30, CURRENT_DATE())
            THEN o.order_id END)                            AS orders_last_30d,

        -- ── REVENUE ──────────────────────────────────────────
        SUM(o.net_revenue)                                  AS total_revenue,

        SUM(CASE
            WHEN o.order_date >= DATEADD('day', -90, CURRENT_DATE())
            THEN o.net_revenue ELSE 0 END)                  AS revenue_last_90d,

        SUM(CASE
            WHEN o.order_date >= DATEADD('day', -30, CURRENT_DATE())
            THEN o.net_revenue ELSE 0 END)                  AS revenue_last_30d,

        AVG(o.net_revenue)                                  AS avg_order_value,

        MAX(o.net_revenue)                                  AS max_order_value,
        MIN(o.net_revenue)                                  AS min_order_value,

        -- ── RECENCY ──────────────────────────────────────────
        MAX(o.order_date)                                   AS last_order_date,
        MIN(o.order_date)                                   AS first_order_date,

        DATEDIFF('day',
            MAX(o.order_date),
            CURRENT_DATE())                                 AS recency_days,

        -- ── DISCOUNT BEHAVIOR ─────────────────────────────────
        SUM(CASE
            WHEN o.is_discounted THEN 1 ELSE 0 END)         AS discounted_orders,

        CASE WHEN COUNT(o.order_id) > 0
            THEN SUM(CASE WHEN o.is_discounted
                     THEN 1 ELSE 0 END)::FLOAT
                 / COUNT(o.order_id)
            ELSE 0
        END                                                 AS discount_ratio,

        SUM(o.discount_amount)                              AS total_discount_amount,

        -- ── RETURNS ───────────────────────────────────────────
        COUNT(DISTINCT CASE
            WHEN o.is_returned
            THEN o.order_id END)                            AS returned_orders,

        CASE WHEN COUNT(o.order_id) > 0
            THEN COUNT(DISTINCT CASE
                WHEN o.is_returned
                THEN o.order_id END)::FLOAT
                / COUNT(o.order_id)
            ELSE 0
        END                                                 AS return_rate,

        -- ── CHANNEL + PAYMENT ─────────────────────────────────
        MODE(o.channel)                                     AS preferred_channel,
        MODE(o.payment_method)                              AS preferred_payment_method,

        -- ── PROMO USAGE ───────────────────────────────────────
        COUNT(DISTINCT CASE
            WHEN o.has_promo
            THEN o.order_id END)                            AS promo_orders,

        -- ── PRODUCT DIVERSITY ─────────────────────────────────
        COUNT(DISTINCT oi.category)                         AS distinct_categories_purchased,

        -- ── ORDER TIMING ──────────────────────────────────────
        ROUND(AVG(o.order_hour), 0)                         AS avg_order_hour,

        SUM(CASE
            WHEN o.order_day_of_week IN (0, 6)
            THEN 1 ELSE 0 END)                              AS weekend_orders,

        CASE WHEN COUNT(o.order_id) > 0
            THEN SUM(CASE
                WHEN o.order_day_of_week IN (0, 6)
                THEN 1 ELSE 0 END)::FLOAT
                / COUNT(o.order_id)
            ELSE 0
        END                                                 AS weekend_order_ratio

    FROM orders o
    LEFT JOIN order_items oi
        ON o.order_id = oi.order_id
    WHERE o.is_cancelled_or_returned = FALSE
    GROUP BY 1
)

SELECT * FROM customer_orders