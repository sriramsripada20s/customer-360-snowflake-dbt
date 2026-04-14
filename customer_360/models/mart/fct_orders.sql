WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT
        customer_id,
        customer_sk
    FROM {{ ref('dim_customers') }}
)

SELECT
    -- Surrogate key
    MD5(o.order_id)                                 AS order_sk,

    -- Natural keys
    o.order_id,
    o.customer_id,
    c.customer_sk,

    -- Dates
    o.order_date,
    o.order_date_day,
    o.order_day_of_week,
    o.order_hour,
    o.order_month,
    o.order_year,

    -- Status
    o.order_status,
    o.is_cancelled_or_returned,
    o.is_returned,

    -- Financials
    o.total_amount,
    o.discount_amount,
    o.tax_amount,
    o.shipping_amount,
    o.net_revenue,

    -- Flags
    o.is_discounted,
    o.has_promo,
    o.promo_code,

    -- Channel
    o.payment_method,
    o.channel,

    -- Shipping
    o.shipping_city,
    o.shipping_state,
    o.shipping_country,

    -- Audit
    o._loaded_at

FROM orders o
LEFT JOIN customers c
    ON o.customer_id = c.customer_id