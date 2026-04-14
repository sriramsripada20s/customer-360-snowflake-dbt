WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

customers AS (
    SELECT
        customer_id,
        customer_sk
    FROM {{ ref('dim_customers') }}
)

SELECT
    -- Surrogate key
    MD5(t.transaction_id)                           AS transaction_sk,

    -- Natural keys
    t.transaction_id,
    t.order_id,
    t.customer_id,
    c.customer_sk,

    -- Dates
    t.transaction_date,
    t.transaction_date_day,
    t.transaction_hour,
    t.transaction_day_of_week,

    -- Financials
    t.amount,
    t.currency,

    -- Payment details
    t.payment_method,
    t.card_bin,
    t.payment_status,
    t.failure_reason,

    -- Device + Geography
    t.device_id,
    t.device_type,
    t.ip_address,
    t.geo_country,
    t.geo_city,
    t.merchant_category,
    t.is_international,

    -- Derived fraud flags
    t.is_failed,
    t.is_chargeback,
    t.is_refunded,
    t.is_authorized,
    t.is_night_transaction,
    t.is_high_value,

    -- Audit
    t._loaded_at

FROM transactions t
LEFT JOIN customers c
    ON t.customer_id = c.customer_id