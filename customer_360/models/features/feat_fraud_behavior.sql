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

        -- From fct_customer_value
        cv.failed_txn_rate,
        cv.failed_txn_count_30d,
        cv.chargeback_rate,
        cv.chargeback_count,
        cv.unique_cards_30d,
        cv.unique_devices_30d,
        cv.unique_countries_30d,
        cv.night_activity_ratio,
        cv.international_txn_count,
        cv.total_transactions,
        cv.avg_txn_amount,
        cv.risk_tier,
        cv.fraud_signal_count,

        -- From int_customer_fraud_signals
        fs.max_customers_per_device,
        fs.high_risk_device_flag,
        fs.multiple_cards_flag,
        fs.unique_countries_7d,
        fs.unique_cities_7d,
        fs.geo_velocity_flag,
        fs.rapid_repeat_txn_count,
        fs.rapid_txn_flag,
        fs.high_night_activity_flag,
        fs.high_failure_rate_flag,
        fs.has_chargeback_flag,

        cv.refreshed_at

    FROM {{ ref('fct_customer_value') }} cv
    LEFT JOIN {{ ref('int_customer_fraud_signals') }} fs
        ON cv.customer_id = fs.customer_id

    {% if is_incremental() %}
    WHERE cv.refreshed_at > (
        SELECT MAX(feature_created_at)
        FROM {{ this }}
    )
    {% endif %}
),

fraud_features AS (
    SELECT
        customer_id,
        customer_sk,

        -- ── PAYMENT FAILURE SIGNALS ───────────────────────────
        failed_txn_rate,
        failed_txn_count_30d,
        chargeback_rate,
        chargeback_count,
        high_failure_rate_flag,
        has_chargeback_flag,

        -- ── CARD VELOCITY SIGNALS ─────────────────────────────
        unique_cards_30d,
        multiple_cards_flag,

        -- ── DEVICE RISK SIGNALS ───────────────────────────────
        unique_devices_30d,
        COALESCE(max_customers_per_device, 1)   AS max_customers_per_device,
        COALESCE(high_risk_device_flag, FALSE)  AS high_risk_device_flag,

        -- ── GEOGRAPHIC SIGNALS ────────────────────────────────
        unique_countries_30d,
        COALESCE(unique_countries_7d, 0)        AS unique_countries_7d,
        COALESCE(unique_cities_7d, 0)           AS unique_cities_7d,
        COALESCE(geo_velocity_flag, FALSE)      AS geo_velocity_flag,

        -- ── BEHAVIORAL SIGNALS ────────────────────────────────
        night_activity_ratio,
        COALESCE(high_night_activity_flag, FALSE) AS high_night_activity_flag,
        international_txn_count,
        CASE WHEN international_txn_count > 0
            THEN TRUE ELSE FALSE
        END                                     AS has_international_txn,

        -- ── RAPID TRANSACTION SIGNALS ─────────────────────────
        COALESCE(rapid_repeat_txn_count, 0)     AS rapid_repeat_txn_count,
        COALESCE(rapid_txn_flag, FALSE)         AS rapid_txn_flag,

        -- ── COMPOSITE SCORE ───────────────────────────────────
        fraud_signal_count,
        risk_tier,

        -- ── NORMALIZED FRAUD FEATURES [0-1] for IsolationForest
        -- Failed txn rate already 0-1
        failed_txn_rate                         AS failed_txn_rate_norm,

        -- Chargeback rate already 0-1
        chargeback_rate                         AS chargeback_rate_norm,

        -- Night activity already 0-1
        night_activity_ratio                    AS night_activity_norm,

        -- Unique cards (cap at 10)
        LEAST(unique_cards_30d::FLOAT / 10, 1.0)
                                                AS card_velocity_norm,

        -- Unique devices (cap at 5)
        LEAST(unique_devices_30d::FLOAT / 5, 1.0)
                                                AS device_velocity_norm,

        -- Unique countries 30d (cap at 5)
        LEAST(unique_countries_30d::FLOAT / 5, 1.0)
                                                AS geo_velocity_norm,

        -- Rapid transactions (cap at 10)
        LEAST(COALESCE(rapid_repeat_txn_count, 0)::FLOAT / 10, 1.0)
                                                AS rapid_txn_norm,

        -- Fraud signal count (max 7)
        fraud_signal_count::FLOAT / 7           AS fraud_signal_norm,

        -- Max customers per device (cap at 20)
        LEAST(COALESCE(max_customers_per_device, 1)::FLOAT / 20, 1.0)
                                                AS device_sharing_norm,

        CURRENT_TIMESTAMP()                     AS feature_created_at

    FROM base
)

SELECT * FROM fraud_features