WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

-- ── DEVICE RISK ───────────────────────────────────────────────
-- How many customers share the same device (mule network signal)
device_risk AS (
    SELECT
        device_id,
        COUNT(DISTINCT customer_id)             AS customers_per_device,
        COUNT(DISTINCT transaction_id)          AS txns_per_device
    FROM transactions
    WHERE device_id IS NOT NULL
      AND transaction_date >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    GROUP BY 1
),

-- ── RAPID REPEAT TRANSACTIONS ─────────────────────────────────
-- Same customer transacting within 60 seconds of previous
rapid_txn AS (
    SELECT
        customer_id,
        COUNT(*) AS rapid_repeat_txn_count
    FROM (
        SELECT
            customer_id,
            transaction_date,
            DATEDIFF('second',
                LAG(transaction_date) OVER (
                    PARTITION BY customer_id
                    ORDER BY transaction_date
                ),
                transaction_date
            ) AS seconds_since_last
        FROM transactions
        WHERE transaction_date >= DATEADD('day', -30, CURRENT_TIMESTAMP())
    )
    WHERE seconds_since_last < 60
    GROUP BY 1
),

-- ── GEOGRAPHIC VELOCITY ───────────────────────────────────────
-- How many countries in last 7 days
geo_velocity AS (
    SELECT
        customer_id,
        COUNT(DISTINCT geo_country)             AS unique_countries_7d,
        COUNT(DISTINCT geo_city)                AS unique_cities_7d
    FROM transactions
    WHERE transaction_date >= DATEADD('day', -7, CURRENT_TIMESTAMP())
    GROUP BY 1
),

-- ── BASE TRANSACTION SIGNALS ──────────────────────────────────
base AS (
    SELECT
        t.customer_id,
        COUNT(DISTINCT t.transaction_id)        AS total_transactions,
        SUM(CASE WHEN t.is_failed
            THEN 1 ELSE 0 END)                  AS failed_txn_count,
        SUM(CASE WHEN t.is_chargeback
            THEN 1 ELSE 0 END)                  AS chargeback_count,
        COUNT(DISTINCT CASE
            WHEN t.transaction_date >= DATEADD('day', -30, CURRENT_TIMESTAMP())
            THEN t.card_bin END)                AS unique_cards_30d,
        CASE WHEN COUNT(t.transaction_id) > 0
            THEN SUM(CASE WHEN t.is_failed
                     THEN 1 ELSE 0 END)::FLOAT
                 / COUNT(t.transaction_id)
            ELSE 0
        END                                     AS failed_txn_rate,
        CASE WHEN COUNT(t.transaction_id) > 0
            THEN SUM(CASE WHEN t.is_night_transaction
                     THEN 1 ELSE 0 END)::FLOAT
                 / COUNT(t.transaction_id)
            ELSE 0
        END                                     AS night_activity_ratio,
        MAX(dr.customers_per_device)            AS max_customers_per_device
    FROM transactions t
    LEFT JOIN device_risk dr
        ON t.device_id = dr.device_id
    GROUP BY 1
),

fraud_signals AS (
    SELECT
        b.customer_id,

        -- ── DEVICE RISK FLAGS ─────────────────────────────────
        COALESCE(b.max_customers_per_device, 1) AS max_customers_per_device,
        CASE WHEN COALESCE(b.max_customers_per_device, 1) > 5
            THEN TRUE ELSE FALSE
        END                                     AS high_risk_device_flag,

        -- ── CARD VELOCITY FLAGS ───────────────────────────────
        b.unique_cards_30d,
        CASE WHEN b.unique_cards_30d > 3
            THEN TRUE ELSE FALSE
        END                                     AS multiple_cards_flag,

        -- ── GEOGRAPHIC FLAGS ──────────────────────────────────
        COALESCE(gv.unique_countries_7d, 0)     AS unique_countries_7d,
        COALESCE(gv.unique_cities_7d, 0)        AS unique_cities_7d,
        CASE WHEN COALESCE(gv.unique_countries_7d, 0) > 2
            THEN TRUE ELSE FALSE
        END                                     AS geo_velocity_flag,

        -- ── RAPID TRANSACTION FLAGS ───────────────────────────
        COALESCE(rt.rapid_repeat_txn_count, 0)  AS rapid_repeat_txn_count,
        CASE WHEN COALESCE(rt.rapid_repeat_txn_count, 0) > 2
            THEN TRUE ELSE FALSE
        END                                     AS rapid_txn_flag,

        -- ── NIGHT ACTIVITY FLAG ───────────────────────────────
        b.night_activity_ratio,
        CASE WHEN b.night_activity_ratio > 0.7
            THEN TRUE ELSE FALSE
        END                                     AS high_night_activity_flag,

        -- ── FAILURE FLAGS ─────────────────────────────────────
        b.failed_txn_rate,
        CASE WHEN b.failed_txn_rate > 0.3
            THEN TRUE ELSE FALSE
        END                                     AS high_failure_rate_flag,

        b.chargeback_count,
        CASE WHEN b.chargeback_count > 0
            THEN TRUE ELSE FALSE
        END                                     AS has_chargeback_flag,

        -- ── COMPOSITE FRAUD SIGNAL SCORE ──────────────────────
        -- Each flag = 1 point
        -- Range: 0 (clean) to 7 (highly suspicious)
        (
            CASE WHEN b.failed_txn_rate > 0.3          THEN 1 ELSE 0 END +
            CASE WHEN b.chargeback_count > 0            THEN 1 ELSE 0 END +
            CASE WHEN b.max_customers_per_device > 5    THEN 1 ELSE 0 END +
            CASE WHEN b.unique_cards_30d > 3            THEN 1 ELSE 0 END +
            CASE WHEN gv.unique_countries_7d > 2        THEN 1 ELSE 0 END +
            CASE WHEN rt.rapid_repeat_txn_count > 2     THEN 1 ELSE 0 END +
            CASE WHEN b.night_activity_ratio > 0.7      THEN 1 ELSE 0 END
        )                                               AS fraud_signal_count,

        -- ── RISK TIER ─────────────────────────────────────────
        CASE
            WHEN (
                CASE WHEN b.failed_txn_rate > 0.3       THEN 1 ELSE 0 END +
                CASE WHEN b.chargeback_count > 0         THEN 1 ELSE 0 END +
                CASE WHEN b.max_customers_per_device > 5 THEN 1 ELSE 0 END +
                CASE WHEN b.unique_cards_30d > 3         THEN 1 ELSE 0 END +
                CASE WHEN gv.unique_countries_7d > 2     THEN 1 ELSE 0 END +
                CASE WHEN rt.rapid_repeat_txn_count > 2  THEN 1 ELSE 0 END +
                CASE WHEN b.night_activity_ratio > 0.7   THEN 1 ELSE 0 END
            ) >= 4 THEN 'HIGH'
            WHEN (
                CASE WHEN b.failed_txn_rate > 0.3       THEN 1 ELSE 0 END +
                CASE WHEN b.chargeback_count > 0         THEN 1 ELSE 0 END +
                CASE WHEN b.max_customers_per_device > 5 THEN 1 ELSE 0 END +
                CASE WHEN b.unique_cards_30d > 3         THEN 1 ELSE 0 END +
                CASE WHEN gv.unique_countries_7d > 2     THEN 1 ELSE 0 END +
                CASE WHEN rt.rapid_repeat_txn_count > 2  THEN 1 ELSE 0 END +
                CASE WHEN b.night_activity_ratio > 0.7   THEN 1 ELSE 0 END
            ) >= 2 THEN 'MEDIUM'
            WHEN (
                CASE WHEN b.failed_txn_rate > 0.3       THEN 1 ELSE 0 END +
                CASE WHEN b.chargeback_count > 0         THEN 1 ELSE 0 END +
                CASE WHEN b.max_customers_per_device > 5 THEN 1 ELSE 0 END +
                CASE WHEN b.unique_cards_30d > 3         THEN 1 ELSE 0 END +
                CASE WHEN gv.unique_countries_7d > 2     THEN 1 ELSE 0 END +
                CASE WHEN rt.rapid_repeat_txn_count > 2  THEN 1 ELSE 0 END +
                CASE WHEN b.night_activity_ratio > 0.7   THEN 1 ELSE 0 END
            ) >= 1 THEN 'LOW'
            ELSE 'NORMAL'
        END                                             AS risk_tier

    FROM base b
    LEFT JOIN geo_velocity gv   ON b.customer_id = gv.customer_id
    LEFT JOIN rapid_txn    rt   ON b.customer_id = rt.customer_id
)

SELECT * FROM fraud_signals