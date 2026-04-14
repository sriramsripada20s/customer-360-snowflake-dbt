WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
)

SELECT
    customer_id,

    -- ── VOLUME ───────────────────────────────────────────────
    COUNT(DISTINCT transaction_id)                      AS total_transactions,
    SUM(amount)                                         AS total_txn_amount,
    AVG(amount)                                         AS avg_txn_amount,
    MAX(amount)                                         AS max_txn_amount,

    -- ── PAYMENT STATUS COUNTS ─────────────────────────────────
    SUM(CASE WHEN is_failed     THEN 1 ELSE 0 END)      AS failed_txn_count,
    SUM(CASE WHEN is_chargeback THEN 1 ELSE 0 END)      AS chargeback_count,
    SUM(CASE WHEN is_refunded   THEN 1 ELSE 0 END)      AS refund_count,
    SUM(CASE WHEN is_authorized THEN 1 ELSE 0 END)      AS authorized_txn_count,
    SUM(CASE WHEN is_high_value THEN 1 ELSE 0 END)      AS high_value_txn_count,

    -- ── FAILURE RATES ─────────────────────────────────────────
    CASE WHEN COUNT(transaction_id) > 0
        THEN SUM(CASE WHEN is_failed
                 THEN 1 ELSE 0 END)::FLOAT
             / COUNT(transaction_id)
        ELSE 0
    END                                                 AS failed_txn_rate,

    CASE WHEN COUNT(transaction_id) > 0
        THEN SUM(CASE WHEN is_chargeback
                 THEN 1 ELSE 0 END)::FLOAT
             / COUNT(transaction_id)
        ELSE 0
    END                                                 AS chargeback_rate,

    CASE WHEN COUNT(transaction_id) > 0
        THEN SUM(CASE WHEN is_refunded
                 THEN 1 ELSE 0 END)::FLOAT
             / COUNT(transaction_id)
        ELSE 0
    END                                                 AS refund_rate,

    -- ── 30 DAY ROLLING ───────────────────────────────────────
    SUM(CASE
        WHEN transaction_date >= DATEADD('day', -30, CURRENT_TIMESTAMP())
         AND is_failed
        THEN 1 ELSE 0 END)                              AS failed_txn_count_30d,

    COUNT(DISTINCT CASE
        WHEN transaction_date >= DATEADD('day', -30, CURRENT_TIMESTAMP())
        THEN card_bin END)                              AS unique_cards_30d,

    COUNT(DISTINCT CASE
        WHEN transaction_date >= DATEADD('day', -30, CURRENT_TIMESTAMP())
        THEN device_id END)                             AS unique_devices_30d,

    COUNT(DISTINCT CASE
        WHEN transaction_date >= DATEADD('day', -30, CURRENT_TIMESTAMP())
        THEN geo_country END)                           AS unique_countries_30d,

    -- ── NIGHT ACTIVITY ────────────────────────────────────────
    SUM(CASE WHEN is_night_transaction
        THEN 1 ELSE 0 END)                              AS night_txn_count,

    CASE WHEN COUNT(transaction_id) > 0
        THEN SUM(CASE WHEN is_night_transaction
                 THEN 1 ELSE 0 END)::FLOAT
             / COUNT(transaction_id)
        ELSE 0
    END                                                 AS night_activity_ratio,

    -- ── INTERNATIONAL ─────────────────────────────────────────
    SUM(CASE WHEN is_international
        THEN 1 ELSE 0 END)                              AS international_txn_count,

    -- ── RECENCY + PREFERENCE ──────────────────────────────────
    MAX(transaction_date)                               AS last_transaction_date,
    MODE(payment_method)                                AS preferred_payment_method

FROM transactions
GROUP BY 1