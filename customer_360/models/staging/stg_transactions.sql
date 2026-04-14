WITH source AS (
    SELECT * FROM {{ source('raw', 'transactions') }}
),

deduplicated AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY LOWER(TRIM(TRANSACTION_ID))
        ORDER BY _LOADED_AT DESC
    ) = 1
),

cleaned AS (
    SELECT
        -- Primary Key
        LOWER(TRIM(TRANSACTION_ID))                     AS transaction_id,

        -- Foreign Keys
        LOWER(TRIM(ORDER_ID))                           AS order_id,
        LOWER(TRIM(CUSTOMER_ID))                        AS customer_id,

        -- Dates
        TRANSACTION_DATE,
        DATE_TRUNC('day', TRANSACTION_DATE)             AS transaction_date_day,
        HOUR(TRANSACTION_DATE)                          AS transaction_hour,
        DAYOFWEEK(TRANSACTION_DATE)                     AS transaction_day_of_week,

        -- Financials
        COALESCE(AMOUNT, 0)                             AS amount,
        UPPER(TRIM(CURRENCY))                           AS currency,

        -- Payment
        LOWER(TRIM(PAYMENT_METHOD))                     AS payment_method,
        TRIM(CARD_BIN)                                  AS card_bin,
        LOWER(TRIM(PAYMENT_STATUS))                     AS payment_status,
        TRIM(FAILURE_REASON)                            AS failure_reason,

        -- Device signals (key for fraud)
        LOWER(TRIM(DEVICE_ID))                          AS device_id,
        LOWER(TRIM(DEVICE_TYPE))                        AS device_type,
        TRIM(IP_ADDRESS)                                AS ip_address,

        -- Geography
        UPPER(TRIM(GEO_COUNTRY))                        AS geo_country,
        INITCAP(TRIM(GEO_CITY))                         AS geo_city,
        LOWER(TRIM(MERCHANT_CATEGORY))                  AS merchant_category,
        COALESCE(IS_INTERNATIONAL, FALSE)               AS is_international,

        -- Derived fraud flags
        CASE
            WHEN LOWER(TRIM(PAYMENT_STATUS)) = 'failed'
            THEN TRUE ELSE FALSE
        END                                             AS is_failed,

        CASE
            WHEN LOWER(TRIM(PAYMENT_STATUS)) = 'chargeback'
            THEN TRUE ELSE FALSE
        END                                             AS is_chargeback,

        CASE
            WHEN LOWER(TRIM(PAYMENT_STATUS)) = 'refunded'
            THEN TRUE ELSE FALSE
        END                                             AS is_refunded,

        CASE
            WHEN LOWER(TRIM(PAYMENT_STATUS)) = 'authorized'
            THEN TRUE ELSE FALSE
        END                                             AS is_authorized,

        -- Night transaction flag (00:00 - 05:59)
        CASE
            WHEN HOUR(TRANSACTION_DATE) BETWEEN 0 AND 5
            THEN TRUE ELSE FALSE
        END                                             AS is_night_transaction,

        -- High value transaction flag (above $300)
        CASE
            WHEN COALESCE(AMOUNT, 0) > 300
            THEN TRUE ELSE FALSE
        END                                             AS is_high_value,

        -- Metadata
        _SOURCE_FILE,
        _LOADED_AT

    FROM deduplicated
    WHERE
        TRANSACTION_ID  IS NOT NULL
        AND CUSTOMER_ID IS NOT NULL
)

SELECT * FROM cleaned