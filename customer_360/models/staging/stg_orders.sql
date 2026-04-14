WITH source AS (
    SELECT * FROM {{ source('raw', 'orders') }}
),

deduplicated AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY LOWER(TRIM(ORDER_ID))
        ORDER BY _LOADED_AT DESC
    ) = 1
),

cleaned AS (
    SELECT
        -- Primary Key
        LOWER(TRIM(ORDER_ID))                           AS order_id,

        -- Foreign Key
        LOWER(TRIM(CUSTOMER_ID))                        AS customer_id,

        -- Dates
        ORDER_DATE,
        DATE_TRUNC('day', ORDER_DATE)                   AS order_date_day,
        DAYOFWEEK(ORDER_DATE)                           AS order_day_of_week,
        HOUR(ORDER_DATE)                                AS order_hour,
        MONTHNAME(ORDER_DATE)                           AS order_month,
        YEAR(ORDER_DATE)                                AS order_year,

        -- Status
        LOWER(TRIM(ORDER_STATUS))                       AS order_status,

        -- Financials
        COALESCE(TOTAL_AMOUNT, 0)                       AS total_amount,
        COALESCE(DISCOUNT_AMOUNT, 0)                    AS discount_amount,
        COALESCE(TAX_AMOUNT, 0)                         AS tax_amount,
        COALESCE(SHIPPING_AMOUNT, 0)                    AS shipping_amount,
        COALESCE(TOTAL_AMOUNT, 0)
            - COALESCE(DISCOUNT_AMOUNT, 0)              AS net_revenue,

        -- Derived flags
        CASE
            WHEN COALESCE(DISCOUNT_AMOUNT, 0) > 0
            THEN TRUE ELSE FALSE
        END                                             AS is_discounted,

        CASE
            WHEN PROMO_CODE IS NOT NULL
            THEN TRUE ELSE FALSE
        END                                             AS has_promo,

        CASE
            WHEN LOWER(TRIM(ORDER_STATUS))
                IN ('cancelled', 'returned')
            THEN TRUE ELSE FALSE
        END                                             AS is_cancelled_or_returned,

        CASE
            WHEN LOWER(TRIM(ORDER_STATUS)) = 'returned'
            THEN TRUE ELSE FALSE
        END                                             AS is_returned,

        -- Channel + Payment
        LOWER(TRIM(PAYMENT_METHOD))                     AS payment_method,
        LOWER(TRIM(CHANNEL))                            AS channel,
        TRIM(PROMO_CODE)                                AS promo_code,

        -- Shipping
        INITCAP(TRIM(SHIPPING_ADDRESS_CITY))            AS shipping_city,
        UPPER(TRIM(SHIPPING_ADDRESS_STATE))             AS shipping_state,
        UPPER(TRIM(SHIPPING_ADDRESS_COUNTRY))           AS shipping_country,

        -- Metadata
        _SOURCE_FILE,
        _LOADED_AT

    FROM deduplicated
    WHERE
        ORDER_ID    IS NOT NULL
        AND CUSTOMER_ID IS NOT NULL
)

SELECT * FROM cleaned