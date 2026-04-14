WITH source AS (
    SELECT * FROM {{ source('raw', 'order_items') }}
),

deduplicated AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY LOWER(TRIM(ORDER_ITEM_ID))
        ORDER BY _LOADED_AT DESC
    ) = 1
),

cleaned AS (
    SELECT
        -- Primary Key
        LOWER(TRIM(ORDER_ITEM_ID))                      AS order_item_id,

        -- Foreign Keys
        LOWER(TRIM(ORDER_ID))                           AS order_id,
        LOWER(TRIM(PRODUCT_ID))                         AS product_id,

        -- Financials
        COALESCE(QUANTITY, 0)                           AS quantity,
        COALESCE(UNIT_PRICE, 0)                         AS unit_price,
        COALESCE(DISCOUNT_AMOUNT, 0)                    AS discount_amount,
        COALESCE(LINE_TOTAL, 0)                         AS line_total,

        -- Derived
        COALESCE(UNIT_PRICE, 0)
            * COALESCE(QUANTITY, 0)                     AS gross_line_total,

        COALESCE(LINE_TOTAL, 0)
            - COALESCE(DISCOUNT_AMOUNT, 0)              AS net_line_total,

        CASE
            WHEN COALESCE(DISCOUNT_AMOUNT, 0) > 0
            THEN TRUE ELSE FALSE
        END                                             AS is_discounted,

        CASE
            WHEN COALESCE(QUANTITY, 0) > 3
            THEN TRUE ELSE FALSE
        END                                             AS is_bulk_purchase,

        -- Category
        LOWER(TRIM(CATEGORY))                           AS category,

        -- Metadata
        _SOURCE_FILE,
        _LOADED_AT

    FROM deduplicated
    WHERE
        ORDER_ITEM_ID IS NOT NULL
        AND ORDER_ID  IS NOT NULL
        AND PRODUCT_ID IS NOT NULL
)

SELECT * FROM cleaned