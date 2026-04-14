WITH source AS (
    SELECT * FROM {{ source('raw', 'products') }}
),

deduplicated AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY LOWER(TRIM(PRODUCT_ID))
        ORDER BY _LOADED_AT DESC
    ) = 1
),

cleaned AS (
    SELECT
        -- Primary Key
        LOWER(TRIM(PRODUCT_ID))                         AS product_id,

        -- Product details
        INITCAP(TRIM(PRODUCT_NAME))                     AS product_name,
        LOWER(TRIM(CATEGORY))                           AS category,
        LOWER(TRIM(SUBCATEGORY))                        AS subcategory,
        INITCAP(TRIM(BRAND))                            AS brand,

        -- Pricing
        COALESCE(BASE_PRICE, 0)                         AS base_price,
        COALESCE(COST_PRICE, 0)                         AS cost_price,

        -- Margin
        COALESCE(BASE_PRICE, 0)
            - COALESCE(COST_PRICE, 0)                   AS gross_margin,

        CASE
            WHEN COALESCE(BASE_PRICE, 0) > 0
            THEN ROUND(
                (COALESCE(BASE_PRICE, 0)
                - COALESCE(COST_PRICE, 0))
                / COALESCE(BASE_PRICE, 0) * 100, 2)
            ELSE 0
        END                                             AS margin_pct,

        -- Price band
        LOWER(TRIM(PRICE_BAND))                         AS price_band,

        CASE LOWER(TRIM(PRICE_BAND))
            WHEN 'luxury'  THEN 4
            WHEN 'premium' THEN 3
            WHEN 'mid'     THEN 2
            ELSE 1
        END                                             AS price_band_rank,

        -- Status
        COALESCE(IS_ACTIVE, TRUE)                       AS is_active,
        CREATED_AT,

        -- Metadata
        _SOURCE_FILE,
        _LOADED_AT

    FROM deduplicated
    WHERE
        PRODUCT_ID   IS NOT NULL
        AND PRODUCT_NAME IS NOT NULL
)

SELECT * FROM cleaned