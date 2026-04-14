WITH source AS (
    SELECT * FROM {{ source('raw', 'customers') }}
),

deduplicated AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY LOWER(TRIM(CUSTOMER_ID))
        ORDER BY _LOADED_AT DESC
    ) = 1
),

cleaned AS (
    SELECT
        -- Primary Key
        LOWER(TRIM(CUSTOMER_ID))                        AS customer_id,

        -- Name
        INITCAP(TRIM(FIRST_NAME))                       AS first_name,
        INITCAP(TRIM(LAST_NAME))                        AS last_name,
        LOWER(TRIM(EMAIL))                              AS email,
        TRIM(PHONE)                                     AS phone,

        -- Demographics
        DATE_OF_BIRTH,
        DATEDIFF('year', DATE_OF_BIRTH, CURRENT_DATE()) AS age_years,
        CASE
            WHEN DATEDIFF('year', DATE_OF_BIRTH, CURRENT_DATE()) < 25
                THEN 'Gen Z (18-24)'
            WHEN DATEDIFF('year', DATE_OF_BIRTH, CURRENT_DATE()) < 35
                THEN 'Millennial (25-34)'
            WHEN DATEDIFF('year', DATE_OF_BIRTH, CURRENT_DATE()) < 45
                THEN 'Xennial (35-44)'
            WHEN DATEDIFF('year', DATE_OF_BIRTH, CURRENT_DATE()) < 55
                THEN 'Gen X (45-54)'
            ELSE 'Boomer+ (55+)'
        END                                             AS age_band,
        LOWER(TRIM(GENDER))                             AS gender,

        -- Dates
        SIGNUP_DATE,
        DATEDIFF('day', SIGNUP_DATE, CURRENT_DATE())    AS tenure_days,
        CASE
            WHEN DATEDIFF('day', SIGNUP_DATE, CURRENT_DATE()) <= 30
                THEN 'new'
            WHEN DATEDIFF('day', SIGNUP_DATE, CURRENT_DATE()) <= 180
                THEN 'developing'
            WHEN DATEDIFF('day', SIGNUP_DATE, CURRENT_DATE()) <= 365
                THEN 'established'
            ELSE 'loyal'
        END                                             AS tenure_band,

        -- Acquisition
        LOWER(TRIM(ACQUISITION_CHANNEL))                AS acquisition_channel,

        -- Loyalty
        LOWER(TRIM(LOYALTY_TIER))                       AS loyalty_tier,
        CASE LOWER(TRIM(LOYALTY_TIER))
            WHEN 'platinum' THEN 4
            WHEN 'gold'     THEN 3
            WHEN 'silver'   THEN 2
            ELSE 1
        END                                             AS loyalty_tier_rank,

        -- Geography
        UPPER(TRIM(REGION))                             AS region,
        INITCAP(TRIM(CITY))                             AS city,
        UPPER(TRIM(STATE))                              AS state,
        TRIM(POSTAL_CODE)                               AS postal_code,
        UPPER(TRIM(COUNTRY))                            AS country,

        -- Status
        COALESCE(IS_ACTIVE, TRUE)                       AS is_active,

        -- Metadata
        _SOURCE_FILE,
        _LOADED_AT

    FROM deduplicated
    WHERE
        CUSTOMER_ID IS NOT NULL
        AND EMAIL   IS NOT NULL
)

SELECT * FROM cleaned