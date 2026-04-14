WITH source AS (
    SELECT * FROM {{ source('raw', 'clickstream') }}
),

deduplicated AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY LOWER(TRIM(EVENT_ID))
        ORDER BY _LOADED_AT DESC
    ) = 1
),

cleaned AS (
    SELECT
        -- Primary Key
        LOWER(TRIM(EVENT_ID))                           AS event_id,

        -- Session + User
        LOWER(TRIM(SESSION_ID))                         AS session_id,
        LOWER(TRIM(CUSTOMER_ID))                        AS customer_id,
        LOWER(TRIM(ANONYMOUS_ID))                       AS anonymous_id,

        -- Identity resolution
        -- Prefer authenticated customer_id over anonymous
        COALESCE(
            NULLIF(LOWER(TRIM(CUSTOMER_ID)), ''),
            LOWER(TRIM(ANONYMOUS_ID))
        )                                               AS resolved_user_id,

        CASE
            WHEN CUSTOMER_ID IS NOT NULL
            THEN 'authenticated'
            ELSE 'anonymous'
        END                                             AS identity_type,

        -- Dates
        EVENT_TIMESTAMP,
        DATE_TRUNC('day', EVENT_TIMESTAMP)              AS event_date,
        HOUR(EVENT_TIMESTAMP)                           AS event_hour,
        DAYOFWEEK(EVENT_TIMESTAMP)                      AS event_day_of_week,
        MONTHNAME(EVENT_TIMESTAMP)                      AS event_month,

        -- Time of day bucket
        CASE
            WHEN HOUR(EVENT_TIMESTAMP) BETWEEN 6  AND 11
                THEN 'morning'
            WHEN HOUR(EVENT_TIMESTAMP) BETWEEN 12 AND 17
                THEN 'afternoon'
            WHEN HOUR(EVENT_TIMESTAMP) BETWEEN 18 AND 22
                THEN 'evening'
            ELSE 'night'
        END                                             AS time_of_day,

        -- Weekend flag
        CASE
            WHEN DAYOFWEEK(EVENT_TIMESTAMP) IN (0, 6)
            THEN TRUE ELSE FALSE
        END                                             AS is_weekend,

        -- Event
        LOWER(TRIM(EVENT_TYPE))                         AS event_type,
        TRIM(PAGE_URL)                                  AS page_url,
        LOWER(TRIM(PRODUCT_ID))                         AS product_id,
        LOWER(TRIM(CATEGORY))                           AS category,
        LOWER(TRIM(SEARCH_TERM))                        AS search_term,

        -- Device + Channel
        LOWER(TRIM(DEVICE_TYPE))                        AS device_type,
        LOWER(TRIM(CHANNEL))                            AS channel,
        TRIM(REFERRER)                                  AS referrer,
        COALESCE(DWELL_TIME_SECONDS, 0)                 AS dwell_time_seconds,

        -- Derived event flags
        CASE
            WHEN LOWER(TRIM(EVENT_TYPE)) = 'product_view'
            THEN TRUE ELSE FALSE
        END                                             AS is_product_view,

        CASE
            WHEN LOWER(TRIM(EVENT_TYPE)) = 'add_to_cart'
            THEN TRUE ELSE FALSE
        END                                             AS is_add_to_cart,

        CASE
            WHEN LOWER(TRIM(EVENT_TYPE)) = 'purchase'
            THEN TRUE ELSE FALSE
        END                                             AS is_purchase,

        CASE
            WHEN LOWER(TRIM(EVENT_TYPE)) = 'search'
            THEN TRUE ELSE FALSE
        END                                             AS is_search,

        CASE
            WHEN LOWER(TRIM(EVENT_TYPE)) = 'checkout_start'
            THEN TRUE ELSE FALSE
        END                                             AS is_checkout_start,

        -- Metadata
        _SOURCE_FILE,
        _LOADED_AT

    FROM deduplicated
    WHERE
        EVENT_ID        IS NOT NULL
        AND EVENT_TIMESTAMP IS NOT NULL
)

SELECT * FROM cleaned