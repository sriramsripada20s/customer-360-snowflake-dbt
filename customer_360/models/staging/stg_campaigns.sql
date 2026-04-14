WITH source AS (
    SELECT * FROM {{ source('raw', 'campaigns') }}
),

deduplicated AS (
    SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY LOWER(TRIM(CAMPAIGN_RESPONSE_ID))
        ORDER BY _LOADED_AT DESC
    ) = 1
),

cleaned AS (
    SELECT
        -- Primary Key
        LOWER(TRIM(CAMPAIGN_RESPONSE_ID))               AS campaign_response_id,

        -- Foreign Keys
        LOWER(TRIM(CUSTOMER_ID))                        AS customer_id,
        LOWER(TRIM(CAMPAIGN_ID))                        AS campaign_id,

        -- Campaign details
        TRIM(CAMPAIGN_NAME)                             AS campaign_name,
        LOWER(TRIM(CAMPAIGN_TYPE))                      AS campaign_type,

        -- Engagement timestamps
        SENT_AT,
        OPENED_AT,
        CLICKED_AT,
        CONVERTED_AT,
        UNSUBSCRIBED_AT,

        -- Conversion
        COALESCE(CONVERSION_AMOUNT, 0)                  AS conversion_amount,

        -- Derived engagement flags
        CASE
            WHEN OPENED_AT IS NOT NULL
            THEN TRUE ELSE FALSE
        END                                             AS is_opened,

        CASE
            WHEN CLICKED_AT IS NOT NULL
            THEN TRUE ELSE FALSE
        END                                             AS is_clicked,

        CASE
            WHEN CONVERTED_AT IS NOT NULL
            THEN TRUE ELSE FALSE
        END                                             AS is_converted,

        CASE
            WHEN UNSUBSCRIBED_AT IS NOT NULL
            THEN TRUE ELSE FALSE
        END                                             AS is_unsubscribed,

        -- Time to open (hours)
        CASE
            WHEN OPENED_AT IS NOT NULL
            THEN DATEDIFF('hour', SENT_AT, OPENED_AT)
            ELSE NULL
        END                                             AS hours_to_open,

        -- Time to click (hours)
        CASE
            WHEN CLICKED_AT IS NOT NULL
            THEN DATEDIFF('hour', SENT_AT, CLICKED_AT)
            ELSE NULL
        END                                             AS hours_to_click,

        -- Time to convert (hours)
        CASE
            WHEN CONVERTED_AT IS NOT NULL
            THEN DATEDIFF('hour', SENT_AT, CONVERTED_AT)
            ELSE NULL
        END                                             AS hours_to_convert,

        -- Engagement level
        CASE
            WHEN CONVERTED_AT    IS NOT NULL THEN 'converted'
            WHEN CLICKED_AT      IS NOT NULL THEN 'clicked'
            WHEN OPENED_AT       IS NOT NULL THEN 'opened'
            WHEN UNSUBSCRIBED_AT IS NOT NULL THEN 'unsubscribed'
            ELSE 'sent_only'
        END                                             AS engagement_level,

        -- Date parts for trend analysis
        DATE_TRUNC('day', SENT_AT)                      AS sent_date,
        MONTHNAME(SENT_AT)                              AS sent_month,
        YEAR(SENT_AT)                                   AS sent_year,

        -- Metadata
        _SOURCE_FILE,
        _LOADED_AT

    FROM deduplicated
    WHERE
        CAMPAIGN_RESPONSE_ID IS NOT NULL
        AND CUSTOMER_ID      IS NOT NULL
        AND SENT_AT          IS NOT NULL
)

SELECT * FROM cleaned