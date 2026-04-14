WITH campaigns AS (
    SELECT * FROM {{ ref('stg_campaigns') }}
),

customers AS (
    SELECT
        customer_id,
        customer_sk
    FROM {{ ref('dim_customers') }}
)

SELECT
    -- Surrogate key
    MD5(c.campaign_response_id)                     AS campaign_response_sk,

    -- Natural keys
    c.campaign_response_id,
    c.customer_id,
    cu.customer_sk,
    c.campaign_id,

    -- Campaign details
    c.campaign_name,
    c.campaign_type,

    -- Engagement timestamps
    c.sent_at,
    c.opened_at,
    c.clicked_at,
    c.converted_at,
    c.unsubscribed_at,

    -- Date parts
    c.sent_date,
    c.sent_month,
    c.sent_year,

    -- Financials
    c.conversion_amount,

    -- Engagement flags
    c.is_opened,
    c.is_clicked,
    c.is_converted,
    c.is_unsubscribed,

    -- Engagement level
    c.engagement_level,

    -- Response speed
    c.hours_to_open,
    c.hours_to_click,
    c.hours_to_convert,

    -- Audit
    c._loaded_at

FROM campaigns c
LEFT JOIN customers cu
    ON c.customer_id = cu.customer_id