WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
)

SELECT
    -- Surrogate key
    MD5(customer_id)                                AS customer_sk,

    -- Natural key
    customer_id,

    -- Identity
    first_name,
    last_name,
    first_name || ' ' || last_name                  AS full_name,
    email,
    phone,

    -- Demographics
    date_of_birth,
    age_years,
    age_band,
    gender,

    -- Acquisition
    signup_date,
    tenure_days,
    tenure_band,
    acquisition_channel,

    -- Loyalty
    loyalty_tier,
    loyalty_tier_rank,

    -- Geography
    region,
    city,
    state,
    postal_code,
    country,

    -- Status
    is_active,

    -- Audit
    _loaded_at                                      AS last_updated_at

FROM customers