{% snapshot snap_customer_profile %}

{{
    config(
        target_schema  = 'CORE',
        target_database = 'CUSTOMER_PLATFORM',
        unique_key     = 'customer_id',
        strategy       = 'check',
        check_cols     = [
            'loyalty_tier',
            'is_active',
            'acquisition_channel',
            'region',
            'city',
            'state'
        ],
        invalidate_hard_deletes = True
    )
}}

SELECT
    customer_id,
    email,
    first_name,
    last_name,
    loyalty_tier,
    loyalty_tier_rank,
    is_active,
    acquisition_channel,
    region,
    city,
    state,
    country,
    tenure_band,
    signup_date,
    _loaded_at

FROM {{ ref('stg_customers') }}

{% endsnapshot %}