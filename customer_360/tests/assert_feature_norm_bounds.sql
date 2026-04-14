-- Test : All normalized ML features must be in [0,1] range
-- Out of bounds features break KMeans distance calculations
-- Severity: error — blocks ML scoring

SELECT
    customer_id,
    recency_norm,
    frequency_norm,
    monetary_norm,
    avg_order_value_norm,
    email_engagement_norm,
    session_activity_norm,
    conversion_propensity_norm,
    price_sensitivity_norm,
    category_diversity_norm,
    loyalty_norm
FROM {{ ref('feat_customer_segmentation') }}
WHERE
    recency_norm                NOT BETWEEN 0 AND 1
    OR frequency_norm           NOT BETWEEN 0 AND 1
    OR monetary_norm            NOT BETWEEN 0 AND 1
    OR avg_order_value_norm     NOT BETWEEN 0 AND 1
    OR email_engagement_norm    NOT BETWEEN 0 AND 1
    OR session_activity_norm    NOT BETWEEN 0 AND 1
    OR conversion_propensity_norm NOT BETWEEN 0 AND 1
    OR price_sensitivity_norm   NOT BETWEEN 0 AND 1
    OR category_diversity_norm  NOT BETWEEN 0 AND 1
    OR loyalty_norm             NOT BETWEEN 0 AND 1