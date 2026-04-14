-- Test : No duplicate customer_id in dim_customers
-- Fails if any customer_id appears more than once
-- Severity: error — blocks pipeline

SELECT
    customer_id,
    COUNT(*) AS duplicate_count
FROM {{ ref('dim_customers') }}
GROUP BY 1
HAVING COUNT(*) > 1