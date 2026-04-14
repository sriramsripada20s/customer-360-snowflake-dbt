{{ config(severity='warn') }}

SELECT
    order_id,
    customer_id,
    net_revenue,
    order_status
FROM {{ ref('fct_orders') }}
WHERE net_revenue < -500
  AND is_cancelled_or_returned = FALSE