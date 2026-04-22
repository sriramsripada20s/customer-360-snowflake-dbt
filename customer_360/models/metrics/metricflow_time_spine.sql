{{
  config(
    materialized = 'table'
  )
}}

WITH date_spine AS (
    SELECT
        DATEADD(DAY, SEQ4(), '2020-01-01'::DATE) AS date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 4018))
)

SELECT date_day
FROM date_spine
WHERE date_day <= '2030-12-31'::DATE