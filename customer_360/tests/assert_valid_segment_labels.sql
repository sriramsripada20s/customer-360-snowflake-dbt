-- Test : Segment thresholds seed must have exactly 6 segments
-- One row per segment_id 0-5
-- Fails if segments are missing or duplicated

SELECT
    segment_id,
    COUNT(*) AS row_count
FROM {{ ref('segment_thresholds') }}
GROUP BY 1
HAVING COUNT(*) > 1

UNION ALL

-- Also check all 6 segment IDs exist
SELECT
    expected.segment_id,
    0 AS row_count
FROM (
    SELECT 0 AS segment_id UNION ALL
    SELECT 1 UNION ALL
    SELECT 2 UNION ALL
    SELECT 3 UNION ALL
    SELECT 4 UNION ALL
    SELECT 5
) expected
LEFT JOIN {{ ref('segment_thresholds') }} st
    ON expected.segment_id = st.segment_id
WHERE st.segment_id IS NULL