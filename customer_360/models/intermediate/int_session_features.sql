WITH events AS (
    SELECT * FROM {{ ref('stg_events') }}
    WHERE identity_type = 'authenticated'
      AND customer_id IS NOT NULL
),

-- ── SESSION LEVEL AGGREGATIONS ────────────────────────────────
-- Roll up events to session level first
-- then aggregate sessions to customer level
session_level AS (
    SELECT
        customer_id,
        session_id,

        -- Session timing
        MIN(event_timestamp)                        AS session_start,
        MAX(event_timestamp)                        AS session_end,
        DATEDIFF('second',
            MIN(event_timestamp),
            MAX(event_timestamp))                   AS session_duration_seconds,

        -- Event counts per session
        COUNT(event_id)                             AS events_in_session,
        SUM(is_product_view::INT)                   AS product_views,
        SUM(is_add_to_cart::INT)                    AS add_to_cart_events,
        SUM(is_purchase::INT)                       AS purchase_events,
        SUM(is_search::INT)                         AS search_events,
        SUM(is_checkout_start::INT)                 AS checkout_starts,
        SUM(dwell_time_seconds)                     AS total_dwell_seconds,

        -- Session attributes
        MODE(device_type)                           AS device_type,
        MODE(channel)                               AS channel,
        MODE(category)                              AS top_category,
        MIN(event_hour)                             AS session_start_hour,
        MAX(is_weekend::INT)                        AS is_weekend_session,
        MODE(time_of_day)                           AS time_of_day,

        -- Cart abandon flag
        -- Added to cart but did not purchase in same session
        CASE
            WHEN SUM(is_add_to_cart::INT) > 0
             AND SUM(is_purchase::INT) = 0
            THEN 1 ELSE 0
        END                                         AS is_cart_abandoned

    FROM events
    GROUP BY 1, 2
),

-- ── CUSTOMER LEVEL AGGREGATIONS ───────────────────────────────
customer_sessions AS (
    SELECT
        customer_id,

        -- ── SESSION VOLUME ────────────────────────────────────
        COUNT(DISTINCT session_id)                  AS total_sessions,

        COUNT(DISTINCT CASE
            WHEN session_start >= DATEADD('day', -30, CURRENT_DATE())
            THEN session_id END)                    AS sessions_last_30d,

        COUNT(DISTINCT CASE
            WHEN session_start >= DATEADD('day', -7, CURRENT_DATE())
            THEN session_id END)                    AS sessions_last_7d,

        -- ── ENGAGEMENT DEPTH ──────────────────────────────────
        AVG(session_duration_seconds)               AS avg_session_duration_seconds,
        MAX(session_duration_seconds)               AS max_session_duration_seconds,
        AVG(events_in_session)                      AS avg_events_per_session,
        AVG(product_views)                          AS avg_product_views_per_session,
        SUM(product_views)                          AS total_product_views,
        SUM(search_events)                          AS total_searches,
        SUM(add_to_cart_events)                     AS total_add_to_cart,
        AVG(total_dwell_seconds)                    AS avg_dwell_seconds_per_session,

        -- ── CART ABANDONMENT ──────────────────────────────────
        SUM(is_cart_abandoned)                      AS total_cart_abandons,

        CASE WHEN COUNT(session_id) > 0
            THEN SUM(is_cart_abandoned)::FLOAT
                 / COUNT(session_id)
            ELSE 0
        END                                         AS cart_abandon_rate,

        -- ── CONVERSION ────────────────────────────────────────
        SUM(purchase_events)                        AS total_purchases_from_sessions,

        CASE WHEN SUM(add_to_cart_events) > 0
            THEN SUM(purchase_events)::FLOAT
                 / SUM(add_to_cart_events)
            ELSE 0
        END                                         AS cart_to_purchase_rate,

        -- ── DEVICE PREFERENCE ─────────────────────────────────
        MODE(device_type)                           AS preferred_device,
        MODE(channel)                               AS preferred_channel,

        -- ── TIME PREFERENCE ───────────────────────────────────
        MODE(time_of_day)                           AS time_of_day_preference,

        CASE WHEN AVG(is_weekend_session) > 0.5
            THEN 'weekend'
            ELSE 'weekday'
        END                                         AS weekday_preference,

        -- ── CATEGORY BROWSE PREFERENCE ────────────────────────
        MODE(top_category)                          AS most_browsed_category,

        -- ── RECENCY ───────────────────────────────────────────
        MAX(session_start)                          AS last_session_date,

        DATEDIFF('day',
            MAX(session_start),
            CURRENT_DATE())                         AS days_since_last_session

    FROM session_level
    GROUP BY 1
)

SELECT * FROM customer_sessions