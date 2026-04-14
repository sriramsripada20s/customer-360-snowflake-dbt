WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

orders AS (
    SELECT
        order_id,
        customer_id
    FROM {{ ref('stg_orders') }}
    WHERE is_cancelled_or_returned = FALSE
),

browse_events AS (
    SELECT
        customer_id,
        category,
        COUNT(event_id)                             AS browse_count
    FROM {{ ref('stg_events') }}
    WHERE event_type    = 'product_view'
      AND category      IS NOT NULL
      AND customer_id   IS NOT NULL
    GROUP BY 1, 2
),

-- ── PURCHASE BEHAVIOR BY CATEGORY ────────────────────────────
purchase_by_category AS (
    SELECT
        o.customer_id,
        oi.category,
        COUNT(DISTINCT o.order_id)                  AS orders_in_category,
        SUM(oi.net_line_total)                      AS spend_in_category,
        SUM(oi.quantity)                            AS units_in_category,
        AVG(oi.unit_price)                          AS avg_unit_price_in_category,
        SUM(CASE WHEN oi.is_discounted
            THEN 1 ELSE 0 END)                      AS discounted_items_in_category
    FROM order_items oi
    INNER JOIN orders o
        ON oi.order_id = o.order_id
    WHERE oi.category IS NOT NULL
    GROUP BY 1, 2
),

-- ── TOTAL SPEND PER CUSTOMER ──────────────────────────────────
customer_total AS (
    SELECT
        customer_id,
        SUM(spend_in_category)                      AS total_spend,
        SUM(orders_in_category)                     AS total_category_orders,
        COUNT(DISTINCT category)                    AS total_distinct_categories
    FROM purchase_by_category
    GROUP BY 1
),

-- ── CATEGORY AFFINITY SCORE ───────────────────────────────────
-- Weighted: 70% purchase share + 30% browse share
category_affinity AS (
    SELECT
        p.customer_id,
        p.category,
        p.orders_in_category,
        p.spend_in_category,
        p.units_in_category,
        p.avg_unit_price_in_category,
        p.discounted_items_in_category,
        COALESCE(b.browse_count, 0)                 AS browse_count,

        -- Share of wallet for this category
        CASE WHEN t.total_spend > 0
            THEN p.spend_in_category / t.total_spend
            ELSE 0
        END                                         AS category_spend_share,

        -- Normalized browse score (capped at 1.0)
        LEAST(
            COALESCE(b.browse_count, 0)::FLOAT / 50,
            1.0
        )                                           AS browse_score,

        -- Combined affinity score
        (
            0.7 * CASE WHEN t.total_spend > 0
                       THEN p.spend_in_category / t.total_spend
                       ELSE 0 END
            +
            0.3 * LEAST(
                COALESCE(b.browse_count, 0)::FLOAT / 50,
                1.0
            )
        )                                           AS affinity_score

    FROM purchase_by_category p
    LEFT JOIN browse_events b
        ON  p.customer_id = b.customer_id
        AND p.category    = b.category
    LEFT JOIN customer_total t
        ON p.customer_id  = t.customer_id
),

-- ── RANK CATEGORIES PER CUSTOMER ─────────────────────────────
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY affinity_score DESC
        )                                           AS category_rank
    FROM category_affinity
),

-- ── PIVOT TOP 3 CATEGORIES PER CUSTOMER ──────────────────────
customer_affinity AS (
    SELECT
        customer_id,

        -- Top categories
        MAX(CASE WHEN category_rank = 1
            THEN category END)                      AS top_category_1,
        MAX(CASE WHEN category_rank = 2
            THEN category END)                      AS top_category_2,
        MAX(CASE WHEN category_rank = 3
            THEN category END)                      AS top_category_3,

        -- Affinity scores for top categories
        MAX(CASE WHEN category_rank = 1
            THEN affinity_score END)                AS top_category_1_score,
        MAX(CASE WHEN category_rank = 2
            THEN affinity_score END)                AS top_category_2_score,
        MAX(CASE WHEN category_rank = 3
            THEN affinity_score END)                AS top_category_3_score,

        -- Spend in top categories
        MAX(CASE WHEN category_rank = 1
            THEN spend_in_category END)             AS top_category_1_spend,
        MAX(CASE WHEN category_rank = 2
            THEN spend_in_category END)             AS top_category_2_spend,

        -- Category diversity metrics
        COUNT(DISTINCT category)                    AS distinct_categories_purchased,

        -- Category entropy
        -- High entropy = diverse buyer
        -- Low entropy = niche/specialist buyer
        -1 * SUM(
            CASE WHEN category_spend_share > 0
                THEN category_spend_share
                     * LN(category_spend_share)
                ELSE 0
            END
        )                                           AS category_entropy,

        -- Price sensitivity per category
        -- High discounted items ratio = price sensitive
        SUM(discounted_items_in_category)::FLOAT
            / NULLIF(SUM(units_in_category), 0)     AS overall_discount_item_ratio,

        -- Average price point preference
        AVG(avg_unit_price_in_category)             AS avg_price_point

    FROM ranked
    GROUP BY 1
)

SELECT * FROM customer_affinity