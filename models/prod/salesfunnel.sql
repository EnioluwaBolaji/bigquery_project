-- Count the number of events per user and session
WITH EventCounts AS (
    SELECT
        user_id,
        session_id,
        COUNT(*) AS event_count
    FROM
        {{ ref("stgevents") }}
    GROUP BY
        user_id, session_id
),

-- Identify the users who placed orders
OrderUsers AS (
    SELECT DISTINCT
        user_id
    FROM
        {{ ref("stgorders") }}
),

-- Count the number of orders per user
OrderCounts AS (
    SELECT
        user_id,
        COUNT(*) AS order_count
    FROM
        {{ ref("stgorders") }}
    GROUP BY
        user_id
),

-- Join event and order counts
FunnelData AS (
    SELECT
        ec.user_id,
        ec.session_id,
        ec.event_count,
        oc.order_count
    FROM
        EventCounts ec
    LEFT JOIN
        OrderCounts oc ON ec.user_id = oc.user_id
),

-- Calculate funnel stages
FunnelStages AS (
    SELECT
        user_id,
        CASE
            WHEN event_count > 0 THEN 'Visited Site'
            ELSE 'No Visit'
        END AS site_visit,
        CASE
            WHEN order_count > 0 THEN 'Ordered'
            ELSE 'No Order'
        END AS placed_order
    FROM
        FunnelData
),

-- Count users in each funnel stage
FunnelAnalysis AS (
    SELECT
        site_visit,
        placed_order,
        COUNT(*) AS user_count
    FROM
        FunnelStages
    GROUP BY
        site_visit, placed_order
)

-- Final funnel analysis results
SELECT
    site_visit,
    placed_order,
    user_count,
    ROUND(user_count * 100.0 / SUM(user_count) OVER (), 2) AS conversion_rate
FROM
    FunnelAnalysis
