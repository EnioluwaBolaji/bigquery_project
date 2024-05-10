-- Extract the date part from the timestamp
WITH EventTimeSeries AS (
    SELECT
        DATE_TRUNC(created_at, DAY) AS event_date,
        COUNT(*) AS event_count
    FROM
        {{ ref("stgevents") }}
    GROUP BY
        event_date
),

-- Calculate moving averages for smoothing
MovingAverages AS (
    SELECT
        event_date,
        event_count,
        AVG(event_count) OVER (ORDER BY event_date ROWS BETWEEN 7 PRECEDING AND 7 FOLLOWING) AS moving_avg_7,
        AVG(event_count) OVER (ORDER BY event_date ROWS BETWEEN 30 PRECEDING AND 30 FOLLOWING) AS moving_avg_30
    FROM
        EventTimeSeries
)

-- Final time series analysis results
SELECT
    event_date,
    event_count,
    moving_avg_7,
    moving_avg_30
FROM
    MovingAverages
