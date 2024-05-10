SELECT
        traffic_source,
        COUNT(DISTINCT user_id) AS num_users_acquired
    FROM
        {{ ref("stgevents") }}
    GROUP BY
        traffic_source