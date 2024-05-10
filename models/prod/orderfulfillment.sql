-- Calculate order fulfillment time
WITH OrderFulfillmentTime AS (
    SELECT
        order_id,
        created_at AS order_created_at,
        delivered_at AS order_delivered_at,
        TIMESTAMP_DIFF(delivered_at, created_at, HOUR) AS fulfillment_time_hours
    FROM
        {{ ref("stgorders") }}
    WHERE
        delivered_at IS NOT NULL -- Exclude orders that haven't been delivered yet
)

-- Analyze order fulfillment time
SELECT
    order_id,
    order_created_at,
    order_delivered_at,
    fulfillment_time_hours
FROM
    OrderFulfillmentTime
ORDER BY
    fulfillment_time_hours DESC -- Order by fulfillment time to identify bottlenecks
