SELECT
        oi1.product_id AS item1,
        oi2.product_id AS item2,
        COUNT(DISTINCT oi1.order_id) AS frequency
    FROM
        {{ ref("stgorderitems") }} oi1
    JOIN
        {{ ref("stgorderitems") }} oi2 ON oi1.order_id = oi2.order_id
        AND oi1.product_id < oi2.product_id -- Avoid counting pairs twice
    GROUP BY
        oi1.product_id, oi2.product_id