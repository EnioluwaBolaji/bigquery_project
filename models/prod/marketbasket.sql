-- Calculate item pairs frequency
WITH ItemPairs AS (
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
),

-- Rank item pairs by frequency
RankedItemPairs AS (
    SELECT
        item1,
        item2,
        frequency,
        RANK() OVER (ORDER BY frequency DESC) AS pair_rank
    FROM
        ItemPairs
),

-- Filter out low-frequency item pairs
FilteredItemPairs AS (
    SELECT
        item1,
        item2,
        frequency
    FROM
        RankedItemPairs
    WHERE
        pair_rank <= 10 -- Adjust the threshold based on your preference
),

-- Map product names to item IDs
ProductMapping AS (
    SELECT
        id AS product_id,
        name AS product_name
    FROM
        {{ ref("stgproducts") }}
),

-- Join item pairs with product names
ItemPairRecommendations AS (
    SELECT
        fp.item1,
        p1.product_name AS item1_name,
        fp.item2,
        p2.product_name AS item2_name,
        fp.frequency
    FROM
        FilteredItemPairs fp
    JOIN
        ProductMapping p1 ON fp.item1 = p1.product_id
    JOIN
        ProductMapping p2 ON fp.item2 = p2.product_id
)

-- Final market basket analysis results
SELECT
    item1_name,
    item2_name,
    frequency
FROM
    ItemPairRecommendations
ORDER BY
    frequency DESC
