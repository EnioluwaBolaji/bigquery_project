
    select
        product_id,
        product_distribution_center_id,
        count(*) as stock_count
    from {{ ref("stginventory") }}
    where sold_at is null -- only consider items that have not been sold
    group by 1,2
