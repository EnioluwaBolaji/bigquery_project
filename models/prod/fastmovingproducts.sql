select
        product_id,
        count(*) as num_sold
    from {{ ref("stginventory") }}
    where sold_at is not null -- consider items that have been sold
    group by product_id
    order by num_sold desc