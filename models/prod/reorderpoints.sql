-- Calculate the total sold quantity for each product
with
    soldquantity as (
        select product_id, count(*) as total_sold
        from {{ ref("stgorderitems") }}
        where status = 'Complete'  -- Consider only delivered items
        group by 1
    ),

    -- Calculate the average daily sales for each product
    averagedailysales as (
        select product_id, avg(total_sold) as avg_daily_sales
        from
            (
                select
                    product_id,
                    count(*) as total_sold,
                    DATE_DIFF(delivered_at, created_at, DAY) AS days_to_deliver,
                from {{ ref("stgorderitems") }}
                where status = 'Complete'  -- Consider only delivered items
            ) as subquery
        group by 1
    ),

    -- Calculate the reorder point for each product
    reorderpoint as (
        select
            p.id as product_id,
            avg(s.avg_daily_sales * (p.retail_price - p.cost)) as reorder_point
        from {{ ref("stgproducts") }} p
        left join averagedailysales s on p.id = s.product_id
        group by 1
    ),

    -- Calculate current stock levels for each product in each distribution center
    currentstocklevels as (
        select i.product_distribution_center_id, i.product_id, count(*) as current_stock
        from {{ ref("stginventory") }} i
        where i.sold_at is null  -- Consider only unsold items
        group by 1, 2
    ),

    -- Join the reorder point and current stock levels to determine replenishment needs
    replenishmentneeds as (
        select
            r.product_id,
            r.reorder_point,
            c.product_distribution_center_id,
            c.current_stock,
            (r.reorder_point - c.current_stock) as replenishment_needed
        from reorderpoint r
        join currentstocklevels c on r.product_id = c.product_id
    )

-- Final query to suggest reorder points for distribution centers
select
    p.product_id,
    p.product_name,
    p.product_brand,
    p.product_department,
    p.product_category,
    r.product_distribution_center_id,
    r.current_stock,
    r.replenishment_needed
from replenishmentneeds r
join {{ ref("stgproducts") }} p on r.product_id = p.id
order by p.product_department, p.product_category, r.product_distribution_center_id
