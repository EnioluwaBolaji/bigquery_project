
        select p.id, p.name, p.category, sum(o.num_of_item) as total_sold
        from {{ ref("stgproducts") }} p
        join {{ ref("stgorderitems") }} oi on p.id = oi.product_id
        join {{ ref("stgorders") }} o on oi.order_id = o.order_id
        group by p.id, p.name, p.category
        order by total_sold desc
        limit 10
    