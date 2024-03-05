 select p.id, p.name as products, sum(o.num_of_item) as total_sold
        from {{ ref("stgproducts") }} p
        left join {{ ref("stgorderitems") }} oi on p.id = oi.product_id
        left join {{ ref("stgorders") }} o on oi.order_id = o.order_id
        where oi.order_id is null or o.status <> 'Complete'
        group by p.id, p.name
    
