select
            d.name as distribution_center,
            count(distinct p.id) as total_products,
            sum(o.num_of_item) as total_sold,
            sum(oi.sale_price * o.num_of_item) as total_revenue
        from {{ ref("stgproducts") }} p
        join {{ ref("stgorderitems") }} oi on p.id = oi.product_id
        join {{ ref("stgorders") }} o on oi.order_id = o.order_id
        join {{ ref("stgdistribution") }} d on p.distribution_center_id = d.id
        group by distribution_center