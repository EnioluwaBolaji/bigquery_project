-- models/customersegmentation.sql

with
    high_value_customers as (
        select
            u.id as user_id,
            concat(u.first_name, ' ', u.last_name) as name,
            sum(oi.sale_price * o.num_of_item) as total_spent
        from {{ ref("stgusers") }} u
        join {{ ref("stgorders") }} o on u.id = o.user_id
        join {{ ref("stgorderitems") }} oi on o.order_id = oi.order_id
        where o.status = 'Complete'
        group by u.id, name
        having sum(oi.sale_price * o.num_of_item) > 1000
    ),

    frequent_customers as (
        select
            u.id as user_id,
            concat(u.first_name, ' ', u.last_name) as name,
            count(distinct o.order_id) as number_of_orders
        from {{ ref("stgusers") }} u
        join {{ ref("stgorderitems") }} o on u.id = o.user_id
        where o.status = 'Complete'
        group by u.id, name
        having count(distinct o.order_id) > 2
    ),

    inactive_customers as (
    select
        u.id as user_id,
        concat(u.first_name, ' ', u.last_name) as name,
        null as number_of_orders
    from {{ ref("stgusers") }} u
    left join {{ ref("stgorders") }} o on u.id = o.user_id and o.status = 'Complete'
    where o.user_id is null
    group by u.id, name
),


    active_customers as (
        select
            u.id as user_id,
            concat(u.first_name, ' ', u.last_name) as name,
            count(distinct o.order_id) as number_of_orders
        from {{ ref("stgusers") }} u
        join {{ ref("stgorderitems") }} o on u.id = o.user_id
        where
            o.status = 'Complete'
            and date(o.created_at) >= date_sub(current_date(), interval 2 month)
        group by u.id, name
        having count(distinct o.order_id) >= 2
    )

select user_id, name, 'High-Value' as segment
from high_value_customers
union all
select user_id, name, 'Frequent Customers' as segment
from frequent_customers
union all
select user_id, name, 'Inactive' as segment
from inactive_customers
union all
select user_id, name, 'Active' as segment
from active_customers
