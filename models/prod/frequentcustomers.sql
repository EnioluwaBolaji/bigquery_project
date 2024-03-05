-- models/frequentcustomers.sql

select
    u.id as user_id,
    CONCAT(u.first_name, ' ', u.last_name) as Name,
    count(distinct o.order_id) as number_of_orders
from {{ ref('stgusers') }} u
join {{ ref('stgorderitems') }} o on u.id = o.user_id
where o.status = 'Complete'
group by u.id, Name
having count(distinct o.order_id) > 2
Order by number_of_orders DESC
