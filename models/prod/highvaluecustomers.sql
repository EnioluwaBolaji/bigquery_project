-- models/highvaluecustomers.sql

select
    u.id as user_id,
    CONCAT(u.first_name, ' ', u.last_name) as Name,
    sum(oi.sale_price * o.num_of_item) as Revenue
from {{ ref('stgusers') }} u
join {{ ref('stgorders') }} o on u.id = o.user_id
join {{ ref('stgorderitems') }} oi on o.order_id = oi.order_id
where o.status = 'Complete'
group by u.id, Name
having sum(oi.sale_price * o.num_of_item) > 1000
Order by Revenue DESC