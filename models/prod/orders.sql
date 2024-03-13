select o.order_id, oi.id, oi.sale_price, o.num_of_item
from {{ ref("stgorderitems") }} oi
join {{ ref("stgorders") }} o on oi.order_id = o.order_id

