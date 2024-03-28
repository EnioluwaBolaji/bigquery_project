select o.order_id, p.name
from {{ ref("stgorderitems") }} o
join {{ ref("stgproducts") }} p on o.product_id= p.id

