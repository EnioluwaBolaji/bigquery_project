select o.order_id, p.name, p.category, p.id
from {{ ref("stgorderitems") }} o
join {{ ref("stgproducts") }} p on o.product_id= p.id

