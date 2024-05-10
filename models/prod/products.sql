select  p.name, p.category, p.id,i.product_name
from {{ ref("stginventory") }} i
join {{ ref("stgproducts") }} p on i.product_id= p.id

