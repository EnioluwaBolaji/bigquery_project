select distinct
    u.id as user_id,
    concat(u.first_name, ' ', u.last_name) as username,
    u.email,
    u.age,
    u.gender,
    u.state,
    u.street_address,
    u.postal_code,
    u.city,
    u.country,
    u.latitude as user_latitude,
    u.longitude as user_longitude,
    u.traffic_source as user_traffic_source,
    u.created_at as user_created_at,
    d.id as distribution_id,
    d.name as distribution_name,
    e.id as event_id,
    e.sequence_number,
    e.session_id,
    e.created_at as event_created_at,
    e.ip_address,
    e.city as event_city,
    e.state as event_state,
    e.postal_code as event_postal_code,
    e.browser,
    e.traffic_source as event_traffic_source,
    e.uri,
    e.event_type,
    i.id as inventory_id,
    i.product_id as inventory_product_id,
    i.created_at as inventory_created_at,
    i.cost as inventory_cost,
    i.product_category,
    i.product_name,
    i.product_brand,
    i.product_retail_price,
    i.product_department,
    i.product_sku,
    oi.id as order_item_id,
    oi.order_id as order_item_order_id,
    oi.status as order_item_status,
    oi.created_at as order_item_created_at,
    oi.shipped_at as order_item_shipped_at,
    oi.delivered_at as order_item_delivered_at,
    oi.returned_at as order_item_returned_at,
    oi.sale_price,
    o.order_id,
    o.status,
    o.created_at,
    o.num_of_item
from {{ ref("stgdistribution") }} d
 join {{ ref("stgproducts") }} p on d.id = p.distribution_center_id
 join {{ ref("stginventory") }} i on p.id = i.product_id
 join {{ ref("stgorderitems") }} oi on i.id = oi.inventory_item_id
 join {{ ref("stgusers") }} u on oi.user_id = u.id
 join {{ ref("stgorders") }} o on u.id = o.user_id
 join {{ ref("stgevents") }} e on o.order_id = e.id
  

