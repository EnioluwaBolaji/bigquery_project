SELECT DISTINCT
    u.id AS user_id,
    CONCAT(u.first_name, ' ', u.last_name) AS name,
    u.email,
    u.age,
    u.gender,
    u.state,
    u.street_address,
    u.postal_code,
    u.city,
    u.country,
    u.latitude AS user_latitude,
    u.longitude AS user_longitude,
    u.traffic_source AS user_traffic_source,
    u.created_at AS user_created_at,
    d.id AS distribution_id,
    d.name AS distribution_name,
    e.id AS event_id,
    e.sequence_number,
    e.session_id,
    e.created_at AS event_created_at,
    e.ip_address,
    e.city AS event_city,
    e.state AS event_state,
    e.postal_code AS event_postal_code,
    e.browser,
    e.traffic_source AS event_traffic_source,
    e.URI,
    e.event_type,
    i.id AS inventory_id,
    i.product_id AS inventory_product_id,
    i.created_at AS inventory_created_at,
    i.sold_at,
    i.cost AS inventory_cost,
    i.product_category,
    i.product_name,
    i.product_brand,
    i.product_retail_price,
    i.product_department,
    i.product_sku,
    i.product_distribution_center_id,
    oi.id AS order_item_id,
    oi.order_id AS order_item_order_id,
    oi.user_id AS order_item_user_id,
    oi.product_id AS order_item_product_id,
    oi.inventory_item_id AS order_item_inventory_id,
    oi.status AS order_item_status,
    oi.created_at AS order_item_created_at,
    oi.shipped_at AS order_item_shipped_at,
    oi.delivered_at AS order_item_delivered_at,
    oi.returned_at AS order_item_returned_at,
    oi.sale_price,
    o.order_id AS order_id,
    o.user_id AS order_user_id,
    o.status AS order_status,
    o.gender AS order_gender,
    o.created_at AS order_created_at,
    o.num_of_item AS order_num_of_item,
    p.id AS product_id,
    p.cost AS product_cost
FROM 
    {{ ref("stgusers") }} u
LEFT JOIN 
    {{ ref("stgorders") }} o ON o.user_id = u.id
LEFT JOIN 
    {{ ref("stgorderitems") }} oi ON oi.user_id = u.id
LEFT JOIN 
    {{ ref("stginventory") }} i ON oi.inventory_item_id = i.id
LEFT JOIN 
    {{ ref("stgdistribution") }} d ON d.id = i.product_distribution_center_id
LEFT JOIN 
    {{ ref("stgevents") }} e ON e.postal_code = u.postal_code
LEFT JOIN 
    {{ ref("stgproducts") }} p ON p.id = i.product_id
