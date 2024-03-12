
    SELECT id,
sequence_number,
session_id,
created_at,
ip_address,
city,
state,
postal_code,
browser,
traffic_source,
uri,
event_type
    FROM `bigquery-public-data.thelook_ecommerce.events`
    WHERE id IS NOT NULL



