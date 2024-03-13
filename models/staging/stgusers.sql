SELECT id as user_id,
    concat(first_name, ' ', last_name) as username,
    email,
    age,
    gender,
    state,
    street_address,
    postal_code,
    city,
    country,
    latitude,
    longitude,
    traffic_source ,
    created_at  FROM `bigquery-public-data.thelook_ecommerce.users`  where id is not null 

