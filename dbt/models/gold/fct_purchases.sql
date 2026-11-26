-- One row per purchase, with the price version that was valid on that day (point-in-time join).
select
    e.event_ts,
    e.event_date,
    e.user_id,
    e.user_session,
    e.product_id,
    e.category,
    e.brand,
    e.price,
    p.price_key,
    p.version as price_version
from {{ source('retail', 'silver_events') }} as e
left join {{ source('retail', 'dim_product_price') }} as p
    on  e.product_id = p.product_id
    and e.event_date between p.valid_from and p.valid_to
where e.event_type = 'purchase'
  and e.is_valid_price
