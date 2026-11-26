-- One row per day: traffic, carts, purchases and conversion.
select
    event_date,
    count_if(event_type = 'view')     as views,
    count_if(event_type = 'cart')     as carts,
    count_if(event_type = 'purchase') as purchases,
    round(count_if(event_type = 'purchase') * 100.0
          / nullif(count_if(event_type = 'view'), 0), 2) as conversion_pct
from {{ source('retail', 'silver_events') }}
group by event_date
