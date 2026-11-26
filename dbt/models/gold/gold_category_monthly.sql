-- One row per category: October vs November purchases and growth.
select
    category,
    count_if(event_date <  '2019-11-01') as oct_purchases,
    count_if(event_date >= '2019-11-01') as nov_purchases,
    round((count_if(event_date >= '2019-11-01')
           / nullif(count_if(event_date < '2019-11-01'), 0) - 1) * 100, 1) as growth_pct
from {{ ref('fct_purchases') }}
group by category
