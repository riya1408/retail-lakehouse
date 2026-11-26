{{ config(severity='warn') }}

-- Plausibility check: a day with real traffic must have purchases.
-- Catches tracking outages that row counts and schema checks miss.
-- Known incident: 2019-11-15 has 5.7M views and 0 purchases (purchase tracking outage in the
-- source data). It cannot be fixed upstream, so this test warns instead of blocking the build.
select event_date, views, purchases
from {{ ref('gold_daily_metrics') }}
where views > {{ var('min_daily_views', 100000) }}
  and purchases = 0