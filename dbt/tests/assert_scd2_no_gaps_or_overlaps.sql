-- SCD2 rule 2: each version ends exactly the day before the next one starts.
select *
from (
    select
        product_id,
        valid_to,
        lead(valid_from) over (partition by product_id order by valid_from) as next_from
    from {{ source('retail', 'dim_product_price') }}
) as v
where next_from is not null
  and valid_to <> cast(next_from - interval 1 day as date)
