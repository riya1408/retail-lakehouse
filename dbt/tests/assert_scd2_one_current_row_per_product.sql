-- SCD2 rule 1: every product has exactly one current version. Any row returned = failure.
select product_id, sum(case when is_current then 1 else 0 end) as current_rows
from {{ source('retail', 'dim_product_price') }}
group by product_id
having sum(case when is_current then 1 else 0 end) <> 1
