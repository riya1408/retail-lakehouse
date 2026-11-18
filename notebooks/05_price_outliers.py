# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW price_moves AS
# MAGIC SELECT product_id, version, valid_from, valid_to,
# MAGIC        price AS new_price,
# MAGIC        LAG(price)  OVER (PARTITION BY product_id ORDER BY valid_from) AS old_price,
# MAGIC        LEAD(price) OVER (PARTITION BY product_id ORDER BY valid_from) AS next_price
# MAGIC FROM workspace.retail.dim_product_price;
# MAGIC
# MAGIC SELECT count_if(new_price / old_price - 1 <= -0.10) AS drops_10pct_plus,
# MAGIC        count_if(new_price / old_price - 1 <= -0.50) AS drops_50pct_plus,
# MAGIC        count_if(new_price / old_price - 1 <= -0.80) AS drops_80pct_plus
# MAGIC FROM price_moves

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT new_price, count(DISTINCT product_id) AS products
# MAGIC FROM price_moves
# MAGIC WHERE new_price / old_price - 1 <= -0.80
# MAGIC GROUP BY new_price
# MAGIC ORDER BY products DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CASE WHEN new_price / old_price - 1 <= -0.80 THEN '1. 80%+'
# MAGIC             WHEN new_price / old_price - 1 <= -0.50 THEN '2. 50-80%'
# MAGIC             ELSE '3. 10-50%' END AS drop_size,
# MAGIC        count(*) AS drops,
# MAGIC        round(avg(CASE WHEN next_price >= old_price * 0.9 THEN 1.0 ELSE 0 END) * 100, 1) AS pct_bounced_back,
# MAGIC        round(avg(datediff(least(valid_to, DATE'2019-10-31'), valid_from) + 1), 1) AS avg_days_at_new_price
# MAGIC FROM price_moves
# MAGIC WHERE new_price / old_price - 1 <= -0.10
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1