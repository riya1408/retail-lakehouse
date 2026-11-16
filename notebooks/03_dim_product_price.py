# Databricks notebook source
from pyspark.sql import functions as F, Window as W

silver = spark.table("workspace.retail.silver_events")

daily = (silver.filter("is_valid_price")
         .groupBy("product_id", "event_date")
         .agg(F.max_by("price", "event_ts").alias("price")))   # price of the LAST event that day

print(f"product-days: {daily.count():,}")

# COMMAND ----------

w = W.partitionBy("product_id").orderBy("event_date")

changes = (daily
    .withColumn("prev_price", F.lag("price").over(w))          # yesterday's price for this product
    .filter(F.col("prev_price").isNull() |                     # first day we saw it = version 1
            (F.abs(F.col("price") / F.col("prev_price") - 1) >= 0.01)))   # moved >= 1%

print(f"price versions: {changes.count():,}")

# COMMAND ----------

dim = (changes
    .withColumn("valid_from", F.col("event_date"))
    .withColumn("next_from", F.lead("valid_from").over(w))     # when the NEXT version starts
    .withColumn("valid_to", F.coalesce(F.date_sub("next_from", 1),
                                       F.lit("9999-12-31").cast("date")))
    .withColumn("is_current", F.col("next_from").isNull())
    .withColumn("version", F.row_number().over(w))
    .withColumn("price_key", F.xxhash64("product_id", "valid_from"))   # surrogate key
    .select("price_key", "product_id", "version", "price", "valid_from", "valid_to", "is_current"))

dim.write.format("delta").mode("overwrite").saveAsTable("workspace.retail.dim_product_price")

# COMMAND ----------

from pyspark.sql import functions as F, Window as W

d = spark.table("workspace.retail.dim_product_price")
print(f"versions: {d.count():,}   products: {d.select('product_id').distinct().count():,}")

# Rule 1: exactly one current row per product
not_one_current = (d.groupBy("product_id")
                    .agg(F.sum(F.col("is_current").cast("int")).alias("n"))
                    .filter("n != 1").count())
print(f"products without exactly 1 current row: {not_one_current}   (expect 0)")

# Rule 2: every version has a unique key
dup_keys = d.groupBy("price_key").count().filter("count > 1").count()
print(f"duplicate price_keys: {dup_keys}   (expect 0)")

# Rule 3: no gaps or overlaps (each version ends the day before the next one starts)
w2 = W.partitionBy("product_id").orderBy("valid_from")
broken = (d.withColumn("next_from", F.lead("valid_from").over(w2))
           .filter("next_from IS NOT NULL AND valid_to != date_sub(next_from, 1)").count())
print(f"gaps or overlaps: {broken}   (expect 0)")

# Look at the product whose price changed most often
top = d.groupBy("product_id").count().orderBy(F.desc("count")).first()["product_id"]
display(d.filter(F.col("product_id") == top).orderBy("version"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)           AS purchase_rows_after_join,
# MAGIC        count(p.price_key) AS matched_to_a_version
# MAGIC FROM workspace.retail.silver_events e
# MAGIC LEFT JOIN workspace.retail.dim_product_price p
# MAGIC   ON  e.product_id = p.product_id
# MAGIC   AND e.event_date BETWEEN p.valid_from AND p.valid_to
# MAGIC WHERE e.event_type = 'purchase' AND e.is_valid_price