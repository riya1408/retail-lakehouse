# Databricks notebook source
from pyspark.sql import functions as F

e = spark.table("workspace.retail.silver_events").filter("is_valid_price").alias("e")
p = spark.table("workspace.retail.dim_product_price").alias("p")

joined = e.join(p, (F.col("e.product_id") == F.col("p.product_id")) &
                   F.col("e.event_date").between(F.col("p.valid_from"), F.col("p.valid_to")))

version_stats = (joined
    .groupBy("p.price_key", "p.product_id", "p.version", "p.price", "p.valid_from", "p.valid_to")
    .agg(F.sum((F.col("e.event_type") == "view").cast("int")).alias("views"),
         F.sum((F.col("e.event_type") == "cart").cast("int")).alias("carts"),
         F.sum((F.col("e.event_type") == "purchase").cast("int")).alias("purchases"),
         F.countDistinct("e.event_date").alias("active_days")))

version_stats.write.format("delta").mode("overwrite").saveAsTable("workspace.retail.gold_product_version_stats")
print(f"{spark.table('workspace.retail.gold_product_version_stats').count():,} versions with activity")

# COMMAND ----------

v = spark.table("workspace.retail.gold_product_version_stats")

prev = v.select("product_id", (F.col("version") + 1).alias("version"),
                F.col("price").alias("old_price"), F.col("views").alias("old_views"),
                F.col("purchases").alias("old_purchases"))

pct = (F.col("price") / F.col("old_price") - 1) * 100
pairs = (v.join(prev, ["product_id", "version"])                  # version N next to N-1
    .filter("old_views >= 50 AND views >= 50")                    # enough traffic to trust
    .withColumn("price_change_pct", F.round(pct, 2))
    .withColumn("bucket", F.when(pct <= -10, "1. drop 10%+")
                           .when(pct < 0,    "2. drop 1-10%")
                           .when(pct < 10,   "3. rise 1-10%")
                           .otherwise(       "4. rise 10%+")))

pairs.write.format("delta").mode("overwrite").saveAsTable("workspace.retail.gold_price_changes")
print(f"{pairs.count():,} price changes with enough traffic")

# COMMAND ----------

display(spark.table("workspace.retail.gold_price_changes")
    .groupBy("bucket")
    .agg(F.count("*").alias("price_changes"),
         F.round(F.sum("old_purchases") / F.sum("old_views") * 100, 2).alias("conversion_before_pct"),
         F.round(F.sum("purchases") / F.sum("views") * 100, 2).alias("conversion_after_pct"))
    .orderBy("bucket"))