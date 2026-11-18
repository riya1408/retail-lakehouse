# Databricks notebook source
from pyspark.sql import functions as F

silver = spark.table("workspace.retail.silver_events").filter("is_valid_price")
dim = spark.table("workspace.retail.dim_product_price")

prod_cat = silver.groupBy("product_id").agg(F.first("category_id").alias("category_id"))

daily = (silver.groupBy("product_id", "event_date")
    .agg(F.sum((F.col("event_type") == "view").cast("int")).alias("views"),
         F.sum((F.col("event_type") == "purchase").cast("int")).alias("purchases"))
    .join(prod_cat, "product_id"))

daily.write.format("delta").mode("overwrite").saveAsTable("workspace.retail.gold_product_daily")
print(f"{spark.table('workspace.retail.gold_product_daily').count():,} product-days")

# COMMAND ----------

daily = spark.table("workspace.retail.gold_product_daily")

stable = dim.groupBy("product_id").count().filter("count = 1").select("product_id")   # only 1 version all month
stable_daily = daily.join(stable, "product_id")

n_stable = stable_daily.groupBy("category_id").agg(F.countDistinct("product_id").alias("n_stable"))
control = (stable_daily.groupBy("category_id", "event_date")
    .agg(F.sum("purchases").alias("c_purchases"))
    .join(n_stable, "category_id")
    .filter("n_stable >= 5"))            # need enough neighbours to be a fair baseline

print(f"categories with a usable control group: {control.select('category_id').distinct().count():,}")

# COMMAND ----------

prev = dim.select("product_id", (F.col("version") + 1).alias("version"), F.col("valid_from").alias("old_from"))
cur  = dim.select("product_id", "version", F.col("valid_to").alias("new_to"))

changes = (spark.table("workspace.retail.gold_price_changes")
    .select("product_id", "version", "bucket", F.col("valid_from").alias("change_date"))
    .join(prod_cat, "product_id")
    .join(prev, ["product_id", "version"])
    .join(cur,  ["product_id", "version"])
    .withColumn("before_start", F.greatest("old_from", F.date_sub("change_date", 7)))
    .withColumn("before_end",   F.date_sub("change_date", 1))
    .withColumn("after_start",  F.col("change_date"))
    .withColumn("after_end",    F.least("new_to", F.date_add("change_date", 6),
                                        F.lit("2019-10-31").cast("date")))
    .withColumn("before_days",  F.datediff("before_end", "before_start") + 1)
    .withColumn("after_days",   F.datediff("after_end", "after_start") + 1))

# COMMAND ----------

def window_sums(left, right, key, value, prefix):
    j = left.alias("c").join(right.alias("d"),
            (F.col(f"c.{key}") == F.col(f"d.{key}")) &
            F.col("d.event_date").between(F.col("c.before_start"), F.col("c.after_end")), "left")
    return (j.groupBy("c.product_id", "c.version")
             .agg(F.sum(F.when(F.col("d.event_date") <  F.col("c.change_date"), F.col(f"d.{value}")).otherwise(0)).alias(f"{prefix}_before"),
                  F.sum(F.when(F.col("d.event_date") >= F.col("c.change_date"), F.col(f"d.{value}")).otherwise(0)).alias(f"{prefix}_after")))

treated  = window_sums(changes, daily,   "product_id",  "purchases",   "t")
neighbours = window_sums(changes, control, "category_id", "c_purchases", "c")

did = (changes.join(treated, ["product_id", "version"])
              .join(neighbours, ["product_id", "version"])
              .join(n_stable, "category_id")
              .filter("n_stable >= 5"))

did.write.format("delta").mode("overwrite").saveAsTable("workspace.retail.gold_price_effect_did")
print(f"{spark.table('workspace.retail.gold_price_effect_did').count():,} price changes with a control group")

# COMMAND ----------

d = spark.table("workspace.retail.gold_price_effect_did")
t_ratio = F.sum(F.col("t_after") / F.col("after_days")) / F.sum(F.col("t_before") / F.col("before_days"))
c_ratio = (F.sum(F.col("c_after") / (F.col("n_stable") * F.col("after_days"))) /
           F.sum(F.col("c_before") / (F.col("n_stable") * F.col("before_days"))))

display(d.groupBy("bucket")
         .agg(F.count("*").alias("price_changes"),
              F.round((t_ratio - 1) * 100, 1).alias("product_purchases_change_pct"),
              F.round((c_ratio - 1) * 100, 1).alias("neighbours_change_pct"),
              F.round(t_ratio / c_ratio, 2).alias("lift"))
         .orderBy("bucket"))