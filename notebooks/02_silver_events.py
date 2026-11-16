# Databricks notebook source
from pyspark.sql import functions as F
spark.conf.set("spark.sql.session.timeZone", "UTC")

bronze = spark.read.parquet("/Volumes/workspace/retail/landing/bronze/events")
raw_cols = ["event_time", "event_type", "product_id", "category_id", "category_code",
            "brand", "price", "user_id", "user_session"]
cat = F.split("category_code", r"\.")

silver = (bronze
    .dropDuplicates(raw_cols)                                                  # rule 1
    .withColumn("event_ts", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss 'UTC'"))  # rule 3
    .withColumn("brand", F.coalesce("brand", F.lit("unknown")))                # rule 2
    .withColumn("category", F.coalesce(cat[0], F.lit("unknown")))              # rule 4
    .withColumn("subcategory", F.coalesce(F.element_at(cat, -1), F.lit("unknown")))
    .withColumn("is_valid_price", F.col("price") > 0)                          # rule 5
    .select("event_ts", "event_date", "event_type", "user_id", "user_session", "product_id",
            "category_id", "category", "subcategory", "brand", "price", "is_valid_price",
            "_source_file"))

silver.write.format("delta").mode("overwrite").saveAsTable("workspace.retail.silver_events")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE workspace.retail.silver_events CLUSTER BY (event_date);
# MAGIC OPTIMIZE workspace.retail.silver_events;

# COMMAND ----------

s = spark.table("workspace.retail.silver_events")
print(f"rows: {s.count():,}   (expect 42,448,764 - 30,220 = 42,418,544)")
print(f"null timestamps: {s.filter('event_ts IS NULL').count()}   (expect 0)")
display(s.limit(5))