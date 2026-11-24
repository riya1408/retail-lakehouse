# Databricks notebook source
from pyspark.sql import functions as F, Window as W
spark.conf.set("spark.sql.session.timeZone", "UTC")

DIM, SILVER = "workspace.retail.dim_product_price", "workspace.retail.silver_events"
START, END, THRESHOLD = "2019-11-01", "2019-11-30", 0.01

dim = spark.table(DIM)
version_before = spark.sql(f"DESCRIBE HISTORY {DIM} LIMIT 1").first()["version"]
print(f"dim rows before: {dim.count():,}   (Delta version {version_before})")
print(f"rows already starting in November: {dim.filter(F.col('valid_from') >= START).count()}   (expect 0)")

# COMMAND ----------

# November: one price per product per day (last valid price that day)
nov_daily = (spark.table(SILVER)
    .filter(F.col("event_date").between(START, END) & F.col("is_valid_price"))
    .groupBy("product_id", "event_date")
    .agg(F.max_by("price", "event_ts").alias("price")))

# Each product's CURRENT price (from October) acts as "yesterday" for its first November day
current = (dim.filter("is_current")
    .select("product_id", F.col("valid_from").alias("event_date"), "price",
            F.col("version").alias("last_version"), F.lit(True).alias("is_seed")))

w = W.partitionBy("product_id").orderBy("event_date")
timeline = (nov_daily.withColumn("last_version", F.lit(None).cast("int"))
                     .withColumn("is_seed", F.lit(False))
            .unionByName(current)
            .withColumn("prev_price", F.lag("price").over(w))
            .withColumn("last_version", F.max("last_version").over(W.partitionBy("product_id"))))

new_versions = (timeline
    .filter(~F.col("is_seed"))
    .filter(F.col("prev_price").isNull() |
            (F.abs(F.col("price") / F.col("prev_price") - 1) >= THRESHOLD))
    .withColumn("valid_from", F.col("event_date"))
    .withColumn("next_from", F.lead("valid_from").over(w))
    .withColumn("valid_to", F.coalesce(F.date_sub("next_from", 1), F.lit("9999-12-31").cast("date")))
    .withColumn("is_current", F.col("next_from").isNull())
    .withColumn("version", F.coalesce("last_version", F.lit(0)) + F.row_number().over(w))
    .withColumn("price_key", F.xxhash64("product_id", "valid_from"))
    .withColumn("existing_product", F.col("last_version").isNotNull())
    .select("price_key", "product_id", "version", "price", "valid_from", "valid_to",
            "is_current", "existing_product"))

# COMMAND ----------

# Copy 1 (real key): closes the October version, ending the day before the first November change
closes = (new_versions.filter("existing_product")
    .groupBy("product_id").agg(F.min("valid_from").alias("first_change"))
    .select(F.col("product_id").alias("merge_key"), "product_id",
            F.date_sub("first_change", 1).alias("close_to")))

# Copy 2 (NULL key): every new version gets inserted
inserts = (new_versions.drop("existing_product")
    .select(F.lit(None).cast("long").alias("merge_key"), "*"))

staged = inserts.unionByName(closes, allowMissingColumns=True)
staged.createOrReplaceTempView("staged")

print(f"October versions to close (copy 1, real key): {closes.count():,}")
print(f"new versions to insert   (copy 2, NULL key):  {inserts.count():,}")
print(f"  of which brand-new products:                {new_versions.filter('NOT existing_product').select('product_id').distinct().count():,}")
display(staged.filter(F.col("product_id") == 3200323).orderBy(F.col("merge_key").isNull(), "valid_from"))

# COMMAND ----------

already = spark.table(DIM).filter(F.col("valid_from") >= START).count()
if already > 0:
    print(f"November already merged ({already:,} rows) - skipping. "
          f"To redo: RESTORE TABLE {DIM} TO VERSION AS OF {version_before}")
else:
    spark.sql(f"""
        MERGE INTO {DIM} t
        USING staged s
        ON t.product_id = s.merge_key AND t.is_current = true
        WHEN MATCHED THEN
          UPDATE SET t.is_current = false, t.valid_to = s.close_to
        WHEN NOT MATCHED THEN
          INSERT (price_key, product_id, version, price, valid_from, valid_to, is_current)
          VALUES (s.price_key, s.product_id, s.version, s.price, s.valid_from, s.valid_to, s.is_current)
    """)
    print("MERGE done")

display(spark.sql(f"DESCRIBE HISTORY {DIM} LIMIT 2").select("version", "operation", "operationMetrics"))

# COMMAND ----------

d = spark.table(DIM)
print(f"dim rows after: {d.count():,}")

w2 = W.partitionBy("product_id").orderBy("valid_from")
not_one_current = (d.groupBy("product_id").agg(F.sum(F.col("is_current").cast("int")).alias("n"))
                    .filter("n != 1").count())
dup_keys = d.groupBy("price_key").count().filter("count > 1").count()
broken = (d.withColumn("next_from", F.lead("valid_from").over(w2))
           .filter("next_from IS NOT NULL AND valid_to != date_sub(next_from, 1)").count())
print(f"not exactly 1 current row: {not_one_current}   duplicate keys: {dup_keys}   gaps/overlaps: {broken}   (all expect 0)")

pit = spark.sql(f"""
    SELECT count(*) AS purchase_rows_after_join, count(p.price_key) AS matched
    FROM {SILVER} e
    LEFT JOIN {DIM} p
      ON e.product_id = p.product_id AND e.event_date BETWEEN p.valid_from AND p.valid_to
    WHERE e.event_type = 'purchase' AND e.is_valid_price""")
display(pit)

display(d.filter((F.col("product_id") == 3200323) & (F.col("version") >= 18)).orderBy("version"))