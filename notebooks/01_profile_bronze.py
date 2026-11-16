# Databricks notebook source
from pyspark.sql import functions as F

path = "/Volumes/workspace/retail/landing/bronze/events"
df = spark.read.parquet(path)

print(f"{df.count():,} rows")          # expect 42,448,764
df.printSchema()
display(df.limit(5))

# COMMAND ----------

display(df.groupBy("event_type").count().orderBy(F.desc("count")))

# COMMAND ----------

cols = ["category_code", "brand", "price", "user_session"]
display(df.select([F.round(F.avg(F.col(c).isNull().cast("int")) * 100, 2).alias(c) for c in cols]))

# COMMAND ----------

raw_cols = ["event_time", "event_type", "product_id", "category_id", "category_code",
            "brand", "price", "user_id", "user_session"]
total = df.count()
distinct = df.select(raw_cols).distinct().count()
print(f"Exact duplicates: {total - distinct:,}")


# COMMAND ----------

prices = (df.filter("event_type = 'view'")
            .groupBy("product_id")
            .agg(F.countDistinct("price").alias("n_prices")))
display(prices.groupBy((F.col("n_prices") > 1).alias("price_changed_in_oct")).count())

# COMMAND ----------

spread = (df.filter("event_type = 'view' AND price > 0")
            .groupBy("product_id")
            .agg(((F.max("price") / F.min("price") - 1) * 100).alias("pct")))
display(spread.filter("pct > 0").select(
    F.count("*").alias("changed"),
    F.sum((F.col("pct") < 1).cast("int")).alias("under_1pct"),
    F.sum(((F.col("pct") >= 1) & (F.col("pct") < 10)).cast("int")).alias("1_to_10pct"),
    F.sum((F.col("pct") >= 10).cast("int")).alias("over_10pct")))
display(df.select(F.sum((F.col("price") <= 0).cast("int")).alias("zero_or_negative_price")))