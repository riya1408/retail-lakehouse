# Databricks notebook source
from pyspark.sql import functions as F
SILVER, DIM = "workspace.retail.silver_events", "workspace.retail.dim_product_price"

nov = spark.table(SILVER).filter(F.col("event_date").between("2019-11-01", "2019-11-30"))
daily = (nov.groupBy("event_date")
         .agg(F.count_if(F.col("event_type") == "view").alias("views"),
              F.count_if(F.col("event_type") == "purchase").alias("purchases"))
         .withColumn("conversion_pct", F.round(F.col("purchases") / F.col("views") * 100, 2))
         .orderBy("event_date"))
display(daily)

# COMMAND ----------

dim = spark.table(DIM)

def price_on(day, name):
    """The price each product had on a given day: the version valid that day (point-in-time)."""
    return (dim.filter((F.col("valid_from") <= day) & (F.col("valid_to") >= day))
               .select("product_id", F.col("price").alias(name)))

change = F.col("price_bf") / F.col("price_before") - 1
prices = (price_on("2019-11-22", "price_before").join(price_on("2019-11-29", "price_bf"), "product_id")
          .withColumn("bucket", F.when(change <= -0.10, "1. dropped 10%+")
                                 .when(change <= -0.01, "2. dropped 1-10%")
                                 .when(change <  0.01,  "3. unchanged")
                                 .otherwise(            "4. rose")))
display(prices.groupBy("bucket").count().orderBy("bucket"))

# COMMAND ----------

per_product = (nov.filter(F.col("event_type") == "purchase")
    .groupBy("product_id")
    .agg((F.count_if(F.col("event_date").between("2019-11-22", "2019-11-28")) / 7).alias("baseline_per_day"),  # clean week before BF
         F.count_if(F.col("event_date") == "2019-11-29").alias("bf_purchases")))           # Black Friday

result = (prices.join(per_product, "product_id")
          .groupBy("bucket")
          .agg(F.count("*").alias("products"),
               F.round(F.sum("baseline_per_day"), 0).alias("normal_day_purchases"),
               F.sum("bf_purchases").alias("black_friday_purchases"))
          .withColumn("lift", F.round(F.col("black_friday_purchases") / F.col("normal_day_purchases"), 2))
          .orderBy("bucket"))
display(result)