# Databricks notebook source
from pyspark.sql import functions as F
spark.conf.set("spark.sql.session.timeZone", "UTC")

START, END = "2019-11-01", "2019-11-30"           # the slice this run is responsible for
silver_table = "workspace.retail.silver_events"

def rows_per_month():
    return (spark.table(silver_table)
            .groupBy(F.date_format("event_date", "yyyy-MM").alias("month")).count()
            .orderBy("month"))

display(rows_per_month())

# COMMAND ----------

raw_cols = ["event_time", "event_type", "product_id", "category_id", "category_code",
            "brand", "price", "user_id", "user_session"]

bronze_nov = (spark.read.parquet("/Volumes/workspace/retail/landing/bronze/events")
                   .filter(F.col("event_date").between(START, END)))   # reads only November's folders

cat = F.split("category_code", r"\.")
silver_nov = (bronze_nov
    .dropDuplicates(raw_cols)
    .withColumn("event_ts", F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss 'UTC'"))
    .withColumn("brand", F.coalesce("brand", F.lit("unknown")))
    .withColumn("category", F.coalesce(cat[0], F.lit("unknown")))
    .withColumn("subcategory", F.coalesce(F.element_at(cat, -1), F.lit("unknown")))
    .withColumn("is_valid_price", F.col("price") > 0)
    .select("event_ts", "event_date", "event_type", "user_id", "user_session", "product_id",
            "category_id", "category", "subcategory", "brand", "price", "is_valid_price",
            "_source_file"))

(silver_nov.write.format("delta").mode("overwrite")
    .option("replaceWhere", f"event_date >= '{START}' AND event_date <= '{END}'")
    .saveAsTable(silver_table))
print("November written")

# COMMAND ----------

display(rows_per_month())

nov_bronze   = bronze_nov.count()
nov_expected = bronze_nov.select(raw_cols).distinct().count()        # bronze minus duplicates
nov_silver   = spark.table(silver_table).filter(F.col("event_date").between(START, END)).count()
nov_nulls    = (spark.table(silver_table)
                .filter(F.col("event_date").between(START, END) & F.col("event_ts").isNull()).count())

print(f"November bronze rows:    {nov_bronze:,}")
print(f"November duplicates:     {nov_bronze - nov_expected:,}")
print(f"November silver rows:    {nov_silver:,}   (expect {nov_expected:,})")
print(f"November null timestamps: {nov_nulls}   (expect 0)")