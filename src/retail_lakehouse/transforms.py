"""Transformation rules for the retail lakehouse.

Every function takes DataFrames in and returns a DataFrame out. Nothing here reads or
writes tables: notebooks and jobs do the I/O, this module holds the rules. That split is
what makes the rules unit-testable on tiny hand-made DataFrames.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

RAW_COLS = ["event_time", "event_type", "product_id", "category_id", "category_code",
            "brand", "price", "user_id", "user_session"]
PRICE_CHANGE_THRESHOLD = 0.01          # a move of 1% or more is a real price change
OPEN_END = "9999-12-31"                # valid_to of the current version

SILVER_COLS = ["event_ts", "event_date", "event_type", "user_id", "user_session", "product_id",
               "category_id", "category", "subcategory", "brand", "price", "is_valid_price",
               "_source_file"]


def clean_events(bronze: DataFrame) -> DataFrame:
    """Bronze -> silver: the 5 cleaning rules found while profiling October."""
    cat = F.split("category_code", r"\.")
    return (bronze
            .dropDuplicates(RAW_COLS)                                              # 1. exact duplicates
            .withColumn("event_ts",                                                # 2. text -> timestamp
                        F.to_timestamp("event_time", "yyyy-MM-dd HH:mm:ss 'UTC'"))
            .withColumn("brand", F.coalesce("brand", F.lit("unknown")))            # 3. keep rows, mark gaps
            .withColumn("category", F.coalesce(cat[0], F.lit("unknown")))          # 4. split category
            .withColumn("subcategory", F.coalesce(F.element_at(cat, -1), F.lit("unknown")))
            .withColumn("is_valid_price", F.col("price") > 0)                      # 5. flag zero prices
            .select(*SILVER_COLS))


def daily_prices(silver: DataFrame) -> DataFrame:
    """One price per product per day: the last valid price seen that day."""
    return (silver.filter("is_valid_price")
            .groupBy("product_id", "event_date")
            .agg(F.max_by("price", "event_ts").alias("price")))


def build_price_scd2(daily: DataFrame, threshold: float = PRICE_CHANGE_THRESHOLD) -> DataFrame:
    """Daily prices -> SCD2 price history. A new version starts only when the price moves
    by `threshold` or more compared with the previous day's price."""
    w = Window.partitionBy("product_id").orderBy("event_date")
    changes = (daily
               .withColumn("prev_price", F.lag("price").over(w))
               .filter(F.col("prev_price").isNull() |
                       (F.abs(F.col("price") / F.col("prev_price") - 1) >= threshold)))
    return (changes
            .withColumn("valid_from", F.col("event_date"))
            .withColumn("next_from", F.lead("valid_from").over(w))
            .withColumn("valid_to", F.coalesce(F.date_sub("next_from", 1),
                                               F.lit(OPEN_END).cast("date")))
            .withColumn("is_current", F.col("next_from").isNull())
            .withColumn("version", F.row_number().over(w))
            .withColumn("price_key", F.xxhash64("product_id", "valid_from"))
            .select("price_key", "product_id", "version", "price", "valid_from", "valid_to",
                    "is_current"))


def scd2_violations(dim: DataFrame) -> dict:
    """The three rules every SCD2 table must obey. Returns counts; all must be 0."""
    w = Window.partitionBy("product_id").orderBy("valid_from")
    not_one_current = (dim.groupBy("product_id")
                       .agg(F.sum(F.col("is_current").cast("int")).alias("n"))
                       .filter("n != 1").count())
    duplicate_keys = dim.groupBy("price_key").count().filter("count > 1").count()
    gaps_or_overlaps = (dim.withColumn("next_from", F.lead("valid_from").over(w))
                        .filter("next_from IS NOT NULL AND valid_to != date_sub(next_from, 1)")
                        .count())
    return {"not_one_current": not_one_current, "duplicate_keys": duplicate_keys,
            "gaps_or_overlaps": gaps_or_overlaps}
