"""Unit tests for the transformation rules, on tiny hand-made DataFrames.

Each test: build a few rows -> run one function -> check one behaviour.
"""
import datetime as dt

import pandas as pd
from pyspark.sql import functions as F

from retail_lakehouse.transforms import (build_price_scd2, clean_events, daily_prices,
                                         scd2_violations)

BRONZE_SCHEMA = ("event_time string, event_type string, product_id long, category_id long, "
                 "category_code string, brand string, price double, user_id long, "
                 "user_session string, event_date date, _source_file string")
D = dt.date


def bronze_row(time="2019-10-01 10:00:00 UTC", event_type="view", product_id=1,
               category_code="electronics.smartphone", brand="acme", price=100.0, user_id=7):
    return (time, event_type, product_id, 99, category_code, brand, price, user_id, "s1",
            D(2019, 10, 1), "2019-Oct.csv")


def make_df(spark, rows, schema):
    """Build a test DataFrame through pandas + Arrow. Arrow hands the rows straight to the
    Java side, so no Python worker processes are needed (they crash on some Windows laptops)."""
    cols = [c.strip().split(" ")[0] for c in schema.split(",")]
    return spark.createDataFrame(pd.DataFrame(rows, columns=cols), schema)


def daily_df(spark, rows):
    return make_df(spark, rows, "product_id long, event_date date, price double")


# ---------------- silver: clean_events ----------------

def test_exact_duplicates_are_removed(spark):
    rows = [bronze_row(), bronze_row(), bronze_row(user_id=8)]     # 2 identical + 1 different
    out = clean_events(make_df(spark, rows, BRONZE_SCHEMA))
    assert out.count() == 2


def test_missing_brand_and_category_become_unknown_not_dropped(spark):
    rows = [bronze_row(category_code=None, brand=None)]
    out = clean_events(make_df(spark, rows, BRONZE_SCHEMA)).first()
    assert (out.brand, out.category, out.subcategory) == ("unknown", "unknown", "unknown")


def test_category_code_is_split(spark):
    rows = [bronze_row(category_code="appliances.kitchen.refrigerators")]
    out = clean_events(make_df(spark, rows, BRONZE_SCHEMA)).first()
    assert (out.category, out.subcategory) == ("appliances", "refrigerators")


def test_event_time_is_parsed_as_utc(spark):
    rows = [bronze_row(time="2019-10-31 23:59:59 UTC")]
    out = clean_events(make_df(spark, rows, BRONZE_SCHEMA))
    # Format the timestamp INSIDE Spark (session time zone = UTC). Pulling it into Python
    # first would convert it to the laptop's own time zone (IST adds 5:30) and the test
    # would pass or fail depending on which machine runs it.
    shown = out.select(F.date_format("event_ts", "yyyy-MM-dd HH:mm:ss").alias("ts")).first().ts
    assert shown == "2019-10-31 23:59:59"


def test_zero_price_is_flagged_not_dropped(spark):
    rows = [bronze_row(price=0.0), bronze_row(price=10.0, user_id=8)]
    out = clean_events(make_df(spark, rows, BRONZE_SCHEMA))
    flags = sorted(r.is_valid_price for r in out.collect())
    assert flags == [False, True]


# ---------------- daily_prices ----------------

def test_daily_price_is_last_valid_price_of_the_day(spark):
    rows = [bronze_row(time="2019-10-01 09:00:00 UTC", price=100.0),
            bronze_row(time="2019-10-01 18:00:00 UTC", price=90.0, user_id=8),   # last valid
            bronze_row(time="2019-10-01 20:00:00 UTC", price=0.0, user_id=9)]    # invalid, ignored
    silver = clean_events(make_df(spark, rows, BRONZE_SCHEMA))
    assert daily_prices(silver).first().price == 90.0


# ---------------- SCD2: build_price_scd2 ----------------

def test_small_change_does_not_create_a_version(spark):
    daily = daily_df(spark, [(1, D(2019, 10, 1), 100.0), (1, D(2019, 10, 2), 100.5)])   # +0.5%
    dim = build_price_scd2(daily)
    assert dim.count() == 1


def test_real_change_closes_old_version_and_opens_new(spark):
    daily = daily_df(spark, [(1, D(2019, 10, 1), 100.0), (1, D(2019, 10, 5), 80.0)])    # -20%
    v1, v2 = build_price_scd2(daily).orderBy("version").collect()
    assert (v1.valid_from, v1.valid_to, v1.is_current) == (D(2019, 10, 1), D(2019, 10, 4), False)
    assert (v2.valid_from, v2.valid_to, v2.is_current) == (D(2019, 10, 5), D(9999, 12, 31), True)
    assert (v1.price, v2.price) == (100.0, 80.0)


def test_price_flip_back_is_a_new_version(spark):
    daily = daily_df(spark, [(1, D(2019, 10, 1), 100.0), (1, D(2019, 10, 2), 80.0),
                             (1, D(2019, 10, 3), 100.0)])                                # sale on, off
    dim = build_price_scd2(daily)
    assert [r.price for r in dim.orderBy("version").collect()] == [100.0, 80.0, 100.0]


def test_version_starts_on_the_day_the_price_changes(spark):
    # Price holds for 2 days, then drops. The new version must start on the day of the drop,
    # and version 1 must cover both days at the old price.
    daily = daily_df(spark, [(1, D(2019, 10, 1), 100.0), (1, D(2019, 10, 2), 100.0),
                             (1, D(2019, 10, 3), 80.0)])
    rows = [(r.price, r.valid_from, r.valid_to)
            for r in build_price_scd2(daily).orderBy("version").collect()]
    assert rows == [(100.0, D(2019, 10, 1), D(2019, 10, 2)),
                    (80.0, D(2019, 10, 3), D(9999, 12, 31))]


def test_products_never_borrow_each_others_prices(spark):
    daily = daily_df(spark, [(1, D(2019, 10, 1), 100.0), (2, D(2019, 10, 2), 50.0)])
    dim = build_price_scd2(daily)
    assert dim.count() == 2 and dim.filter("version = 1").count() == 2


def test_built_table_obeys_scd2_invariants(spark):
    daily = daily_df(spark, [(1, D(2019, 10, d), p) for d, p in
                             [(1, 100.0), (2, 80.0), (3, 80.4), (4, 100.0)]] +
                     [(2, D(2019, 10, 1), 10.0)])
    assert scd2_violations(build_price_scd2(daily)) == {
        "not_one_current": 0, "duplicate_keys": 0, "gaps_or_overlaps": 0}


# ---------------- the checker itself must catch broken tables ----------------

def test_invariant_checker_catches_an_overlap(spark):
    broken = make_df(spark,
        [(11, 1, 1, 100.0, D(2019, 10, 1), D(2019, 10, 9), False),     # ends on the 9th...
         (12, 1, 2, 80.0, D(2019, 10, 5), D(9999, 12, 31), True)],     # ...but next starts on the 5th
        "price_key long, product_id long, version int, price double, valid_from date, "
        "valid_to date, is_current boolean")
    assert scd2_violations(broken)["gaps_or_overlaps"] == 1


def test_invariant_checker_catches_two_current_rows(spark):
    broken = make_df(spark,
        [(11, 1, 1, 100.0, D(2019, 10, 1), D(2019, 10, 4), True),
         (12, 1, 2, 80.0, D(2019, 10, 5), D(9999, 12, 31), True)],
        "price_key long, product_id long, version int, price double, valid_from date, "
        "valid_to date, is_current boolean")
    assert scd2_violations(broken)["not_one_current"] == 1
