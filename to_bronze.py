"""
Retail lakehouse - BRONZE step (runs on your laptop).

Turns one raw monthly CSV into Parquet files split by day:
    data/bronze/events/event_date=2019-10-01/data_0.parquet
    data/bronze/events/event_date=2019-10-02/data_0.parquet
    ...

Bronze rule: keep the raw columns exactly as they arrived. We only ADD columns:
    event_date    - the day (used to split the files)
    _source_file  - which file each row came from
    _ingested_at  - when we loaded it
No cleaning or deduplication here; that belongs in silver.

Run from the retail-lakehouse folder:
    .venv\\Scripts\\python.exe to_bronze.py data/raw/2019-Oct.csv
Safe to re-run: that month's old output is replaced, not duplicated.
"""
import os
import shutil
import sys
import time

import duckdb

SRC = sys.argv[1] if len(sys.argv) > 1 else "data/raw/2019-Oct.csv"
OUT = "data/bronze/events"
SOURCE_FILE = os.path.basename(SRC)

# Raw schema, stated explicitly (never trust auto-detection on 42M rows).
COLUMNS = ("{'event_time': 'VARCHAR', 'event_type': 'VARCHAR', 'product_id': 'BIGINT', "
           "'category_id': 'BIGINT', 'category_code': 'VARCHAR', 'brand': 'VARCHAR', "
           "'price': 'DOUBLE', 'user_id': 'BIGINT', 'user_session': 'VARCHAR'}")

con = duckdb.connect()
con.execute("SET memory_limit = '4GB'")
con.execute("SET preserve_insertion_order = false")   # lets DuckDB stream instead of holding rows

start = time.time()
raw_rows = con.execute(
    "SELECT count(*) FROM read_csv('" + SRC + "', header = true, columns = " + COLUMNS + ")"
).fetchone()[0]
print(f"Raw rows in {SOURCE_FILE}: {raw_rows:,}")

days = [r[0] for r in con.execute(
    "SELECT DISTINCT substr(event_time, 1, 10) FROM read_csv('" + SRC + "', header = true, columns = "
    + COLUMNS + ")").fetchall()]
# Idempotent: remove this month's days before writing them again.
for d in days:
    shutil.rmtree(os.path.join(OUT, f"event_date={d}"), ignore_errors=True)
os.makedirs(OUT, exist_ok=True)

con.execute(
    "COPY ("
    "  SELECT *,"
    "         CAST(substr(event_time, 1, 10) AS DATE) AS event_date,"
    "         '" + SOURCE_FILE + "' AS _source_file,"
    "         now() AS _ingested_at"
    "  FROM read_csv('" + SRC + "', header = true, columns = " + COLUMNS + ")"
    ") TO '" + OUT + "' (FORMAT parquet, PARTITION_BY (event_date), COMPRESSION zstd, OVERWRITE_OR_IGNORE)"
)

# Quality gate: every raw row must land in bronze, no more, no less.
written, n_days, first, last = con.execute(
    "SELECT count(*), count(DISTINCT event_date), min(event_date), max(event_date) "
    "FROM read_parquet('" + OUT + "/*/*.parquet', hive_partitioning = true) "
    "WHERE _source_file = '" + SOURCE_FILE + "'"
).fetchone()

size_mb = sum(os.path.getsize(os.path.join(dp, f))
              for dp, _, fs in os.walk(OUT) for f in fs) / 1e6
print(f"Bronze rows written: {written:,} across {n_days} days ({first} to {last})")
print(f"Bronze size on disk: {size_mb:,.0f} MB   (raw CSV: {os.path.getsize(SRC) / 1e6:,.0f} MB)")
print(f"Took {int(time.time() - start)} s")

if written != raw_rows:
    sys.exit(f"QUALITY GATE FAILED: {raw_rows:,} raw rows but {written:,} in bronze")
print("Quality gate passed: row counts match.")
