"""
Fake factory telemetry, so the pipeline runs anywhere without a database.

There are three families of lines, each with its own set of sensors. The
schemas don't match, which is why the dbt side stores readings in long format.
It also writes a material-source assignment log that changes over time, which
becomes the SCD dimension later.

Usage:
    python generate.py --backfill-days 30      # seed history, run once
    python generate.py --tick                  # add one new minute (run_pipeline.py calls this)
"""
import argparse
import datetime as dt
import math
import random

import duckdb

DB_PATH = "warehouse.duckdb"

# Three families, three different wide schemas.
FAMILIES = {
    "raw_process": {
        "lines": ["line_01", "line_02", "line_03"],
        "sensors": {
            "line_speed":  (40, 6),     # (mean, stdev)
            "pressure":    (2400, 120),
            "temp_zone_1": (210, 4),
            "temp_zone_2": (225, 5),
            "motor_rpm":   (95, 8),
        },
    },
    "raw_assembly": {
        "lines": ["line_04", "line_05"],
        "sensors": {
            "tension":          (180, 15),
            "roll_diameter":    (600, 40),
            "run_speed":        (320, 25),
            "station_pressure": (75, 6),
        },
    },
    "raw_oee": {
        "lines": ["line_06"],
        "sensors": {
            "output_rate":  (1200, 80),
            "good_count":   (1180, 80),
            "reject_count": (20, 8),
        },
    },
}

N_SOURCES = 12
MATERIAL_CODES = ["MAT-100", "MAT-220", "MAT-340", "MAT-450", "RECYCLED-1", "RECYCLED-2"]


def _connect():
    return duckdb.connect(DB_PATH)


def _ensure_tables(con):
    for table, spec in FAMILIES.items():
        cols = ", ".join(f"{s} DOUBLE" for s in spec["sensors"])
        con.execute(
            f"CREATE TABLE IF NOT EXISTS {table} "
            f"(line_id VARCHAR, reading_time TIMESTAMP, {cols})"
        )
    con.execute(
        "CREATE TABLE IF NOT EXISTS raw_material_source "
        "(source_id INTEGER, material_code VARCHAR, assigned_date DATE)"
    )


def _reading(mean, stdev, t):
    """One sensor value: gaussian noise around a mean that swings slowly,
    with a rare spike so there's something for anomaly checks to catch."""
    drift = 1.0 + (0.04 * math.sin(t.timestamp() / 3600.0))
    spike = random.random() < 0.002
    val = random.gauss(mean * drift, stdev)
    if spike:
        val *= random.choice([1.18, 0.82])
    return round(val, 3)


def _rows_for_minute(t):
    rows = {table: [] for table in FAMILIES}
    for table, spec in FAMILIES.items():
        for line in spec["lines"]:
            vals = [_reading(m, s, t) for (m, s) in spec["sensors"].values()]
            rows[table].append((line, t) + tuple(vals))
    return rows


def _insert(con, rows):
    for table, recs in rows.items():
        if not recs:
            continue
        placeholders = ", ".join(["?"] * len(recs[0]))
        con.executemany(f"INSERT INTO {table} VALUES ({placeholders})", recs)


def backfill(days):
    con = _connect()
    _ensure_tables(con)
    now = dt.datetime.now().replace(second=0, microsecond=0)
    start = now - dt.timedelta(days=days)

    # one row per line per minute
    t = start
    batch = {table: [] for table in FAMILIES}
    count = 0
    while t <= now:
        for table, recs in _rows_for_minute(t).items():
            batch[table].extend(recs)
        t += dt.timedelta(minutes=1)
        count += 1
        if count % 1440 == 0:          # write out once a day of data to keep memory low
            _insert(con, batch)
            batch = {table: [] for table in FAMILIES}
            print(f"  ...generated through {t.date()}")
    _insert(con, batch)

    # every source gets a new material assignment every 5 days
    source_rows = []
    d = start.date()
    while d <= now.date():
        for src in range(1, N_SOURCES + 1):
            if (d - start.date()).days % 5 == 0:
                source_rows.append((src, random.choice(MATERIAL_CODES), d))
        d += dt.timedelta(days=1)
    con.executemany("INSERT INTO raw_material_source VALUES (?, ?, ?)", source_rows)

    print(f"Backfilled {days} days of telemetry and {len(source_rows)} material assignments -> {DB_PATH}")
    con.close()


def tick():
    """Add the current minute. Called once per cycle by run_pipeline.py."""
    con = _connect()
    _ensure_tables(con)
    now = dt.datetime.now().replace(second=0, microsecond=0)
    _insert(con, _rows_for_minute(now))
    print(f"tick: appended minute {now:%Y-%m-%d %H:%M}")
    con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Synthetic factory telemetry generator")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--backfill-days", type=int, help="seed N days of history")
    g.add_argument("--tick", action="store_true", help="append one fresh minute")
    args = ap.parse_args()

    if args.backfill_days:
        backfill(args.backfill_days)
    else:
        tick()
