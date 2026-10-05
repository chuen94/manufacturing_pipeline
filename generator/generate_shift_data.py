"""
Fake shift production records for the material-substitution analysis.

Each run is one line, one shift, one formula, with a record of which material
fed it and how the shift turned out (output, downtime, scrap, yield). Every
line id, material code and formula code is made up. See the Phase 2 section
of the README.

Run it with the same --backfill-days as generate.py, or fewer, so the run
windows fall inside the telemetry history. int_run_conditions needs the two
to overlap in time. The default here is 240 days, longer than generate.py's,
because compare_materials.py needs a decent number of runs in every
shift x formula x material group. With a short backfill you mostly get
"insufficient_data".

Usage:
    python generator/generate_shift_data.py --backfill-days 240
"""
import argparse
import datetime as dt
import random

import duckdb

DB_PATH = "warehouse.duckdb"

LINES = ["line_01", "line_02", "line_03"]
FORMULAS = ["FORM-1", "FORM-2"]
SHIFTS = ["D", "N"]  # day and night, 12 hours each

# Two material families. Inside a family the two materials can replace each
# other. Across families they can't.
MATERIAL_FAMILIES = {
    "FAM-A": ["MAT-100", "MAT-220"],
    "FAM-B": ["RECYCLED-1", "RECYCLED-2"],
}
ADDITIVE = "MB-ADD"  # about 3% masterbatch in every run, never swapped

# Effects built into the data, keyed by (line, family). Each value is the
# fractional change in a metric's mean for the second material compared with
# the first. They exist so compare_materials.py has something to find,
# including one pair that goes opposite ways on two different lines
# (line_01 vs line_03, FAM-A).
PLANTED_EFFECTS = {
    ("line_01", "FAM-A"): {"total_scrap_lbs": +0.18},
    ("line_03", "FAM-A"): {"total_scrap_lbs": -0.15},
    ("line_01", "FAM-B"): {"downtime_min": +0.25},
}

# (mean, stdev) per metric before any planted effect
BASE_RATES = {
    "prod_lbs": (12000.0, 900.0),
    "downtime_min": (35.0, 10.0),
    "total_scrap_lbs": (260.0, 40.0),
    "qc_scrap_lbs": (15.0, 6.0),
    "yield_pct": (95.5, 1.2),
}


def _connect():
    return duckdb.connect(DB_PATH)


def _ensure_tables(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_shift_runs (
            run_key VARCHAR, line_id VARCHAR, prod_date DATE, shift VARCHAR,
            formula VARCHAR, prod_lbs DOUBLE, downtime_min DOUBLE,
            total_scrap_lbs DOUBLE, qc_scrap_lbs DOUBLE, yield_pct DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_shift_materials (
            run_key VARCHAR, material_code VARCHAR, blend_pct DOUBLE
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_shift_timeline (
            run_key VARCHAR, line_id VARCHAR, event_ts TIMESTAMP
        )
    """)


def _family_choice():
    """Pick a family for the day. FAM-A most of the time, FAM-B the other 20%,
    so the second family has a smaller sample."""
    return "FAM-A" if random.random() < 0.8 else "FAM-B"


def _material_choice(day_index, family):
    """Pick the baseline or the candidate material. The mix flips every 14
    days (70/30, then 30/70) so a substitution looks like a campaign that runs
    for a couple of weeks, not a coin flip each shift."""
    campaign = (day_index // 14) % 2
    base_weight = 0.7 if campaign == 0 else 0.3
    materials = MATERIAL_FAMILIES[family]
    return materials[0] if random.random() < base_weight else materials[1]


def _metric_value(metric, line, family, material, materials):
    mean, stdev = BASE_RATES[metric]
    if material == materials[1]:  # the candidate material
        effect = PLANTED_EFFECTS.get((line, family), {}).get(metric, 0.0)
        mean = mean * (1 + effect)
    val = random.gauss(mean, stdev)
    if metric == "yield_pct":
        val = max(0.0, min(100.0, val))
    else:
        val = max(0.0, val)
    return round(val, 2)


def backfill(days):
    con = _connect()
    _ensure_tables(con)
    today = dt.date.today()
    start = today - dt.timedelta(days=days)

    runs, materials, timeline = [], [], []
    d = start
    day_index = 0
    while d <= today:
        for line in LINES:
            family = _family_choice()
            materials_in_family = MATERIAL_FAMILIES[family]
            formula = random.choice(FORMULAS)
            for shift in SHIFTS:
                run_key = f"{line}-{d.isoformat()}-{shift}"
                material = _material_choice(day_index, family)
                values = {m: _metric_value(m, line, family, material, materials_in_family)
                          for m in BASE_RATES}
                runs.append((run_key, line, d, shift, formula, values["prod_lbs"],
                             values["downtime_min"], values["total_scrap_lbs"],
                             values["qc_scrap_lbs"], values["yield_pct"]))
                materials.append((run_key, material, 97.0))
                materials.append((run_key, ADDITIVE, 3.0))
                shift_start_hour = 6 if shift == "D" else 18
                event_ts = dt.datetime.combine(d, dt.time(shift_start_hour, 0))
                timeline.append((run_key, line, event_ts))
        d += dt.timedelta(days=1)
        day_index += 1

    con.executemany("INSERT INTO raw_shift_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", runs)
    con.executemany("INSERT INTO raw_shift_materials VALUES (?, ?, ?)", materials)
    con.executemany("INSERT INTO raw_shift_timeline VALUES (?, ?, ?)", timeline)
    print(f"Backfilled {days} days of shift records: {len(runs)} runs, "
          f"{len(materials)} material rows, {len(timeline)} timeline events -> {DB_PATH}")
    con.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Synthetic shift-production-record generator")
    ap.add_argument("--backfill-days", type=int, default=240,
                     help="how many days of shift history to generate (default 240)")
    args = ap.parse_args()
    backfill(args.backfill_days)
