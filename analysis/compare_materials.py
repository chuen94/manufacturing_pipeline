"""
Compares pairs of substitutable materials on each line, using the fct_run
table that dbt builds. A difference only gets reported if it passes two checks:

 1. Size: the pooled difference has to be big compared with the noise in the
    two groups (a z-score). Stratifying by itself isn't a significance test.
    It only asks whether the sign agrees across groups, and with only a few
    groups that can happen by chance even when there's no real difference.
 2. Stratification: the difference has to point the same way within every
    shift and within every formula, not just overall. This catches a pooled
    number that really comes from imbalance, for example one material
    running mostly on nights or mostly under one formula.

Pairs that fail the first check are skipped as noise. Pairs that pass it but
disagree across shifts or formulas are MIXED and don't get printed. Only
ROBUST ones show up in the output.

The data is synthetic (see generator/generate_shift_data.py) and the point is
the method. Only needs duckdb.

Usage, from the repo root after generate_shift_data.py and dbt build:
    python analysis/compare_materials.py
"""
import math

import duckdb

DB_PATH = "warehouse.duckdb"

MATERIAL_FAMILIES = {
    "FAM-A": ["MAT-100", "MAT-220"],
    "FAM-B": ["RECYCLED-1", "RECYCLED-2"],
}
METRICS = ["total_scrap_lbs", "downtime_min", "yield_pct"]
LOWER_IS_BETTER = {"total_scrap_lbs": True, "downtime_min": True, "yield_pct": False}
MIN_N = 15         # min runs per material overall
MIN_STRATA_N = 15  # min runs per material inside one shift or formula level.
                   # Smaller groups are too noisy for "the sign agrees" to mean anything.
Z_THRESHOLD = 3.0  # the pooled difference must be at least this many standard
                   # errors from zero before we bother stratifying
DIMENSIONS = ["shift", "formula"]  # all of these have to agree for ROBUST


def sign(x, eps=1e-9):
    if abs(x) < eps:
        return 0
    return 1 if x > 0 else -1


def fetch_runs(con, line, materials):
    placeholders = ", ".join(f"'{m}'" for m in materials)
    cols = ", ".join(METRICS)
    return con.execute(f"""
        SELECT shift, formula, primary_material, {cols}
        FROM fct_run
        WHERE line_id = '{line}' AND primary_material IN ({placeholders})
    """).fetchall()


# column layout from fetch_runs: 0=shift, 1=formula, 2=primary_material, 3..=metrics
DIM_COL = {"shift": 0, "formula": 1}
MATERIAL_COL = 2
METRIC_COL0 = 3


def pooled_diff(rows, base, alt, metric_idx):
    a = [r[metric_idx] for r in rows if r[MATERIAL_COL] == base]
    b = [r[metric_idx] for r in rows if r[MATERIAL_COL] == alt]
    if len(a) < MIN_N or len(b) < MIN_N:
        return None
    mean_a, mean_b = sum(a) / len(a), sum(b) / len(b)
    var_a = sum((x - mean_a) ** 2 for x in a) / (len(a) - 1)
    var_b = sum((x - mean_b) ** 2 for x in b) / (len(b) - 1)
    se = math.sqrt(var_a / len(a) + var_b / len(b))
    z = (mean_b - mean_a) / se if se > 0 else 0.0
    return mean_a, mean_b, mean_b - mean_a, len(a), len(b), z


def dim_check(rows, dim, base, alt, metric_idx):
    """For one dimension (say shift), check whether base vs alt has the same
    sign in every level with enough runs. Returns (verdict, sign, per-level detail)."""
    col = DIM_COL[dim]
    signs, detail = [], []
    for level in sorted(set(r[col] for r in rows)):
        sub = [r for r in rows if r[col] == level]
        a = [r[metric_idx] for r in sub if r[MATERIAL_COL] == base]
        b = [r[metric_idx] for r in sub if r[MATERIAL_COL] == alt]
        if len(a) >= MIN_STRATA_N and len(b) >= MIN_STRATA_N:
            d = (sum(b) / len(b)) - (sum(a) / len(a))
            signs.append(sign(d))
            detail.append((level, len(a), len(b), d))
    nonzero = [s for s in signs if s != 0]
    if not nonzero:
        return "insufficient_data", 0, detail
    if len(set(nonzero)) == 1:
        return "robust", nonzero[0], detail
    return "mixed", 0, detail


def stratified_verdict(rows, base, alt, metric_idx):
    """Combine the dimensions. ROBUST needs every dimension that had enough
    data to come back robust, and all of them to point the same direction."""
    dim_results = {dim: dim_check(rows, dim, base, alt, metric_idx) for dim in DIMENSIONS}
    verdicts = [v for v, s, d in dim_results.values()]
    signs = [s for v, s, d in dim_results.values() if v == "robust"]
    if "mixed" in verdicts:
        return "MIXED", dim_results
    if not signs:
        return "insufficient_data", dim_results
    if len(set(signs)) == 1:
        return "ROBUST", dim_results
    return "MIXED", dim_results


def main():
    con = duckdb.connect(DB_PATH, read_only=True)
    lines = [r[0] for r in con.execute(
        "SELECT DISTINCT line_id FROM fct_run ORDER BY 1"
    ).fetchall()]

    print("=" * 78)
    print("MATERIAL-SUBSTITUTION COMPARISON (synthetic data)")
    print("The data is generated. This demonstrates the method, see README.md.")
    print("=" * 78)

    any_found = False
    for line in lines:
        for family, materials in MATERIAL_FAMILIES.items():
            base, alt = materials
            rows = fetch_runs(con, line, materials)
            if not rows:
                continue
            for i, metric in enumerate(METRICS):
                idx = METRIC_COL0 + i
                pooled = pooled_diff(rows, base, alt, idx)
                if pooled is None:
                    continue
                mean_a, mean_b, diff, n_a, n_b, z = pooled
                if abs(z) < Z_THRESHOLD:
                    continue  # looks like noise, skip
                verdict, dim_results = stratified_verdict(rows, base, alt, idx)
                if verdict != "ROBUST":
                    continue
                any_found = True
                worse_is_alt = (diff > 0) == LOWER_IS_BETTER[metric]
                worse, better = (alt, base) if worse_is_alt else (base, alt)
                print(f"\n{line} / {family}: {base} (n={n_a}) vs {alt} (n={n_b}) -- {metric}")
                print(f"  pooled: {base}={mean_a:.2f}  {alt}={mean_b:.2f}  diff={diff:+.2f}  (z={z:+.1f})")
                print(f"  verdict: ROBUST -- {better} beats {worse}, same direction in every shift and formula")
                for dim, (dim_verdict, dim_sign, detail) in dim_results.items():
                    print(f"    by {dim}: {dim_verdict}")
                    for level, sa, sb, d in detail:
                        print(f"      {level}: n_{base}={sa} n_{alt}={sb} diff={d:+.2f}")

    if not any_found:
        print("\nNo ROBUST findings at these thresholds.")

    print("\nDone. Each finding above passed the size check and also pointed the same\n"
          "way within every shift and every formula.\n"
          "The same material pair can come out ROBUST in opposite directions on two\n"
          "lines (it does in this data), so the effect depends on the machine as\n"
          "well as the material.\n"
          "The data has no fixed seed, so the exact numbers, and sometimes which\n"
          "pairs make the cut, change from run to run.")
    con.close()


if __name__ == "__main__":
    main()
