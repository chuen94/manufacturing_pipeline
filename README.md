# Manufacturing Analytics Pipeline

An analytics pipeline for manufacturing data, built in two parts. **Part 1**
ingests per-minute line telemetry on a short micro-batch loop, transforms it
with dbt into a tested analytical model, and serves it to a dashboard.
**Part 2** joins shift-level production records to that telemetry to test
whether substituting one material for another changes line outcomes (see
[Phase 2](#phase-2-material-substitution-analysis)).

It runs entirely on synthetic data, so anyone can clone it and run the whole
thing end to end with no external database. It is a personal portfolio project
that demonstrates the engineering and the analytical approach; it is not
connected to any company's data or systems.

```
 generator (synthetic source)           transform (dbt)                    serving
 ┌───────────────────────────┐   ┌────────────────────────────┐   ┌────────────────────────┐
 │ raw_process │raw_assembly │   │ staging  → fct_telemetry   │   │                        │
 │ raw_oee     │ raw_matsrc  │ → │ (unpivot)  (incremental)   │ → │ gold_telemetry.parquet │ → Grafana
 │ (DuckDB)                  │   │          → dim_matsrc(SCD2)│   │                        │
 └───────────────────────────┘   └────────────────────────────┘   └────────────────────────┘
      ▲ tick every 60s                ▲ sub-second build               ▲ file, no lock
```

**Stack:** Python (generator + loop), DuckDB (warehouse), dbt (transform/test),
Parquet (serving layer), Grafana (dashboard).

## Design choice: micro-batch for the telemetry feed

The source emits once per minute and the consumer is a human watching a
dashboard. Streaming's only advantage is lower latency, and latency below ~1
minute has no taker here: there's no new information between minutes, and an
operator's reaction loop is minutes long anyway. Streaming would also add real
operational cost and failure modes (Kafka, a stream processor, offset/consumer
management) for latency the use case can't convert into value.

So this is deliberately **micro-batch**: ~1-minute end-to-end lag, which matches
the data's cadence. Streaming would be the right call for kHz sensors, automated
sub-second control loops with no human in the path, or many consumers needing
one event stream — none of which apply here.

## Key design decisions

- **Long/narrow fact table.** The line families expose different sensor sets
  (process vs assembly vs OEE lines). A wide table would be sparse and need a schema edit
  every time a sensor is added. Long format keeps every parameter for every line
  automatically — new sensors just flow through.
- **Incremental fact model.** First run builds all history; every run after only
  reprocesses a trailing 2-hour window with `delete+insert` dedupe. That turns a
  full rebuild into a sub-second append, which is what makes the 60-second loop
  viable. (A full rebuild each cycle would stack runs on top of each other.)
- **Parquet serving layer.** DuckDB is single-writer across processes — if the
  dashboard holds the `.duckdb` file open, the pipeline can't write to it. The
  gold model materializes to a Parquet file instead, so the writer owns the
  warehouse and the reader owns the file. They never collide.
- **Derived SCD2 for material source.** The material assignment log is append-only
  event data, so the material-over-time dimension is derived with window
  functions (`valid_from`/`valid_to`) rather than a dbt snapshot — snapshots are
  for mutable sources where history isn't already kept.
- **Tests, freshness, and an exposure.** `not_null` / `accepted_values` on the
  models, `source freshness` tied to the real-time claim, and an exposure
  declaring the dashboard as a downstream consumer.

## Quickstart

> **Requires Python 3.11 or 3.12.** dbt 1.8 does not run on Python 3.13/3.14
> (a `mashumaro` import error). A `.python-version` file pins this.

```bash
python -m venv venv && source venv/bin/activate   # Windows: .\venv\Scripts\activate
pip install -r requirements.txt

# 1. seed history (once)
python generator/generate.py --backfill-days 30
python generator/generate_shift_data.py --backfill-days 240   # Phase 2, see below

# 2. build + test the models
cd transform && dbt build --profiles-dir . && cd ..

# 3. run the near-real-time loop (tick -> dbt run, every 60s)
python run_pipeline.py

# 4. (Phase 2) run the material-substitution comparison
python analysis/compare_materials.py
```

The loop writes `serving/gold_telemetry.parquet`. Point Grafana's DuckDB
datasource (in-memory) at it:

```sql
SELECT reading_time AS "time", parameter_name, reading_value
FROM read_parquet('serving/gold_telemetry.parquet')
WHERE $__timeFilter(reading_time) AND line_id = ${line_id:singlequote}
  AND parameter_name IN (${parameter:singlequote})
ORDER BY reading_time
```

## Phase 2: material-substitution analysis

**The idea.** A plant running the same formula on the same line over time
often substitutes one material for another underneath an unchanged formula
code — a supply swap, a cost decision, a trial that was never labeled as
one. The production record already contains this history; it just isn't
lined up against what the machine was actually doing at the time. This phase
demonstrates how to recover that signal: join shift-level production
records (what was fed to the line, and the shift's yield/scrap/downtime
outcome) against machine telemetry over the same time window, then test
whether a material substitution shows up as a real, repeatable difference
or just noise.

**Why this needed its own verdict logic, not just an average.** Two
separate problems can each make a naive pooled average lie to you, so the
verdict logic in `analysis/compare_materials.py` checks for both:

- **Is it even a real effect, or just noise?** With enough pairs and metrics
  tested, *some* pooled difference will look nonzero by pure chance. A
  pooled-difference-vs-standard-error check (effectively, "is this outside
  roughly 3 standard errors of pure sampling noise") has to pass before
  anything downstream is trusted.
- **Does it hold up, or is it a sampling imbalance in disguise?** A real
  pooled difference can still be an artifact if the two materials didn't
  run under comparable conditions — e.g. one of them happening to run
  mostly on one shift, or under a different formula. So the effect also has
  to hold up in the *same direction* when checked separately within every
  level of every stratification dimension (shift, and formula) — not just
  pooled.

A pair has to clear both checks to be reported as a finding; failing either
one gets it reported as `MIXED` (or skipped outright as noise), never
quietly dropped — "no real effect found here" is treated as a finding in
its own right, not a gap in the analysis.

```
 generator                     transform (dbt)                                     analysis
 ┌─────────────────────┐  ┌──────────────────────────────────────────────────┐  ┌───────────────────────┐
 │ raw_shift_runs      │  │ staging → int_run_windows → int_run_conditions   │  │ compare_materials.py  │
 │ raw_shift_materials │→ │ (cast)    (LEAD → window)    (range join vs      │→ │ pooled diff, then     │
 │ raw_shift_timeline  │  │                                fct_telemetry)    │  │ shift+formula-        │
 │ (DuckDB)            │  │          → dim_material → fct_run → mart_*       │  │ stratified verdict    │
 └─────────────────────┘  └──────────────────────────────────────────────────┘  └───────────────────────┘
```

**Note on the data:** everything in this phase, including every line id,
material code, and formula code, is synthetic, generated by
`generator/generate_shift_data.py`, with a few effects deliberately planted
so the verdict logic has something to find. The repo demonstrates an
analysis *method* — the join pattern connecting production records to
machine telemetry, and the stratified-verdict logic that guards against
false positives — and shows the approach's potential. It is a proof of
concept: it has not been deployed to, or run against, any live system.
Because the data is randomly generated with no fixed seed, exact numbers
differ from run to run; the shape of the result is what's reproducible.

## CI

`.github/workflows/ci.yml` runs on every push: it generates synthetic data
(both telemetry and shift records), runs `dbt build` (models + tests), checks
source freshness, and runs the material-substitution analysis end to end. A
green check means the models compile, the tests pass, and the whole thing —
including Phase 2 — is reproducible from scratch.

## Lessons learned / known limitations

- **Single-writer lock was the hardest bug.** Pointing the dashboard at the live
  `.duckdb` file silently blocked the writer; the Parquet serving layer is the
  fix. Reader/writer decoupling is the real lesson.
- **Timezones bite at the serving boundary.** Naive local timestamps get
  interpreted as UTC by dashboard tools, shifting the plot by the UTC offset.
  Make timestamps timezone-aware (or set the dashboard timezone) at the serving
  edge.
- **It's tied to one always-on machine.** "Live whenever you open it" holds only
  while the loop and the host are running. A production version would run the
  loop as a managed service and add a data-freshness alert.
- **A pooled difference needs a stratification check before it's a finding.**
  See Phase 2 above — this was the single most important correctness
  guard added in that phase, and the reason the analysis reports some
  pairs as `MIXED` rather than forcing a finding out of every comparison.

## Repo structure

```
generator/generate.py              synthetic telemetry source (backfill + per-minute tick)
generator/generate_shift_data.py   synthetic shift-production-record source (Phase 2)
transform/                         dbt project (staging, intermediate, marts, sources, tests)
analysis/compare_materials.py      Phase 2: stratified material-substitution comparison
run_pipeline.py                    near-real-time loop: tick -> dbt run
serving/                           gold_telemetry.parquet (gitignored, rebuilt)
.github/workflows/ci.yml           generate -> dbt build -> freshness -> analysis
```
