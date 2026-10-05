"""
Micro-batch loop. Run from the repo root.

Every 60 seconds it adds one new minute of synthetic data and runs dbt. The
incremental fact model keeps the dbt step fast, and Grafana reads the Parquet
file that comes out, so the dashboard trails the clock by about a minute.

Locally this is just a while loop. Anywhere real you'd want a scheduler.
"""
import datetime as dt
import subprocess
import sys
import time

INTERVAL_SECONDS = 60


def cycle():
    subprocess.run([sys.executable, "generator/generate.py", "--tick"], check=True)
    subprocess.run(["dbt", "run", "--profiles-dir", "."], cwd="transform", check=True)


if __name__ == "__main__":
    print(f"Loop started: tick -> dbt run every {INTERVAL_SECONDS}s. Ctrl+C to stop.")
    while True:
        try:
            cycle()
            print(f"[{dt.datetime.now():%H:%M:%S}] cycle ok")
        except subprocess.CalledProcessError as exc:
            print(f"[{dt.datetime.now():%H:%M:%S}] cycle FAILED: {exc}")
        time.sleep(INTERVAL_SECONDS)
