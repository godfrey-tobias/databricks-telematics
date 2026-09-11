# Databricks notebook source
# MAGIC %md
# MAGIC # Generate synthetic GPS pings
# MAGIC
# MAGIC Adapted from Appendix A. Writes newline-delimited JSON into the target's
# MAGIC landing volume, where Auto Loader picks it up.
# MAGIC
# MAGIC Differences from the appendix script:
# MAGIC
# MAGIC * the catalog / schema / volume are **not** created here — the bundle owns
# MAGIC   them, so this notebook only ever writes files;
# MAGIC * batch count and pause are job parameters, so dev generates more data
# MAGIC   than prod without a code change;
# MAGIC * filenames carry a run id, so two runs cannot collide and Auto Loader
# MAGIC   sees every file exactly once.
# MAGIC
# MAGIC It still emits the two awkward rows the brief asks for: an exact duplicate
# MAGIC ping and a row with a null latitude. Silver has to handle both.

# COMMAND ----------

import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone

for _candidate in ("../common", "src/common", "./common"):
    _p = os.path.abspath(os.path.join(os.getcwd(), _candidate))
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

from telematics import Config, widget  # noqa: E402

cfg = Config.from_widgets(dbutils)
BATCHES = int(widget(dbutils, "batches", "20"))
SLEEP_SECONDS = float(widget(dbutils, "sleep_seconds", "1"))

LANDING = cfg.landing_path
RUN_ID = uuid.uuid4().hex[:8]

TRUCKS = [f"TRK-{i:03d}" for i in range(1, 21)]
LAT0, LON0 = 41.85, -87.65  # near Chicago

print(f"writing {BATCHES} batch(es) to {LANDING} (run {RUN_ID})")

# COMMAND ----------


def make_ping(truck_id):
    return {
        "truck_id": truck_id,
        "latitude": round(LAT0 + random.uniform(-0.2, 0.2), 6),
        "longitude": round(LON0 + random.uniform(-0.2, 0.2), 6),
        "event_ts": datetime.now(timezone.utc).isoformat(),
    }


total_rows = 0
for batch in range(BATCHES):
    rows = [make_ping(random.choice(TRUCKS)) for _ in range(random.randint(5, 15))]
    rows.append(dict(rows[0]))                                           # exact duplicate
    rows.append({**make_ping(random.choice(TRUCKS)), "latitude": None})   # invalid row

    filename = f"{LANDING}/pings_{RUN_ID}_{int(time.time() * 1000)}_{batch:04d}.json"
    with open(filename, "w") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")

    total_rows += len(rows)
    print(f"wrote {os.path.basename(filename)} ({len(rows)} rows)")
    if SLEEP_SECONDS:
        time.sleep(SLEEP_SECONDS)

print(f"done — {total_rows} row(s) across {BATCHES} file(s)")
