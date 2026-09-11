# Databricks notebook source
# MAGIC %md
# MAGIC # Seed the static truck dimension
# MAGIC
# MAGIC Adapted from Appendix B of the brief, with two changes that matter for a
# MAGIC deploy-only `prod`:
# MAGIC
# MAGIC * the table is **not** created here — migration `001` owns its structure;
# MAGIC * the write is a **MERGE**, not `mode("overwrite")`. Re-running converges
# MAGIC   instead of replacing, so a re-deploy can never blow away the dimension
# MAGIC   or break a concurrent reader.
# MAGIC
# MAGIC `random.seed(42)` makes the 20 rows deterministic, so dev, test and prod
# MAGIC all get the same dimension and a re-run is a genuine no-op.

# COMMAND ----------

import os
import random
import sys

for _candidate in ("../common", "src/common", "./common"):
    _p = os.path.abspath(os.path.join(os.getcwd(), _candidate))
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

from pyspark.sql import functions as F  # noqa: E402
from telematics import Config, merge_into  # noqa: E402

cfg = Config.from_widgets(dbutils)
TARGET = cfg.table("truck_details")

# COMMAND ----------

random.seed(42)

MAKES = [("Freightliner", "Cascadia"), ("Volvo", "VNL"),
         ("Kenworth", "T680"), ("Peterbilt", "579")]
DEPOTS = [("Chicago", "Midwest"), ("Dallas", "South"),
          ("Denver", "West"), ("Atlanta", "Southeast")]
DRIVERS = ["A. Rivera", "B. Chen", "C. Okafor", "D. Patel", "E. Nguyen",
           "F. Santos", "G. Kim", "H. Brooks", "I. Novak", "J. Alvarez"]

rows = []
for i in range(1, 21):
    make, model = random.choice(MAKES)
    depot, region = random.choice(DEPOTS)
    rows.append((f"TRK-{i:03d}", make, model,
                 random.choice([20000, 26000, 34000, 40000]),
                 depot, region, random.choice(DRIVERS)))

trucks = (
    spark.createDataFrame(
        rows,
        "truck_id string, make string, model string, capacity_lbs int, "
        "home_depot string, region string, driver string",
    )
    .withColumn("_updated_at", F.current_timestamp())
)

merge_into(spark, trucks, TARGET, keys=["truck_id"], mode="upsert")

print(f"seeded {TARGET}")
display(spark.table(TARGET).orderBy("truck_id"))
