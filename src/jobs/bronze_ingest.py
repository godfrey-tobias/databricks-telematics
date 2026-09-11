# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Auto Loader ingest
# MAGIC
# MAGIC Landing volume -> `bronze_pings_raw`, append-only, as close to raw as
# MAGIC possible. No casting, no filtering, no dedup: Bronze's only job is to be a
# MAGIC faithful, replayable record of what arrived.
# MAGIC
# MAGIC * `cloudFiles.schemaLocation` gives Auto Loader somewhere to track the
# MAGIC   inferred schema, so a new field in the JSON is an evolution rather than a
# MAGIC   crash.
# MAGIC * `schemaHints` pins the four known fields to the types migration `001`
# MAGIC   declared, so inference can never disagree with the table.
# MAGIC * `_rescued_data` catches anything that does not fit, instead of dropping it.
# MAGIC * The checkpoint lives in the environment's own volume, so re-running
# MAGIC   resumes rather than reprocessing — that is what makes the job idempotent.

# COMMAND ----------

import os
import sys

for _candidate in ("../common", "src/common", "./common"):
    _p = os.path.abspath(os.path.join(os.getcwd(), _candidate))
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

from pyspark.sql import functions as F  # noqa: E402
from telematics import Config, run_available_now  # noqa: E402

cfg = Config.from_widgets(dbutils)

TARGET = cfg.table("bronze_pings_raw")
CHECKPOINT = cfg.checkpoint_path("bronze_pings_raw")
SCHEMA_LOCATION = cfg.checkpoint_path("bronze_pings_raw_schema")

print(f"source     : {cfg.landing_path}")
print(f"target     : {TARGET}")
print(f"checkpoint : {CHECKPOINT}")

# COMMAND ----------

raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaHints",
            "truck_id string, latitude double, longitude double, event_ts string")
    .option("rescuedDataColumn", "_rescued_data")
    .option("multiLine", "false")
    .load(cfg.landing_path)
)

bronze = raw.select(
    F.col("truck_id"),
    F.col("latitude"),
    F.col("longitude"),
    F.col("event_ts"),
    F.col("_rescued_data"),
    F.col("_metadata.file_path").alias("_source_file"),
    F.current_timestamp().alias("_ingested_at"),
)

# COMMAND ----------

writer = (
    bronze.writeStream.format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    # The table already exists (migration 001). mergeSchema lets a genuinely new
    # field discovered by Auto Loader land without a failed run — the follow-up
    # is still a migration, so the change is captured in git.
    .option("mergeSchema", "true")
)

run_available_now(writer, "bronze_ingest", table=TARGET)

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT count(*) AS bronze_rows,
               count(DISTINCT _source_file) AS files_ingested,
               min(_ingested_at) AS first_ingest,
               max(_ingested_at) AS last_ingest
        FROM {TARGET}
    """)
)
