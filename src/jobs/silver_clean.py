# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — clean, conform, deduplicate, quarantine
# MAGIC
# MAGIC Streams `bronze_pings_raw` -> `silver_pings`, with rejects diverted to
# MAGIC `silver_pings_quarantine`.
# MAGIC
# MAGIC **Dedup key.** `ping_id = sha2(truck_id || raw event_ts)`. A truck at one
# MAGIC instant has one position, so that pair is the natural business key; hashing
# MAGIC it into a single column keeps the MERGE condition to one equality. The
# MAGIC generator's exact-duplicate row collapses onto the same key.
# MAGIC
# MAGIC **Why insert-only MERGE.** The write is `WHEN NOT MATCHED THEN INSERT` with
# MAGIC no MATCHED branch. That is idempotent — a replayed ping is matched and
# MAGIC skipped — *and* Delta executes it as an append, which keeps `silver_pings`
# MAGIC a valid streaming source for Gold. A full upsert would rewrite files and
# MAGIC Gold's stream would fail with "Detected a data update". First write wins is
# MAGIC also the right semantic for an immutable event.
# MAGIC
# MAGIC **Validation.** Bad rows are quarantined, not dropped, so data quality is
# MAGIC observable. `try_cast` turns an unparseable timestamp into a null and a
# MAGIC quarantine row rather than a failed run.

# COMMAND ----------

import os
import sys

for _candidate in ("../common", "src/common", "./common"):
    _p = os.path.abspath(os.path.join(os.getcwd(), _candidate))
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

from pyspark.sql import functions as F  # noqa: E402
from telematics import Config, merge_into, run_available_now  # noqa: E402

cfg = Config.from_widgets(dbutils)

SOURCE = cfg.table("bronze_pings_raw")
TARGET = cfg.table("silver_pings")
QUARANTINE = cfg.table("silver_pings_quarantine")
CHECKPOINT = cfg.checkpoint_path("silver_pings")

print(f"source     : {SOURCE}")
print(f"target     : {TARGET}")
print(f"quarantine : {QUARANTINE}")
print(f"checkpoint : {CHECKPOINT}")

# COMMAND ----------

bronze = spark.readStream.table(SOURCE)

typed = (
    bronze
    .withColumnRenamed("event_ts", "event_ts_raw")
    # Cast rather than to_timestamp with a format: the generator emits ISO-8601
    # with an offset, and try_cast handles it while returning null (not an
    # error) for anything malformed.
    .withColumn("event_ts", F.expr("try_cast(event_ts_raw AS TIMESTAMP)"))
    .withColumn("event_date", F.to_date("event_ts"))
    .withColumn("ping_id",
                F.sha2(F.concat_ws("||", F.col("truck_id"), F.col("event_ts_raw")), 256))
    .withColumn("_processed_at", F.current_timestamp())
)

# First failing rule wins, so the reason column says what to fix.
reject_reason = (
    F.when(F.col("truck_id").isNull() | (F.trim(F.col("truck_id")) == ""),
           "missing_truck_id")
     .when(F.col("event_ts_raw").isNull(), "missing_event_ts")
     .when(F.col("event_ts").isNull(), "unparseable_event_ts")
     .when(F.col("latitude").isNull() | F.col("longitude").isNull(),
           "missing_coordinates")
     .when(~F.col("latitude").between(-90.0, 90.0), "latitude_out_of_range")
     .when(~F.col("longitude").between(-180.0, 180.0), "longitude_out_of_range")
     .otherwise(F.lit(None).cast("string"))
)

validated = typed.withColumn("reject_reason", reject_reason)

# COMMAND ----------


def write_batch(batch_df, batch_id):
    """Land one micro-batch. Re-running a batch id produces the same table.

    No `.persist()` on the batch: serverless compute rejects it outright
    ([NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE). The batch is therefore
    evaluated once per branch. That is correct — a micro-batch is a fixed set
    of input files, so recomputation yields identical rows — and cheap at this
    volume. At scale the answer is a wider micro-batch, not a cache.
    """
    clean = (
        batch_df.filter(F.col("reject_reason").isNull())
        .select("ping_id", "truck_id", "event_ts", "event_date",
                "latitude", "longitude", "event_ts_raw",
                "_source_file", "_ingested_at", "_processed_at")
    )
    merge_into(spark, clean, TARGET, keys=["ping_id"], mode="insert_only",
               sequence_col="_ingested_at")

    rejects = (
        batch_df.filter(F.col("reject_reason").isNotNull())
        .withColumn(
            # A rejected row may have a null truck_id or timestamp, so the
            # Silver key is not usable. Fingerprint the whole row plus its
            # source file instead, so re-processing cannot duplicate it.
            "reject_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.col("truck_id"), F.lit("")),
                    F.coalesce(F.col("event_ts_raw"), F.lit("")),
                    F.coalesce(F.col("latitude").cast("string"), F.lit("")),
                    F.coalesce(F.col("longitude").cast("string"), F.lit("")),
                    F.coalesce(F.col("_source_file"), F.lit("")),
                ),
                256,
            ),
        )
        .withColumn("_quarantined_at", F.current_timestamp())
        .select("reject_id", "truck_id", "event_ts_raw", "latitude",
                "longitude", "reject_reason", "_source_file",
                "_quarantined_at")
    )
    merge_into(spark, rejects, QUARANTINE, keys=["reject_id"],
               mode="insert_only")

    # Deliberately no per-branch count(): without a cache each count is another
    # full pass over the batch. The post-stream cells below report the same
    # numbers from the tables, once.
    print(f"[silver] batch {batch_id}: merged")


writer = (
    validated.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .foreachBatch(write_batch)
)

run_available_now(writer, "silver_clean")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT
          (SELECT count(*) FROM {SOURCE})                            AS bronze_rows,
          (SELECT count(*) FROM {TARGET})                            AS silver_rows,
          (SELECT count(DISTINCT ping_id) FROM {TARGET})             AS silver_distinct_keys,
          (SELECT count(*) FROM {QUARANTINE})                        AS quarantined_rows
    """)
)

display(
    spark.sql(f"SELECT reject_reason, count(*) AS rows FROM {QUARANTINE} "
              f"GROUP BY reject_reason ORDER BY rows DESC")
)
