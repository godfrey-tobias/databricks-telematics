# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — stream-static join
# MAGIC
# MAGIC Streams `silver_pings` and joins it to the static `truck_details`
# MAGIC dimension, producing two business-ready outputs:
# MAGIC
# MAGIC | table | grain | meaning |
# MAGIC |---|---|---|
# MAGIC | `gold_truck_current_position` | one row per truck | latest position + truck, driver, depot, region |
# MAGIC | `gold_region_activity_5m` | region x 5-minute window | pings and distinct trucks per region |
# MAGIC
# MAGIC **How the static side is handled.** `truck_details` is read with
# MAGIC `spark.read.table(...)` *inside* `foreachBatch`, so each micro-batch sees the
# MAGIC dimension as of the moment it runs — a seeded change is picked up on the
# MAGIC next batch with no stream restart, and there is no risk of a stale
# MAGIC dimension pinned at stream-start. It is broadcast: 20 rows today, and even
# MAGIC at hundreds of thousands of trucks a dimension of that width is a few tens
# MAGIC of MB, well inside broadcast range. Past that, drop the broadcast hint and
# MAGIC let Databricks choose a shuffle-hash join, or switch the current-position
# MAGIC table to a Silver-only fact and join the dimension in the serving view.
# MAGIC
# MAGIC **Why both outputs are recomputed, not incremented.** The window aggregate
# MAGIC recomputes each touched window from `silver_pings` and MERGEs the result,
# MAGIC rather than adding to a running counter. A replayed batch therefore writes
# MAGIC the same numbers instead of double counting.
# MAGIC
# MAGIC **Forward-compatible writer.** The geofence columns are computed here
# MAGIC unconditionally, but `merge_into` writes only columns the target actually
# MAGIC has. So this notebook runs correctly both before and after migration `002`
# MAGIC adds them — deploy order and migration order are independent.

# COMMAND ----------

import os
import sys

for _candidate in ("../common", "src/common", "./common"):
    _p = os.path.abspath(os.path.join(os.getcwd(), _candidate))
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

from pyspark.sql import functions as F  # noqa: E402
from telematics import Config, merge_into, run_available_now, widget  # noqa: E402

cfg = Config.from_widgets(dbutils)

SOURCE = cfg.table("silver_pings")
DIMENSION = cfg.table("truck_details")
CURRENT_POSITION = cfg.table("gold_truck_current_position")
REGION_ACTIVITY = cfg.table("gold_region_activity_5m")
CHECKPOINT = cfg.checkpoint_path("gold_build")

GEOFENCE_NAME = widget(dbutils, "geofence_name", "chicago_loop")
GEOFENCE = {
    "min_lat": float(widget(dbutils, "geofence_min_lat", "41.80")),
    "max_lat": float(widget(dbutils, "geofence_max_lat", "41.95")),
    "min_lon": float(widget(dbutils, "geofence_min_lon", "-87.75")),
    "max_lon": float(widget(dbutils, "geofence_max_lon", "-87.55")),
}

WINDOW = "5 minutes"

print(f"stream     : {SOURCE}")
print(f"static     : {DIMENSION}")
print(f"targets    : {CURRENT_POSITION}, {REGION_ACTIVITY}")
print(f"checkpoint : {CHECKPOINT}")
print(f"geofence   : {GEOFENCE_NAME} {GEOFENCE}")

# COMMAND ----------


def in_geofence_expr():
    return (
        F.col("latitude").between(GEOFENCE["min_lat"], GEOFENCE["max_lat"])
        & F.col("longitude").between(GEOFENCE["min_lon"], GEOFENCE["max_lon"])
    )


def build_current_position(batch_df, trucks):
    """Latest ping per truck, enriched from the static dimension."""
    latest = (
        batch_df.groupBy("truck_id")
        .agg(F.max(F.struct("event_ts", "latitude", "longitude")).alias("latest"))
        .select(
            "truck_id",
            F.col("latest.event_ts").alias("last_event_ts"),
            F.col("latest.latitude").alias("latitude"),
            F.col("latest.longitude").alias("longitude"),
        )
    )

    return (
        latest.join(F.broadcast(trucks), on="truck_id", how="left")
        .withColumn("in_geofence", in_geofence_expr())
        .withColumn("geofence_name", F.lit(GEOFENCE_NAME))
        .withColumn("_updated_at", F.current_timestamp())
    )


def build_region_activity(batch_df, trucks):
    """Recompute every 5-minute window this batch touched, from Silver.

    Recomputing (rather than incrementing) is what makes the aggregate safe to
    replay: the same batch produces the same counts.
    """
    touched = [
        row["window_start"]
        for row in batch_df.select(
            F.window("event_ts", WINDOW).getField("start").alias("window_start")
        ).distinct().collect()
    ]
    if not touched:
        return None

    scoped = (
        spark.read.table(SOURCE)
        .withColumn("w", F.window("event_ts", WINDOW))
        .filter(F.col("w.start").isin(touched))
    )

    return (
        scoped.join(F.broadcast(trucks), on="truck_id", how="left")
        .groupBy(F.coalesce(F.col("region"), F.lit("UNKNOWN")).alias("region"),
                 F.col("w.start").alias("window_start"),
                 F.col("w.end").alias("window_end"))
        .agg(F.count(F.lit(1)).alias("ping_count"),
             F.countDistinct("truck_id").alias("truck_count"))
        .withColumn("_updated_at", F.current_timestamp())
    )


def write_batch(batch_df, batch_id):
    """Build both Gold outputs from one micro-batch.

    No `.persist()`: serverless compute rejects it
    ([NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE), so the batch is evaluated
    once per output. Correct either way — a micro-batch is a fixed set of input
    files — and the extra pass is cheap at this volume.
    """
    if batch_df.isEmpty():
        print(f"[gold] batch {batch_id}: empty, nothing to do")
        return

    # Read the static side per micro-batch so dimension changes are picked up
    # without restarting the stream.
    trucks = spark.read.table(DIMENSION).select(
        "truck_id", "make", "model", "capacity_lbs",
        "home_depot", "region", "driver",
    )

    positions = build_current_position(batch_df, trucks)
    merge_into(spark, positions, CURRENT_POSITION, keys=["truck_id"],
               mode="upsert", sequence_col="last_event_ts")

    activity = build_region_activity(batch_df, trucks)
    if activity is not None:
        merge_into(spark, activity, REGION_ACTIVITY,
                   keys=["region", "window_start"], mode="upsert")

    print(f"[gold] batch {batch_id}: merged")


# COMMAND ----------

silver = spark.readStream.table(SOURCE)

writer = (
    silver.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .foreachBatch(write_batch)
)

run_available_now(writer, "gold_build")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT truck_id, last_event_ts, round(latitude, 4) AS lat,
               round(longitude, 4) AS lon, make, model, driver, home_depot, region
        FROM {CURRENT_POSITION}
        ORDER BY last_event_ts DESC
        LIMIT 20
    """)
)

display(
    spark.sql(f"""
        SELECT region, window_start, window_end, ping_count, truck_count
        FROM {REGION_ACTIVITY}
        ORDER BY window_start DESC, region
        LIMIT 20
    """)
)
