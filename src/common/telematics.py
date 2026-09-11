"""Shared helpers for the telematics medallion pipeline.

Imported by the notebooks in ``src/jobs``. Standard library + PySpark only:
Free Edition blocks ``%pip install``, so there are no third-party dependencies.

The notebooks find this module by adding ``../common`` to ``sys.path`` — the
bundle syncs the whole repo into the workspace, so the relative layout of
``src/jobs`` and ``src/common`` is preserved after ``databricks bundle deploy``.
"""

from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


def widget(dbutils, name, default=""):
    """Declare a notebook widget and read it. Job `base_parameters` land here."""
    dbutils.widgets.text(name, default)
    return dbutils.widgets.get(name).strip()


@dataclass(frozen=True)
class Config:
    """Everything a job needs to know about the environment it is running in.

    There is no environment detection anywhere in the code: the target the
    bundle was deployed to decides these values, and they arrive as job
    parameters. The same notebook bytes run in dev, test and prod.
    """

    catalog: str
    schema: str
    landing_volume: str = "landing"
    checkpoints_volume: str = "checkpoints"

    @classmethod
    def from_widgets(cls, dbutils):
        return cls(
            catalog=widget(dbutils, "catalog", "telematics"),
            schema=widget(dbutils, "schema", "dev"),
            landing_volume=widget(dbutils, "landing_volume", "landing"),
            checkpoints_volume=widget(dbutils, "checkpoints_volume", "checkpoints"),
        )

    def table(self, name):
        return f"{self.catalog}.{self.schema}.{name}"

    @property
    def landing_path(self):
        return f"/Volumes/{self.catalog}/{self.schema}/{self.landing_volume}"

    def checkpoint_path(self, stream_name):
        """One checkpoint directory per stream per environment.

        Keeping checkpoints inside the environment's own volume means a target
        can be torn down and rebuilt without touching any other target, and
        that re-running a job resumes instead of reprocessing.
        """
        return (
            f"/Volumes/{self.catalog}/{self.schema}/{self.checkpoints_volume}"
            f"/{stream_name}"
        )


def table_columns(spark, table):
    return [f.name for f in spark.table(table).schema.fields]


def merge_into(spark, df: DataFrame, target: str, keys, mode="upsert",
               sequence_col=None):
    """Idempotent write of ``df`` into the Delta table ``target``.

    ``mode="insert_only"`` emits a MERGE with no MATCHED clause. Delta turns
    that into an append, which is what keeps Silver a valid streaming source
    for Gold while still being safe to re-run: a ping that has already landed
    is matched and skipped rather than duplicated.

    ``mode="upsert"`` also updates matched rows, optionally guarded by
    ``sequence_col`` so a late-arriving older record can never overwrite a
    newer one.

    Only columns that exist in the target are written. That is deliberate: it
    lets a job that already knows how to produce a new column be deployed
    *before* the migration that adds the column, and keeps working after —
    the expand/contract pattern that makes the dev -> test -> prod promotion
    in this repo safe in either order.
    """
    target_cols = table_columns(spark, target)
    cols = [c for c in df.columns if c in target_cols]
    skipped = [c for c in df.columns if c not in target_cols]
    if skipped:
        print(f"[merge_into] {target}: target has no column(s) {skipped} yet — "
              f"not written (deploy the migration that adds them).")

    df = df.select(*cols)

    # Delta refuses a MERGE whose source matches a target row more than once,
    # so collapse duplicates in the incoming batch first. This is the dedup
    # step for Silver: the generator emits the same ping twice.
    if sequence_col and sequence_col in cols:
        ordering = Window.partitionBy(*keys).orderBy(F.col(sequence_col).desc())
        df = (df.withColumn("_rn", F.row_number().over(ordering))
                .filter(F.col("_rn") == 1)
                .drop("_rn"))
    else:
        df = df.dropDuplicates(list(keys))

    view = "src_" + target.replace(".", "_").replace("-", "_")
    df.createOrReplaceTempView(view)

    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    insert_cols = ", ".join(cols)
    insert_vals = ", ".join(f"s.{c}" for c in cols)

    matched = ""
    if mode == "upsert":
        updatable = [c for c in cols if c not in keys]
        if updatable:
            guard = ""
            if sequence_col and sequence_col in cols:
                guard = f" AND s.{sequence_col} >= t.{sequence_col}"
            assignments = ", ".join(f"t.{c} = s.{c}" for c in updatable)
            matched = f"WHEN MATCHED{guard} THEN UPDATE SET {assignments}"

    spark.sql(f"""
        MERGE INTO {target} AS t
        USING {view} AS s
        ON {on}
        {matched}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)


def run_available_now(stream_writer, name, table=None):
    """Start a stream with Trigger.AvailableNow and block until it drains.

    AvailableNow gives us real streaming semantics — checkpointed, exactly-once
    progress, restartable — while letting the job finish so serverless compute
    shuts down. Swapping in ``processingTime`` for an always-on stream is a
    one-line change; see DESIGN.md.

    Pass ``table`` to write into a Unity Catalog table, otherwise the writer is
    expected to already carry its sink (``foreachBatch``).
    """
    writer = stream_writer.trigger(availableNow=True).queryName(name)
    query = writer.toTable(table) if table else writer.start()
    query.awaitTermination()
    progress = query.lastProgress
    rows = progress.get("numInputRows", 0) if progress else 0
    print(f"[{name}] finished — {rows} row(s) in the final micro-batch, "
          f"checkpoint committed.")
    return query
