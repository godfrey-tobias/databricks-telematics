# EXPLAINER — how this thing works, from the ground up

This document explains the project in five passes. Each pass covers the *same*
system at a deeper level. Read until it stops being useful; come back when you
need the next layer.

- [Level 1 — the idea, in plain words](#level-1--the-idea-in-plain-words)
- [Level 2 — the moving parts](#level-2--the-moving-parts)
- [Level 3 — reading the actual code](#level-3--reading-the-actual-code)
- [Level 4 — the hard parts, and why they are done this way](#level-4--the-hard-parts-and-why-they-are-done-this-way)
- [Level 5 — the theory underneath](#level-5--the-theory-underneath)
- [Glossary](#glossary)

---

## Level 1 — the idea, in plain words

Imagine a fleet of 20 delivery trucks. Every few seconds each truck radios in:
*"I'm truck 7, I'm at this latitude and longitude, and it's 2:15pm."* That
message is called a **ping**.

Separately, in a filing cabinet, you have one index card per truck: its make,
model, how much it can carry, which depot it belongs to, and who drives it.
That never changes minute to minute. That's the **truck details**.

The pings are useless on their own — `TRK-007 is at 41.88, -87.70` tells a
dispatcher nothing. What they actually want is:

> *"D. Patel, driving the Freightliner out of the Chicago depot, is currently
> inside the city geofence."*

Getting from the first sentence to the second is the entire project. You take a
fast-moving stream of dumb messages, clean it up, and glue the slow-moving
filing cabinet onto it.

### Why it happens in three stages

The pipeline does this in three steps, and the steps have colour-coded names
that are an industry convention — the **medallion architecture**:

| Stage | Nickname | What happens | Kitchen analogy |
|---|---|---|---|
| **Bronze** | raw | Copy messages in, change nothing | Groceries dumped on the counter, still in bags |
| **Silver** | clean | Fix types, drop junk, remove duplicates | Washed, peeled, chopped |
| **Gold** | useful | Join to truck details, answer a question | The finished dish, plated |

Why not do it in one step? Because when something goes wrong — and it will —
you need to know *where*. If Bronze is a perfect untouched copy of what arrived,
you can always replay from it. If you cleaned the data on the way in and threw
away the original, a bug in your cleaning code has destroyed data you can never
get back.

**Keep Bronze dumb.** That's the rule, and it's the whole reason the stage
exists.

### And why all the deployment machinery?

Real companies don't have one copy of a system. They have three:

- **dev** — where you break things on purpose
- **test** — where you check it still works
- **prod** — the real one, the one the business depends on

The rule that matters: **nobody touches prod by hand.** Not to add a column,
not to fix a row, not "just this once". Every change gets typed into a file,
committed to git, and *deployed*. If you can't deploy it, it doesn't happen.

That discipline is what a **Databricks Asset Bundle** gives us: one folder of
configuration that can build all three environments from the same code.

---

## Level 2 — the moving parts

### The flow

```
  generate_pings.py
        │  writes JSON files
        ▼
  /Volumes/telematics/dev/landing/          ← a folder in cloud storage
        │  Auto Loader notices new files
        ▼
  bronze_pings_raw     247 rows    (raw, includes junk and duplicates)
        │  cast types, validate, deduplicate
        ├──────────────► silver_pings_quarantine    20 rows  (the junk, kept)
        ▼
  silver_pings         207 rows    (clean, one row per real ping)
        │  join to the filing cabinet
        │◄──────────────  truck_details   20 rows   (make, driver, depot…)
        ▼
  gold_truck_current_position   20 rows   (one per truck: where it is + who drives it)
  gold_region_activity_5m        4 rows   (pings per region per 5 minutes)
```

Those row counts are from the real dev run. They're worth understanding,
because they *prove* the pipeline works:

- Generator wrote 247 rows across 20 files.
- Each file deliberately contains **one exact duplicate** and **one broken row**
  (null latitude). 20 files → 20 duplicates + 20 broken.
- 247 − 20 broken = 227 valid-ish rows; 227 − 20 duplicates = **207**. ✅
- The 20 broken rows went to quarantine, not the bin. ✅

### Where things live

Databricks organises data in three levels, like folders:

```
catalog          telematics          ← the whole project
  └── schema     dev / test / prod   ← one per environment
        ├── tables    bronze_pings_raw, silver_pings, …
        └── volumes   landing/, checkpoints/    ← for files, not tables
```

A **table** holds rows. A **volume** holds files. Pings arrive as files (into
`landing`), so they need a volume; once ingested they're rows, so they live in
tables.

### The two jobs

A **job** is a scheduled sequence of tasks. There are two:

**`setup_job`** — run by hand when you want data:
1. `apply_migrations` — make sure all the tables exist
2. `seed_truck_details` — fill the filing cabinet
3. `generate_pings` — write fake ping files

**`medallion_job`** — the actual pipeline, on a schedule:
1. `apply_migrations` — *again*, because structure changes must arrive before data
2. `bronze_ingest`
3. `silver_clean`
4. `gold_build`

### The three environments

Same code, different settings. Nothing in the Python knows which environment
it's in — it's told, via job parameters:

| | dev | test | prod |
|---|---|---|---|
| Schema | `telematics.dev` | `telematics.test` | `telematics.prod` |
| Schedule | paused (manual) | hourly | every 15 minutes |
| Ping batches generated | 20 | 10 | 5 |
| Silver rows produced | 207 | 94 | 38 |

That last row is the proof the configuration is really taking effect.

---

## Level 3 — reading the actual code

### Bronze — [src/jobs/bronze_ingest.py](src/jobs/bronze_ingest.py)

```python
spark.readStream.format("cloudFiles")          # "cloudFiles" = Auto Loader
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.schemaHints", "truck_id string, latitude double, …")
    .option("rescuedDataColumn", "_rescued_data")
    .load(cfg.landing_path)
```

Line by line:

- **`readStream`** — not `read`. A normal read answers *"what's in this folder?"*
  once. A stream answers *"what's in this folder, and tell me again whenever
  something new shows up."*
- **Auto Loader (`cloudFiles`)** — tracks which files it has already seen, so it
  never processes the same file twice. Without it you'd have to write that
  bookkeeping yourself, and it's harder than it sounds.
- **`schemaLocation`** — a folder where Auto Loader writes down the shape of the
  data it has seen. If a new field appears in the JSON next week, it notices
  rather than crashing.
- **`schemaHints`** — *"trust me: latitude is a double."* Without this, Auto
  Loader guesses from a sample, and a guess that disagrees with the table
  definition breaks the write.
- **`rescuedDataColumn`** — anything that doesn't fit the expected shape lands
  in `_rescued_data` instead of being silently dropped.

Then the write:

```python
.option("checkpointLocation", CHECKPOINT)
```

The **checkpoint** is the stream's memory. It records exactly how far it got. If
the job dies halfway, the next run reads the checkpoint and resumes instead of
starting over. Delete the checkpoint and the stream reprocesses everything from
the beginning.

### Silver — [src/jobs/silver_clean.py](src/jobs/silver_clean.py)

The dedup key:

```python
F.sha2(F.concat_ws("||", F.col("truck_id"), F.col("event_ts_raw")), 256)
```

Glue the truck id and the timestamp together, hash it. A truck can only be in
one place at one instant, so *(truck, time)* uniquely identifies a ping. Two
rows with the same hash are the same ping, and the second one is a duplicate.

Validation uses a first-failure-wins chain:

```python
F.when(F.col("truck_id").isNull(), "missing_truck_id")
 .when(F.col("event_ts").isNull(), "unparseable_event_ts")
 .when(F.col("latitude").isNull() | F.col("longitude").isNull(), "missing_coordinates")
 .when(~F.col("latitude").between(-90.0, 90.0), "latitude_out_of_range")
 …
 .otherwise(None)          # None = the row is fine
```

Rows with a reason go to quarantine. Rows without go to Silver. **Nothing is
deleted** — if you drop bad rows you can never answer "how bad is our data?"

Note `try_cast` rather than `cast`:

```python
F.expr("try_cast(event_ts_raw AS TIMESTAMP)")
```

`cast` throws and kills the job on a malformed timestamp. `try_cast` returns
null, which the validation chain turns into a quarantined row. One bad message
should never stop the pipeline.

### Gold — [src/jobs/gold_build.py](src/jobs/gold_build.py)

The stream–static join:

```python
def write_batch(batch_df, batch_id):
    trucks = spark.read.table(DIMENSION)      # ← read INSIDE the function
    positions = latest.join(F.broadcast(trucks), on="truck_id", how="left")
```

Two details matter enormously here:

**Reading inside the function.** `foreachBatch` calls `write_batch` once per
micro-batch. Reading `truck_details` inside means every batch sees the current
filing cabinet. If you read it *outside*, you'd capture a snapshot at stream
start and a driver change from three weeks ago would still be invisible.

**`broadcast`.** Normally joining two tables means shuffling both across the
network, which is expensive. But `truck_details` is 20 rows. `broadcast` says
*"just send a copy to every machine"* — then each machine joins locally and
nothing moves. Valid whenever the small side genuinely is small.

### Migrations — [src/jobs/run_migrations.py](src/jobs/run_migrations.py)

This is the part most beginner projects skip, and it's the most valuable part.

The tables are **not** created by the pipeline code. They're created by numbered
SQL files:

```
src/migrations/001_create_medallion_tables.sql
src/migrations/002_add_geofence_to_gold.sql
```

The runner reads them in order, applies them, and records what it applied in a
table called `schema_migrations`:

```
version | filename                        | applied_at
001     | 001_create_medallion_tables.sql | 2026-09-11T02:24:45Z
002     | 002_add_geofence_to_gold.sql    | 2026-09-11T02:30:28Z
```

Why this matters: **you now have an audit trail.** You can look at prod and say
with certainty when each column appeared and who put it there. And because the
runner skips anything already recorded, re-running is harmless.

---

## Level 4 — the hard parts, and why they are done this way

### Idempotency: the property everything depends on

**Idempotent** means: doing it twice has the same effect as doing it once.

Why it matters — distributed systems fail *midway*. The network drops, a machine
dies, someone hits cancel. Your recovery plan is always "run it again". That
plan only works if running it again is safe.

Proven on the real deployment: running `medallion_job` twice with no new data
left the counts at exactly 247 / 207 / 20 both times.

Three mechanisms deliver it:

1. **Checkpoints** — the stream knows where it stopped.
2. **`MERGE` instead of `INSERT`** — *"add this row if it isn't there"* rather
   than *"add this row"*.
3. **The migration ledger** — already-applied migrations are skipped.

Note that these overlap on purpose. If you lose a checkpoint, the MERGE still
prevents duplicates. Belt *and* braces, because the cost of duplicated financial
or safety data is far higher than the cost of a redundant safeguard.

### Why Silver uses an "insert-only" MERGE

A normal `MERGE` does two things: update rows that match, insert rows that
don't. Silver deliberately only does the second half:

```sql
MERGE INTO silver_pings AS t USING source AS s ON t.ping_id = s.ping_id
WHEN NOT MATCHED THEN INSERT (…) VALUES (…)
-- deliberately no WHEN MATCHED clause
```

Two reasons, and the second one is subtle:

1. **Semantics.** A ping is a historical fact. Truck 7 *was* at that spot at
   2:15pm. Facts don't get updated. First write wins.

2. **It keeps Silver streamable.** Delta tables can be used as a streaming
   source — that's how Gold reads Silver. But a stream can only read a table
   that is *append-only*. If Silver updated rows in place, Gold's stream would
   die with `Detected a data update`. An insert-only MERGE compiles down to an
   append, so Silver stays a legal source.

That second reason is the sort of thing that's invisible until it bites.

### Why the region aggregate is recomputed, not incremented

The obvious way to count pings per region per 5 minutes is a counter:

```python
new_count = old_count + rows_in_this_batch     # ← tempting, and wrong
```

It's wrong because it's not idempotent. Replay a batch and every count is
inflated. Since replay is *routine*, the counter is guaranteed to drift.

What the code does instead: work out which 5-minute windows this batch touched,
then **recompute those windows from scratch** out of Silver, and MERGE the
answer:

```python
touched = [distinct window starts in this batch]
scoped = spark.read.table(SILVER).filter(window_start.isin(touched))
agg = scoped.groupBy(region, window).agg(count(…), countDistinct(…))
```

Replaying now writes the same numbers. It costs a re-read of a slice of Silver,
and that's a price worth paying: **prefer recomputing a derived value over
maintaining it incrementally**, unless you've measured that you can't afford to.

### The expand/contract pattern

Here's a genuine ordering problem. You want to add a column to a Gold table.
Two things must happen — the table gains the column, and the job starts writing
it. Which goes first?

- Job first → it writes a column that doesn't exist → crash.
- Migration first → the column exists but sits empty until the job catches up.

The trick used here removes the question. `merge_into` writes **only the columns
the target table actually has**:

```python
target_cols = table_columns(spark, target)
cols = [c for c in df.columns if c in target_cols]
```

So `gold_build.py` computes `in_geofence` unconditionally. Before migration 002
the column isn't in the table, so it's silently not written. After, it is. The
job is correct on both sides of the change, and deploy order stops mattering.

This is called **expand/contract** (or parallel change): make the change
backwards-compatible first, migrate, then clean up.

### Why forward-only migrations

There's no "undo" script. To reverse migration 002 you write migration 003 that
drops the column.

That feels like extra work, but rollback scripts are a trap: they're rarely
tested, and they run in the exact moment when things are already on fire. A
forward migration goes through the identical deploy-and-verify path as every
other change.

This is why the Gold tables are created with:

```sql
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
```

Without column mapping, Delta identifies columns by position in the underlying
files, and `DROP COLUMN` / `RENAME COLUMN` are impossible. With it, columns have
stable names, so a future migration *can* undo this one.

### Trigger.AvailableNow

A stream can run two ways:

- **Continuously** — always on, waiting for data, latency in seconds, meter
  always running.
- **`Trigger.AvailableNow`** — start, process everything waiting, stop.

This project uses `AvailableNow`, with the job schedule controlling latency
(prod every 15 minutes). You get genuine streaming semantics — checkpoints,
exactly-once progress, restart safety — but compute shuts down between runs,
which matters on Free Edition's quota.

The important point: **this is a scheduling decision, not an architectural
one.** The switch to continuous is one line. Nothing about the correctness of
Bronze, Silver or Gold changes.

---

## Level 5 — the theory underneath

### Exactly-once is a lie; effectively-once is achievable

No distributed system can guarantee a message is *delivered* exactly once —
that's a consequence of the Two Generals problem. What you can guarantee is that
the *effect* happens once, and you get it by combining:

- **at-least-once delivery** (retry until acknowledged — the checkpoint), and
- **idempotent application** (applying twice ≡ applying once — the MERGE).

Structured Streaming's contract is precisely this pair. The checkpoint is a
write-ahead log of batch boundaries; sink idempotency is *your* job. Delta gives
you it for free on `toTable` appends (batch ids are recorded transactionally),
which is why Bronze needs no MERGE. The moment you drop into `foreachBatch` you
leave that guarantee behind and must reconstruct it — hence the MERGE keys in
Silver and Gold.

### The stream–static join is a temporal join with an unstated assumption

Joining a stream to a static table quietly assumes the dimension is
**slowly-changing relative to the stream**, and that using the *current* value
for *historical* events is acceptable.

For "where is this truck now", that's right. For "what did this shipment cost at
the time", it's wrong — you'd need a temporal join against an SCD2 dimension
with validity intervals (`valid_from`/`valid_to`), matching each event to the
row that was live when the event occurred.

This project takes the first reading, deliberately. Knowing that you've made
that choice — rather than defaulting into it — is what separates a correct
pipeline from a lucky one.

### Watermarks, and why this design sidesteps them

Streaming aggregations normally need a **watermark**: a declaration of how late
data is allowed to be, which lets the engine release state for closed windows.
Without one, state grows forever. With one, late arrivals past the threshold are
dropped.

The recompute-touched-windows strategy needs no watermark and no state, because
the aggregate isn't maintained — it's derived from Silver on demand. A ping
arriving three days late still lands in the right window, since its window is
simply recomputed.

The trade-off is read amplification: each batch re-reads a slice of Silver. At
20 trucks that's free. At 200,000 it isn't, and the correct move is to adopt a
watermarked stateful aggregation with `update` output mode and accept a
late-data cutoff. **The right answer depends on scale, and changes with it.**

### Why schema-as-code is really about the CAP of organisations

The deploy-only-prod rule isn't a technical constraint — you *could* type
`ALTER TABLE` into a prod notebook. It's an organisational one.

The moment prod's schema can diverge from the repo, the repo stops being a
description of reality and becomes a description of intent. Every subsequent
deploy is a gamble on whether they still agree. The migration ledger makes
divergence *detectable* (checksums) and *rare* (there's an easier path — commit
and deploy).

That's the deep reason the checksum exists. It isn't there to catch corruption.
It's there to catch a human editing history, which is the one failure mode a
version-numbered migration system cannot otherwise see.

### Where this design would break

Being honest about limits is more useful than claiming there are none:

- **Dedup MERGE over an unbounded table.** Silver's MERGE scans for matching
  `ping_id`s across the whole table. At billions of rows this dominates. The fix
  is a bounded predicate (`event_date >= current_date() - 1`), which trades
  *dedup of very-late duplicates* for *a bounded scan*. That's a real semantic
  loss, consciously taken.
- **`countDistinct` in the region aggregate.** Exact distinct counting is
  O(cardinality) in memory. At scale you'd move to HyperLogLog
  (`approx_count_distinct`) and accept ~2% error.
- **Broadcast of the dimension.** Fine to roughly 100MB. Past that you're
  shuffling, and the current-position table should stop denormalising and let a
  serving view do the join.
- **Directory-listing file discovery.** Auto Loader lists the landing directory
  by default — O(files). Past ~100k files you need file-notification mode, which
  is a queue subscription rather than a listing.

---

## Glossary

| Term | Meaning |
|---|---|
| **Auto Loader** | Databricks feature that incrementally ingests new files and remembers which ones it has seen |
| **Bundle (DAB)** | A folder of YAML + code that deploys a whole project to a workspace |
| **Catalog / schema / table** | Unity Catalog's three-level namespace: project → environment → data |
| **Checkpoint** | A stream's saved position, so it can resume instead of restarting |
| **Delta** | The storage format underneath; gives tables transactions, versions and `MERGE` |
| **Expand/contract** | Making a change backwards-compatible so deploy order doesn't matter |
| **Idempotent** | Doing it twice has the same effect as doing it once |
| **Medallion** | The Bronze → Silver → Gold layering convention |
| **MERGE** | SQL for "update if present, insert if not" |
| **Micro-batch** | One small chunk of a stream, processed as a unit |
| **Migration** | A numbered SQL file that changes table structure |
| **Ping** | One GPS message from one truck |
| **Quarantine** | Keeping bad rows in a side table instead of discarding them |
| **Serverless** | Compute you don't configure; Databricks starts and stops it |
| **Stream–static join** | Joining fast-moving events to a slow-moving reference table |
| **Target** | One environment's configuration within a bundle (dev / test / prod) |
| **Volume** | A folder for files in Unity Catalog (as opposed to a table of rows) |
| **Watermark** | A declared bound on how late data may arrive |
