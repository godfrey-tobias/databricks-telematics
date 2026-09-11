# DESIGN

Key choices and the trade-offs behind them.

## Structured Streaming rather than Lakeflow / DLT

Lakeflow would have been less code — expectations instead of a hand-written
quarantine branch, and no checkpoint management. I chose hand-rolled Structured
Streaming because the exercise puts table structure under version control and
requires `prod` to be deploy-only. A declarative pipeline owns the DDL of the
tables it materialises, which makes "this column was added by migration 002, at
this timestamp, by this deploy" something the framework decides rather than
something the repo states. With `foreachBatch` + `MERGE` I own the write, so the
table contract lives in `src/migrations` and the pipeline only fills it in.

The cost is real: I hand-roll dedup, quarantine and idempotency that DLT gives
for free, and I own checkpoint hygiene. For a larger estate of similar pipelines
I would flip the decision and use Lakeflow, keeping only the Gold serving tables
under explicit migration control.

## Dedup key

`ping_id = sha2(truck_id || raw event_ts, 256)`. A truck has one position at one
instant, so that pair is the natural business key; hashing it to one column keeps
the MERGE condition a single equality. Duplicates are collapsed twice: once
inside the micro-batch (Delta rejects a source that matches a target row more
than once), then again by the MERGE itself across batches.

The Silver write is **insert-only** — `WHEN NOT MATCHED THEN INSERT`, no MATCHED
branch. Two reasons. First, a ping is an immutable event, so first-write-wins is
the correct semantic. Second, Delta executes an insert-only merge as an append,
which keeps `silver_pings` a legal streaming source for Gold; a full upsert
rewrites files and Gold's stream would fail with *Detected a data update*. If
Silver ever does need in-place updates, the fix is `skipChangeCommits` on Gold's
reader, with the data-loss caveat that it skips the whole commit.

## Trigger choice

`Trigger.AvailableNow` everywhere: each task resumes from its checkpoint, drains
what has arrived, commits, and exits, so serverless compute shuts down between
runs. That matters on Free Edition quota, and it makes the scheduled job the
thing that sets latency — 15 minutes in prod, hourly in test, manual in dev. The
semantics are identical to a continuous stream; only the arrival cadence
differs. Switching to always-on is one line
(`.trigger(processingTime="30 seconds")` in `run_available_now`) plus dropping
`awaitTermination`'s expectation of completion.

## The stream–static join

Gold streams `silver_pings` and reads `truck_details` with `spark.read.table()`
**inside `foreachBatch`**. So the dimension is re-read once per micro-batch: a
change to a driver or depot is visible on the next batch with no stream restart,
and there is no risk of a snapshot silently pinned at stream-start time — which
is what happens if you resolve the static side outside the batch function.

It is broadcast. Twenty rows today; at hundreds of thousands of trucks a
dimension this narrow is still tens of MB, comfortably inside broadcast range.
Past that, drop the hint and let the optimiser pick a shuffle-hash join, or stop
denormalising in Gold entirely: keep the facts keyed by `truck_id` and join the
dimension in a serving view.

Re-reading per batch costs a metadata read and a broadcast per batch. With a
large dimension I would cache it and refresh on a TTL, or drive refresh from the
dimension's Change Data Feed.

`gold_region_activity_5m` is **recomputed, not incremented**. Each batch collects
the distinct 5-minute windows it touched, re-aggregates those windows from
`silver_pings`, and MERGEs the result. A replayed batch therefore writes the same
numbers, where a running counter would double count. The trade-off is re-reading
a slice of Silver per batch, which liquid clustering on `(truck_id, event_date)`
keeps cheap.

## Checkpoints and restart

One checkpoint directory per stream per environment, inside that environment's
own `checkpoints` volume. Consequences: killing a job mid-run and re-running it
resumes rather than reprocessing; tearing down one target cannot disturb another;
and re-running `medallion_job` twice in a row is a near no-op. Idempotency does
not *depend* on the checkpoint, though — the MERGEs are idempotent on their own,
so a lost checkpoint means reprocessing, not duplicate rows.

Auto Loader's `schemaLocation` lives beside the checkpoint, with `schemaHints`
pinning the four known fields to the types migration 001 declared, so inference
can never disagree with the table. `_rescued_data` catches anything unexpected
rather than dropping it.

## What differs across dev / test / prod

One bundle, three targets, one workspace. Everything environment-specific is a
bundle variable, and no code branches on environment: schema (`telematics.dev` /
`test` / `prod`), landing and checkpoint volume paths, job names, schedule
(paused / hourly / every 15 minutes) and generated data volume (20 / 10 / 5
batches). `dev` uses `mode: development`; `prod` uses `mode: production` with an
explicit `run_as` and a bundle root under `/Workspace/Shared` rather than a
user's home.

In a real workspace the separation would also be a grant model: the prod schema
would be owned by a service principal, with humans holding `SELECT` and job
`RUN` but no `MODIFY` or `CREATE TABLE`. Free Edition is single-identity, so
that discipline is enforced by process here rather than by privilege — worth
saying out loud rather than pretending otherwise.

## Table and column changes as code

Every `CREATE TABLE` and `ALTER TABLE` is a numbered SQL file in
`src/migrations`, applied by the `apply_migrations` task that runs first in
*both* jobs. There is no other path by which a table changes in any environment.

Idempotency is guaranteed twice over:

- **The ledger.** `schema_migrations` records version, filename, sha256, time
  and identity. An unchanged version is skipped. An edited version is loudly
  re-applied and flagged — history is append-only, roll forward with a new file.
- **The statements.** `CREATE TABLE IF NOT EXISTS`; the 002 backfill is guarded
  by `WHERE in_geofence IS NULL`. Where SQL offers no guard —
  `ALTER TABLE ... ADD COLUMNS` has no `IF NOT EXISTS` — the runner treats an
  "already exists" error as *already applied*, so a lost ledger is recoverable
  instead of fatal.

Migration `002` is the promoted change: it adds `in_geofence` and
`geofence_name` to `gold_truck_current_position` and backfills them from the
geofence bounds, which are passed in as job parameters so the SQL stays
environment-agnostic. It reaches dev, then test, then prod through
`databricks bundle deploy` plus a run, and nothing else.

The Gold job is deliberately **forward-compatible**: it computes the geofence
columns unconditionally, and the MERGE helper writes only columns the target
actually has. So the job can be deployed before or after the migration and is
correct either way — the expand/contract pattern. That is what removes the
ordering hazard from a promotion.

### Rollback

Forward-only. To undo 002 you ship `003_drop_gold_geofence.sql` with
`ALTER TABLE ... DROP COLUMN` and promote it the same way — which is why the
Gold tables are created with `delta.columnMapping.mode = 'name'`, since drop and
rename need it. Because the writer tolerates a missing column, 003 needs no code
change to accompany it. For a bad *data* migration rather than a bad structural
one, Delta time travel is the faster lever: `RESTORE TABLE ... TO VERSION AS OF`,
recorded as its own migration so the ledger still tells the truth.

## Scaling from 20 trucks to hundreds of thousands

- **Ingest.** Auto Loader already scales; at high file counts switch
  `cloudFiles` from directory listing to file notification mode so discovery
  stops being O(files). Have producers write into date/hour-partitioned
  prefixes.
- **Bronze/Silver.** Unchanged in shape. `maxFilesPerTrigger` /
  `maxBytesPerTrigger` bound micro-batch size; liquid clustering on
  `(truck_id, event_date)` keeps the MERGE's file pruning effective without a
  partition-count explosion. The dedup MERGE becomes the hot spot — I would scope
  it with a window predicate (`event_date >= current_date() - 1`) so it rewrites
  recent files only, and accept that a ping arriving days late is dropped rather
  than deduped.
- **Gold.** Current-position is one row per truck, so it stays small, but the
  MERGE fans out; cluster by `truck_id` and consider sharding by region. The
  windowed aggregate's recompute-touched-windows strategy is the piece that
  needs revisiting: past a certain rate I would move it to a watermarked
  streaming aggregation with `update` output mode and accept late-data cutoffs
  in exchange for not re-reading Silver.
- **The dimension.** Stop broadcasting per batch; cache with a TTL, or drive
  refresh from Change Data Feed.
- **Operations.** Continuous triggers instead of AvailableNow, autoscaling
  serverless, and per-stream alerting on backlog rather than on job failure.

## CI

`.github/workflows/bundle.yml` runs `databricks bundle validate` for all three
targets on every push and PR, then deploys and runs `dev` on `main`. Validating
all three is the point: a change that only breaks prod cannot pass on dev.

It authenticates with a PAT in repo secrets, which is fine for Free Edition and
wrong for anything real. The path forward is OIDC — GitHub's token exchanged for
a Databricks service principal credential, so no long-lived secret exists — plus
a GitHub Environment on the prod job with required reviewers, so promotion needs
a human approval but never a human `ALTER TABLE`. The prod job is in the
workflow file, commented, showing that shape.

## What I would do next

1. Data-quality monitoring on the quarantine table (a threshold alert on reject
   rate, not just a row count).
2. A smoke-test task after Gold that asserts row counts and key uniqueness and
   fails the run, so a bad deploy is caught by the job rather than by a reader.
3. Move the dimension seed out of the pipeline entirely — in reality
   `truck_details` is an SCD2 sourced from a system of record, not a seed script.
