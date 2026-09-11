# Telematics — real-time truck pipeline on Databricks Free Edition

A Bronze → Silver → Gold streaming pipeline over synthetic truck GPS pings,
joined against a static truck dimension, deployed to **dev / test / prod** as a
single Databricks Asset Bundle. Every object — schema, volumes, tables, columns,
jobs — is created by `databricks bundle deploy` and a bundle job. Nothing is
clicked in the UI.

```
landing volume (JSON)
      |  Auto Loader (cloudFiles + schemaLocation)
      v
bronze_pings_raw ---------------- append-only, raw, replayable
      |  try_cast - validate - dedup on sha2(truck_id||event_ts)
      +--------------> silver_pings_quarantine   (bad rows kept, not dropped)
      v
silver_pings -------------------- append-only (insert-only MERGE)
      |  stream-static join <----- truck_details (static dimension, broadcast)
      v
gold_truck_current_position      one row per truck: position + driver/depot/region
gold_region_activity_5m          pings & trucks per region per 5-minute window
```

| | |
|---|---|
| Engine | Structured Streaming (`foreachBatch` + Delta `MERGE`) |
| Trigger | `Trigger.AvailableNow` — drains, commits the checkpoint, exits |
| Table DDL | versioned SQL in `src/migrations`, applied by a bundle job |
| Targets | `dev` (manual) · `test` (hourly) · `prod` (every 15 min, deploy-only) |

### Documentation

| | |
|---|---|
| [DESIGN.md](DESIGN.md) | the choices and trade-offs, written to be defended |
| [EXPLAINER.md](EXPLAINER.md) | how it all works, in five passes from plain English to the theory underneath |
| [TROUBLESHOOTING.md](TROUBLESHOOTING.md) | every failure hit while building this, with cause and fix |
| [EVIDENCE.md](EVIDENCE.md) | live figures read back from all three targets; regenerate with `scripts/capture-evidence.ps1` |

### Verified on a real deployment

All three targets deployed and ran on Databricks Free Edition. Dev numbers:

```
bronze_pings_raw              247     raw rows across 20 generated files
silver_pings                  207     = 247 − 20 duplicates − 20 invalid
silver_pings_quarantine        20     one null-latitude row per file, kept not dropped
gold_truck_current_position    20     one row per truck, joined to the dimension
gold_region_activity_5m         4     4 regions × one 5-minute window
```

Per-target config visibly differs: 20 / 10 / 5 generator batches produce
207 / 94 / 38 Silver rows in dev / test / prod. Re-running `medallion_job` with
no new data left every count unchanged — the pipeline is idempotent in practice,
not just in principle.

---

## Repository layout

```
databricks.yml                  bundle + the three targets and their variables
resources/
  uc_objects.yml                UC schema and the landing / checkpoints volumes
  setup_job.yml                 migrations -> seed dimension -> generate pings
  medallion_job.yml             migrations -> bronze -> silver -> gold (scheduled)
src/
  common/telematics.py          config from job params, idempotent MERGE helper
  migrations/*.sql              every CREATE TABLE and ALTER TABLE, versioned
  jobs/run_migrations.py        applies migrations, ledger-backed and idempotent
  jobs/seed_truck_details.py    static ~20-truck dimension (MERGE, not overwrite)
  jobs/generate_pings.py        synthetic ping files into the landing volume
  jobs/bronze_ingest.py         Auto Loader -> bronze_pings_raw
  jobs/silver_clean.py          clean, dedup, quarantine -> silver_pings
  jobs/gold_build.py            stream-static join -> the two Gold tables
sql/verify.sql                  the queries worth screenshotting as evidence
scripts/deploy.{sh,ps1}         validate + deploy + run one target
.github/workflows/bundle.yml    validate all targets, deploy dev on main
```

---

## 0. One-time setup

**a. Databricks Free Edition account** — sign up, no card required.

**b. Databricks CLI** (v0.218+, the Go CLI — `databricks version` must not print
a Python version).

```bash
databricks configure --host https://<your-workspace>.cloud.databricks.com
```

```bash
databricks current-user me
```

The bundle does not pin `workspace.host`, so it uses your configured profile.
In a real multi-workspace setup you would pin a distinct host per target; this
exercise has one workspace, so the environments are separated by schema instead.

**c. Create the catalog — once.** A catalog is a workspace-level object shared by
all three targets, so it sits outside the bundle. Create it from the CLI, not
the UI:

```bash
databricks catalogs create telematics
```

If your account cannot create a catalog, point the bundle at one you can already
write to instead — no file edits needed:

```bash
databricks bundle deploy -t dev --var="catalog=workspace"
```

Everything below the catalog (schema, volumes, tables, jobs) belongs to the
bundle and is created by `deploy` + `run`.

---

## 1. Deploy and run — dev

```bash
databricks bundle validate -t dev
```

```bash
databricks bundle deploy -t dev
```

```bash
databricks bundle run setup_job -t dev
```

```bash
databricks bundle run medallion_job -t dev
```

Or all of it at once: `./scripts/deploy.sh dev --setup` (PowerShell:
`.\scripts\deploy.ps1 dev -Setup`).

`setup_job` applies migrations, seeds the truck dimension and writes ping files —
re-run it whenever you want more data. `medallion_job` is re-runnable at any
time: it resumes from its checkpoints, so a second run in a row picks up only
what is new.

## 2. Deploy and run — test

```bash
databricks bundle validate -t test && databricks bundle deploy -t test && databricks bundle run setup_job -t test && databricks bundle run medallion_job -t test
```

## 3. Deploy and run — prod

```bash
databricks bundle validate -t prod && databricks bundle deploy -t prod && databricks bundle run setup_job -t prod && databricks bundle run medallion_job -t prod
```

### What actually differs between the targets

| | dev | test | prod |
|---|---|---|---|
| Schema | `telematics.dev` | `telematics.test` | `telematics.prod` |
| Landing volume | `/Volumes/telematics/dev/landing` | `.../test/landing` | `.../prod/landing` |
| Job names | `telematics_medallion_dev` | `..._test` | `..._prod` |
| Schedule | paused (manual) | hourly | every 15 minutes |
| Generated batches | 20 | 10 | 5 |
| Bundle mode | `development` | default | `production` |
| Bundle root | user home | user home | restricted home path |

## 4. Verify

Open `sql/verify.sql` in a SQL editor, point `USE SCHEMA` at the target, and run
it. It checks row counts through the medallion, proves dedup happened
(`bronze_rows > silver_rows`, and `silver_rows == silver_distinct_keys`), shows
the join carrying driver/depot/region into Gold, and prints the migration ledger.

---

## 5. The promotion demo — a column change reaching prod through deploy only

Migration `002_add_geofence_to_gold.sql` adds `in_geofence` and `geofence_name`
to `gold_truck_current_position` and backfills them. It is the **last commit** in
this repo, so the promotion is reproducible two ways.

**Option A — via git, which is what the real flow looks like.** Deploy the
commit before the migration to all three targets, then deploy `main` to all
three:

```bash
git checkout HEAD~1
```

```bash
./scripts/deploy.sh dev --setup && ./scripts/deploy.sh test --setup && ./scripts/deploy.sh prod --setup
```

```bash
git checkout main
```

```bash
./scripts/deploy.sh dev && ./scripts/deploy.sh test && ./scripts/deploy.sh prod
```

**Option B — without rewinding git,** using the `migrate_through` job parameter
(the same idea as Flyway's `target` property):

```bash
databricks bundle run medallion_job -t dev --params migrate_through=001
```

```bash
databricks bundle run medallion_job -t dev
```

After each pass, in the target's schema:

```sql
DESCRIBE TABLE gold_truck_current_position;         -- the columns appear

SELECT geofence_name, in_geofence, count(*) AS trucks
FROM gold_truck_current_position GROUP BY 1, 2;     -- and are populated

SELECT * FROM schema_migrations ORDER BY version;   -- 002 recorded, with a timestamp
```

No `ALTER TABLE` is ever typed into a notebook or the UI, in any environment.

---

## 6. Evidence checklist

For the write-up, capture:

1. `databricks bundle deploy -t dev` / `-t test` / `-t prod` succeeding in the terminal.
2. The three job pairs in **Workflows**, with their different names and schedules.
3. A successful `medallion_job` run graph (migrations → bronze → silver → gold).
4. `gold_truck_current_position` populated, with driver/depot/region from the join.
5. `DESCRIBE TABLE gold_truck_current_position` **before and after** migration 002
   in `prod`, with only `bundle deploy` + `bundle run` in between.
6. `SELECT * FROM schema_migrations` in prod, showing both versions and when they landed.

---

## 7. CI

`.github/workflows/bundle.yml` validates all three targets on every push and PR,
and deploys + runs `dev` on `main`. It authenticates with repo secrets
`DATABRICKS_HOST` and `DATABRICKS_TOKEN`.

```bash
gh secret set DATABRICKS_HOST --body "https://<your-workspace>.cloud.databricks.com"
```

```bash
gh secret set DATABRICKS_TOKEN --body "<your personal access token>"
```

Promotion to `test` and `prod` is deliberately left manual; the workflow file
carries the gated job commented out, and DESIGN.md explains how it would be
wired with OIDC and a service principal.

---

## 8. Tear down

```bash
databricks bundle destroy -t dev
```

> No token or secret is committed to this repo. Auth comes from your local CLI
> profile or, in CI, from GitHub repo secrets.
