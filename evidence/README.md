# Evidence

Screenshots from the live Databricks Free Edition workspace, mapped to the four
things the exercise asks to see. Figures in [../EVIDENCE.md](../EVIDENCE.md) are
read back from the same workspace by `scripts/capture-evidence.ps1`.

| Requirement | Screenshot | What it shows |
|---|---|---|
| Bundle deploying from the CLI to all three targets | `01-deploy-three-targets.png` | One `databricks bundle deploy` per target, each reporting `Resources: … 5 unchanged` — deploys are idempotent too |
| Assets running in the workspace | `02-workflows-jobs.png` | All six jobs with per-target names and `dev`/`test`/`prod` tags; Trigger column reads **Paused** for dev against **Scheduled** for test and prod |
| " | `03a-prod-schedule-and-runs.png` | `telematics_medallion_prod`: **Every 15 minutes**, `migrate_through` job parameter, and ~25 consecutive green runs across all four tasks |
| " | `03b-prod-run-graph.png` | The task graph `apply_migrations → bronze_ingest → silver_clean → gold_build`, all Serverless, all Succeeded. Metrics show **Rows read 119, Rows written 0** — a scheduled run with no new data is a no-op, which is idempotency visible in the UI |
| Gold table populated, with the join, plus a sample query | `04-gold-joined-prod.png` | Query against `telematics.prod.gold_truck_current_position` returning `make`, `model`, `driver`, `home_depot`, `region` — columns that exist **only** in the static `truck_details` dimension. Their presence is the stream–static join |
| One column change promoted to prod purely via deploy | `05a-prod-before.png` | `DESCRIBE TABLE` on prod **before**: last column is `geofence_name`, footer reads **16 rows** |
| " | `05b-deploy-promotion.png` | `bundle deploy` + `bundle run` for dev, test and prod. No `ALTER TABLE` typed anywhere |
| " | `05c-prod-after.png` | Same `DESCRIBE` **after**: `capacity_tons` at row 14, footer reads **17 rows** |
| " | `05d-prod-migration-ledger.png` | `schema_migrations` in prod: `003` applied at 15:40, hours after `001` (02:24) and `002` (02:30) — it arrived in its own deploy |
| " | `05e-prod-capacity-populated.png` | The new column populated by the migration's backfill: 34000 lb → 15.42 t, 20000 → 9.07, 26000 → 11.79, 40000 → 18.14 |
| Stretch: CI | `06-github-actions.png` | Green build on `main`, and the repo's **Public** badge |

## The promotion, in order

Migration `003_add_capacity_tons.sql` was committed, deployed and run — nothing
else. The ledger timestamps record where it landed and when:

| | 001 | 002 | 003 |
|---|---|---|---|
| dev | 02:08 | 02:27 | **15:27** |
| test | 02:21 | 02:29 | **15:39** |
| prod | 02:24 | 02:30 | **15:40** |

## A note on the red marks in `02-workflows-jobs.png`

The dev jobs show failed runs in their history. Those are real: the
`mode: development` schema rename and the `persist()`-on-serverless failure,
both fixed and both written up in [../TROUBLESHOOTING.md](../TROUBLESHOOTING.md).
They are left in place rather than hidden — the history is what actually
happened, and the fixes are in the commit log.
