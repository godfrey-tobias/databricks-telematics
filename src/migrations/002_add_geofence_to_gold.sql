-- 002 — Add geofence columns to the Gold current-position table.
--
-- This is the change promoted dev -> test -> prod purely through
-- `databricks bundle deploy` + `databricks bundle run`. No ALTER TABLE is ever
-- typed into a notebook or the workspace UI, in any environment.
--
-- Safety:
--   * The ALTER is guarded by the migration ledger. ADD COLUMNS has no
--     IF NOT EXISTS, so the runner additionally treats an "already exists"
--     failure as already-applied — a lost ledger is recoverable, not fatal.
--   * The backfill is guarded by WHERE in_geofence IS NULL, so re-running it
--     touches nothing and cannot overwrite a value the pipeline has since
--     recomputed.
--   * gold_build.py already knows how to produce these columns and writes only
--     the ones the target has, so the job is correct deployed either side of
--     this migration. Deploy order and migration order are independent.
--
-- Rollback: forward-only. Ship 003 with
--   ALTER TABLE ... DROP COLUMN in_geofence, geofence_name
-- and promote it the same way. The table is created with
-- delta.columnMapping.mode = 'name' precisely so that drop/rename is available.

ALTER TABLE ${catalog}.${schema}.gold_truck_current_position
  ADD COLUMNS (
    in_geofence   BOOLEAN COMMENT 'True when the latest ping is inside the configured geofence box',
    geofence_name STRING  COMMENT 'Which geofence the flag refers to; comes from a bundle variable, so it can differ per environment'
  );

UPDATE ${catalog}.${schema}.gold_truck_current_position
SET in_geofence = (latitude  BETWEEN ${geofence_min_lat} AND ${geofence_max_lat})
                  AND (longitude BETWEEN ${geofence_min_lon} AND ${geofence_max_lon}),
    geofence_name = '${geofence_name}',
    _updated_at = current_timestamp()
WHERE in_geofence IS NULL;
