-- 003 — Add a derived capacity column to the Gold current-position table.
--
-- The second promoted change. Like 002, it reaches every environment only by
-- `databricks bundle deploy` followed by a run; no ALTER TABLE is typed into a
-- notebook or the workspace UI anywhere.
--
-- Self-contained on purpose: the column is derived from data the table already
-- holds, so no pipeline code has to change alongside it. That makes it a clean
-- demonstration of the migration path on its own.
--
-- Safety:
--   * The ALTER is guarded by the migration ledger, and the runner treats an
--     "already exists" failure as already-applied, so a lost ledger is
--     recoverable rather than fatal.
--   * The backfill is guarded by WHERE capacity_tons IS NULL, so re-running it
--     touches nothing.
--
-- Rollback: forward-only. Ship 004 with
--   ALTER TABLE ... DROP COLUMN capacity_tons
-- which is available because the table is created with
-- delta.columnMapping.mode = 'name'.

ALTER TABLE ${catalog}.${schema}.gold_truck_current_position
  ADD COLUMNS (
    capacity_tons DOUBLE COMMENT 'Rated capacity in metric tons, derived from capacity_lbs'
  );

UPDATE ${catalog}.${schema}.gold_truck_current_position
SET capacity_tons = round(capacity_lbs / 2204.62, 2),
    _updated_at = current_timestamp()
WHERE capacity_tons IS NULL;
