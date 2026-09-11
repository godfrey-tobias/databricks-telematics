-- 001 — Create the medallion tables.
--
-- Idempotent by construction (CREATE TABLE IF NOT EXISTS) and by ledger: the
-- migration runner records what it has applied. Re-deploying and re-running is
-- a no-op, which is what makes `prod` safe to redeploy at any time.
--
-- Placeholders ${catalog} / ${schema} are substituted by the runner from the
-- job parameters, so the same file creates the dev, test and prod tables.

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.bronze_pings_raw (
  truck_id      STRING    COMMENT 'Truck identifier exactly as emitted',
  latitude      DOUBLE    COMMENT 'Raw latitude, not yet validated',
  longitude     DOUBLE    COMMENT 'Raw longitude, not yet validated',
  event_ts      STRING    COMMENT 'Raw ISO-8601 timestamp string, unparsed',
  _rescued_data STRING    COMMENT 'Auto Loader rescued column: anything that did not fit the schema',
  _source_file  STRING    COMMENT 'Landing file this row came from',
  _ingested_at  TIMESTAMP COMMENT 'When Auto Loader wrote the row'
)
USING DELTA
COMMENT 'Bronze: append-only landing of raw GPS pings. Deliberately dumb — no casting, no filtering.';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.silver_pings (
  ping_id       STRING    NOT NULL COMMENT 'sha2(truck_id || raw event_ts) — the dedup key',
  truck_id      STRING    COMMENT 'Validated truck identifier',
  event_ts      TIMESTAMP COMMENT 'Parsed event timestamp (UTC)',
  event_date    DATE      COMMENT 'Date of event_ts, for pruning',
  latitude      DOUBLE    COMMENT 'Validated latitude, -90..90',
  longitude     DOUBLE    COMMENT 'Validated longitude, -180..180',
  event_ts_raw  STRING    COMMENT 'Original timestamp string, kept for lineage',
  _source_file  STRING    COMMENT 'Landing file this row came from',
  _ingested_at  TIMESTAMP COMMENT 'Bronze ingest time',
  _processed_at TIMESTAMP COMMENT 'Silver processing time'
)
USING DELTA
CLUSTER BY (truck_id, event_date)
COMMENT 'Silver: cleaned, typed, deduplicated pings. Append-only by construction so Gold can stream from it.';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.silver_pings_quarantine (
  reject_id      STRING    NOT NULL COMMENT 'Row fingerprint — stable so re-processing does not duplicate',
  truck_id       STRING,
  event_ts_raw   STRING,
  latitude       DOUBLE,
  longitude      DOUBLE,
  reject_reason  STRING    COMMENT 'Why the row failed validation',
  _source_file   STRING,
  _quarantined_at TIMESTAMP
)
USING DELTA
COMMENT 'Rows that failed Silver validation. Quarantined rather than dropped so bad data is observable, not invisible.';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.truck_details (
  truck_id     STRING NOT NULL COMMENT 'Join key to the ping stream',
  make         STRING,
  model        STRING,
  capacity_lbs INT,
  home_depot   STRING,
  region       STRING,
  driver       STRING,
  _updated_at  TIMESTAMP COMMENT 'When this row was last seeded/changed'
)
USING DELTA
COMMENT 'Static truck dimension (~20 rows). The static side of the stream-static join in Gold.';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.gold_truck_current_position (
  truck_id      STRING NOT NULL,
  last_event_ts TIMESTAMP COMMENT 'Timestamp of the most recent ping seen for this truck',
  latitude      DOUBLE,
  longitude     DOUBLE,
  make          STRING,
  model         STRING,
  capacity_lbs  INT,
  home_depot    STRING,
  region        STRING,
  driver        STRING,
  _updated_at   TIMESTAMP
)
USING DELTA
CLUSTER BY (truck_id)
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'Gold: one row per truck — latest position enriched with truck/driver/depot. Column mapping is on so a column can be renamed or dropped by a later migration.';

CREATE TABLE IF NOT EXISTS ${catalog}.${schema}.gold_region_activity_5m (
  region       STRING NOT NULL,
  window_start TIMESTAMP NOT NULL,
  window_end   TIMESTAMP,
  ping_count   BIGINT COMMENT 'Pings in this region in this 5-minute window',
  truck_count  BIGINT COMMENT 'Distinct trucks seen in this region in this window',
  _updated_at  TIMESTAMP
)
USING DELTA
CLUSTER BY (region, window_start)
TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
COMMENT 'Gold: pings per region per 5-minute tumbling window. Recomputed from Silver, never incremented, so it is idempotent.';
