-- Verification queries. Run in a SQL editor against the target you deployed,
-- e.g. set catalog/schema to telematics.dev, telematics.test or telematics.prod.
-- These are the queries worth screenshotting as evidence.

USE CATALOG telematics;
USE SCHEMA dev;          -- change to test / prod

-- 1. Everything the bundle created, in one place.
SHOW TABLES;
SHOW VOLUMES;

-- 2. Row counts through the medallion. Quarantine should be non-zero:
--    the generator emits a null-latitude row on purpose.
SELECT 'bronze_pings_raw'        AS tbl, count(*) AS rows FROM bronze_pings_raw
UNION ALL SELECT 'silver_pings',            count(*) FROM silver_pings
UNION ALL SELECT 'silver_pings_quarantine', count(*) FROM silver_pings_quarantine
UNION ALL SELECT 'truck_details',           count(*) FROM truck_details
UNION ALL SELECT 'gold_truck_current_position', count(*) FROM gold_truck_current_position
UNION ALL SELECT 'gold_region_activity_5m',     count(*) FROM gold_region_activity_5m;

-- 3. Dedup actually happened: Bronze holds duplicates, Silver does not.
SELECT
  (SELECT count(*) FROM bronze_pings_raw)                              AS bronze_rows,
  (SELECT count(*) FROM silver_pings)                                  AS silver_rows,
  (SELECT count(*) FROM (SELECT DISTINCT ping_id FROM silver_pings))   AS silver_distinct_keys;

-- 4. The stream-static join: Gold carries driver/depot/region that only ever
--    existed in the static dimension.
SELECT truck_id, last_event_ts, round(latitude, 4) AS lat, round(longitude, 4) AS lon,
       make, model, driver, home_depot, region
FROM gold_truck_current_position
ORDER BY last_event_ts DESC
LIMIT 20;

-- 5. Windowed Gold output.
SELECT region, window_start, window_end, ping_count, truck_count
FROM gold_region_activity_5m
ORDER BY window_start DESC, region
LIMIT 20;

-- 6. The promoted migration. Before migration 002 these columns do not exist;
--    after `databricks bundle deploy` + a run, they do and they are populated.
DESCRIBE TABLE gold_truck_current_position;

SELECT geofence_name, in_geofence, count(*) AS trucks
FROM gold_truck_current_position
GROUP BY geofence_name, in_geofence
ORDER BY in_geofence DESC;

-- 7. The migration ledger — what was applied, when, and to which schema.
SELECT version, filename, applied_at, applied_by, statements
FROM schema_migrations
ORDER BY version;
