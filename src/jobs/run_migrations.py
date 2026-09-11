# Databricks notebook source
# MAGIC %md
# MAGIC # Apply schema migrations
# MAGIC
# MAGIC Table creation and every structural change are versioned SQL files under
# MAGIC `src/migrations`, applied by this task. Nothing in this repo — and nothing
# MAGIC in any environment — creates or alters a table any other way.
# MAGIC
# MAGIC Two independent guarantees make a re-run a no-op:
# MAGIC
# MAGIC 1. **The ledger.** `schema_migrations` records every applied version with a
# MAGIC    checksum; an unchanged version is skipped.
# MAGIC 2. **The statements.** Written idempotently where SQL allows
# MAGIC    (`CREATE TABLE IF NOT EXISTS`, backfills guarded by `WHERE col IS NULL`).
# MAGIC    Where it does not — `ALTER TABLE ... ADD COLUMNS` has no `IF NOT EXISTS`
# MAGIC    — an "already exists" failure is treated as *already applied*, so a lost
# MAGIC    or truncated ledger is recoverable instead of fatal.

# COMMAND ----------

import glob
import hashlib
import os
import sys

for _candidate in ("../common", "src/common", "./common"):
    _p = os.path.abspath(os.path.join(os.getcwd(), _candidate))
    if os.path.isdir(_p) and _p not in sys.path:
        sys.path.insert(0, _p)

from telematics import Config, widget  # noqa: E402

cfg = Config.from_widgets(dbutils)

# Upper bound on the version to apply, Flyway's `target` idea. Defaults to
# "everything". The promotion demo uses it to hold migration 002 back without
# rewinding git.
migrate_through = widget(dbutils, "migrate_through", "9999")

# Any other ${placeholder} a migration may reference. Passing config in rather
# than hard-coding it keeps the SQL environment-agnostic.
substitutions = {
    "catalog": cfg.catalog,
    "schema": cfg.schema,
    "geofence_name": widget(dbutils, "geofence_name", "chicago_loop"),
    "geofence_min_lat": widget(dbutils, "geofence_min_lat", "41.80"),
    "geofence_max_lat": widget(dbutils, "geofence_max_lat", "41.95"),
    "geofence_min_lon": widget(dbutils, "geofence_min_lon", "-87.75"),
    "geofence_max_lon": widget(dbutils, "geofence_max_lon", "-87.55"),
}

MIGRATIONS_DIR = next(
    os.path.abspath(p)
    for p in (
        os.path.join(os.getcwd(), "..", "migrations"),
        os.path.join(os.getcwd(), "src", "migrations"),
    )
    if os.path.isdir(os.path.abspath(p))
)

LEDGER = cfg.table("schema_migrations")

print(f"target schema     : {cfg.catalog}.{cfg.schema}")
print(f"migrations folder : {MIGRATIONS_DIR}")
print(f"applying up to    : {migrate_through}")

# COMMAND ----------

# The ledger is the one object the runner creates directly — it is the
# bootstrap, so it cannot itself be a migration.
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {LEDGER} (
      version    STRING    NOT NULL COMMENT 'Numeric prefix of the migration filename',
      filename   STRING    COMMENT 'Migration applied',
      checksum   STRING    COMMENT 'sha256 of the file, so an edited migration is detectable',
      applied_at TIMESTAMP,
      applied_by STRING,
      statements INT       COMMENT 'How many statements the file contained'
    )
    USING DELTA
    COMMENT 'Ledger of applied schema migrations. Managed by the telematics bundle.'
""")

applied = {r["version"]: r["checksum"] for r in spark.table(LEDGER).collect()}
print(f"already applied   : {sorted(applied) or '(none — fresh environment)'}")

# COMMAND ----------

# ALTER TABLE ... ADD COLUMNS has no IF NOT EXISTS. These messages mean the
# change is already in place, which is success, not failure.
ALREADY_APPLIED = ("already exists", "already_exists", "fields already exist",
                   "field_already_exists", "duplicate column")

QUOTES = {"'": "'", '"': '"', "`": "`"}


def split_statements(sql_text):
    """Split a migration into statements on top-level semicolons only.

    A regex split on ";" is wrong, and wrong in a way that is easy to miss: a
    COMMENT string containing a semicolon gets torn in half and the fragment
    fails with a PARSE_SYNTAX_ERROR pointing at a stray quote, nowhere near the
    real cause. So this walks the text tracking whether it is inside a string
    literal (single, double or backtick, with '' escaping) or a comment, and
    only breaks on a semicolon that is outside both.

    Comments are dropped rather than passed through, so a `--` inside a string
    survives while a real comment does not.
    """
    statements, current = [], []
    quote = None          # the closing character of the string we are inside
    line_comment = False
    block_comment = False
    i, n = 0, len(sql_text)

    while i < n:
        char = sql_text[i]
        nxt = sql_text[i + 1] if i + 1 < n else ""

        if line_comment:
            if char == "\n":
                line_comment = False
                current.append(char)
            i += 1
        elif block_comment:
            if char == "*" and nxt == "/":
                block_comment = False
                i += 2
            else:
                i += 1
        elif quote:
            current.append(char)
            if char == quote:
                if nxt == quote:          # '' inside a string is an escaped quote
                    current.append(nxt)
                    i += 2
                    continue
                quote = None
            i += 1
        elif char == "-" and nxt == "-":
            line_comment = True
            i += 2
        elif char == "/" and nxt == "*":
            block_comment = True
            i += 2
        elif char in QUOTES:
            quote = QUOTES[char]
            current.append(char)
            i += 1
        elif char == ";":
            statements.append("".join(current))
            current = []
            i += 1
        else:
            current.append(char)
            i += 1

    statements.append("".join(current))
    return [s.strip() for s in statements if s.strip()]


def render(sql_text):
    for key, value in substitutions.items():
        sql_text = sql_text.replace("${" + key + "}", str(value))
    return sql_text


results = []

for path in sorted(glob.glob(os.path.join(MIGRATIONS_DIR, "*.sql"))):
    filename = os.path.basename(path)
    version = filename.split("_")[0]

    with open(path, "r", encoding="utf-8") as handle:
        raw = handle.read()
    checksum = hashlib.sha256(raw.encode("utf-8")).hexdigest()

    if version > migrate_through:
        results.append((version, filename, "held back"))
        print(f"-- {filename}: held back (migrate_through={migrate_through})")
        continue

    if applied.get(version) == checksum:
        results.append((version, filename, "already applied"))
        print(f"-- {filename}: already applied, skipping")
        continue

    if version in applied:
        # Migration history is append-only. An edited file is a mistake worth
        # shouting about, but the statements are safe to re-apply.
        print(f"!! {filename}: checksum differs from the applied version — "
              f"re-applying. Roll forward with a NEW migration instead of "
              f"editing history.")

    statements = split_statements(render(raw))
    print(f">> {filename}: applying {len(statements)} statement(s)")

    for i, statement in enumerate(statements, start=1):
        try:
            spark.sql(statement)
            print(f"   [{i}/{len(statements)}] ok")
        except Exception as exc:  # noqa: BLE001 — narrowed by the message check
            message = str(exc).lower()
            if any(token in message for token in ALREADY_APPLIED):
                print(f"   [{i}/{len(statements)}] already in place, skipping")
                continue
            print(f"   [{i}/{len(statements)}] FAILED:\n{statement}")
            raise

    spark.sql(f"DELETE FROM {LEDGER} WHERE version = '{version}'")
    spark.sql(f"""
        INSERT INTO {LEDGER}
        SELECT '{version}', '{filename}', '{checksum}', current_timestamp(),
               current_user(), {len(statements)}
    """)
    results.append((version, filename, "applied"))

# COMMAND ----------

for version, filename, outcome in results:
    print(f"{version}  {filename:<45} {outcome}")

display(spark.sql(f"SELECT * FROM {LEDGER} ORDER BY version"))
