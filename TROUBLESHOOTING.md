# TROUBLESHOOTING — what broke, why, and how it was fixed

Every failure below actually happened while building and deploying this project
on Databricks Free Edition, on Windows 11 with PowerShell 5.1. They're recorded
in the order they were hit, with the real error text, so they're searchable.

Nothing here is hypothetical.

**Jump to:** [Environment](#part-1--environment-and-cli) ·
[Databricks platform](#part-2--databricks-platform) ·
[Bundle config](#part-3--bundle-configuration) ·
[Pipeline code](#part-4--pipeline-code) ·
[Tooling](#part-5--tooling-and-scripting) ·
[Patterns](#patterns-worth-keeping)

---

## Part 1 — Environment and CLI

### 1.1 `databricks` is not recognized, immediately after installing it

```
databricks : The term 'databricks' is not recognized as the name of a cmdlet,
function, script file, or operable program.
```

**Cause.** Two separate things, stacked:

1. The CLI was *already* installed (`winget` reported
   `Found an existing package already installed`), but winget hadn't created its
   usual shim in `%LOCALAPPDATA%\Microsoft\WinGet\Links`. The real binary sat in
   `…\WinGet\Packages\Databricks.DatabricksCLI_…\databricks.exe`.
2. That folder *was* on the persistent user PATH — but every terminal tab inside
   the Claude desktop app inherits the **app's** environment, captured when the
   app launched. Opening a new tab doesn't re-read PATH. Only a new app process
   does.

**Diagnosis.** Read the registry rather than the process environment, so you're
looking at what's persisted rather than what was inherited:

```powershell
(Get-ItemProperty -Path 'HKCU:\Environment' -Name Path).Path -split ';'
```

If the entry is there but `Get-Command databricks` fails, it's a stale
environment, not a broken install.

**Fix.** Any of:

```powershell
# per-tab, immediate
$env:Path = [Environment]::GetEnvironmentVariable('Path','Machine') + ';' +
            [Environment]::GetEnvironmentVariable('Path','User')
```

…or restart the desktop app (permanent), or use a PowerShell window opened
outside the app.

**Lesson.** "Command not found" right after an install is almost never a failed
install. Check whether the *binary exists* and whether the *shell can see it* as
two separate questions.

---

### 1.2 `git init` fails with `Filename too long`

```
fatal: cannot stat '…/.git/hooks/fsmonitor-watchman.sample': Filename too long
```

**Cause.** The initial working directory was a temporary scratch workspace
nested ~10 levels deep under `AppData\Local\Packages\…\LocalCache\Roaming\…`.
Adding `.git/hooks/fsmonitor-watchman.sample` pushed past the Windows 260-char
`MAX_PATH` limit.

**Fix.** Move the repo somewhere short — here,
`C:\Users\gotob\source\repos\databricks-telematics`.

**Lesson.** On Windows, keep repos near the drive root. `git config --system
core.longpaths true` helps but doesn't cover every tool in the chain.

---

## Part 2 — Databricks platform

### 2.1 Creating a catalog fails on Free Edition

```
Error: Metastore storage root URL does not exist. Default Storage is enabled in
your account. You can use the UI to create a new catalog using Default Storage,
or please provide a storage location for the catalog.
```

**Cause.** `databricks catalogs create` goes through the Unity Catalog API,
which wants an explicit managed storage location. Free Edition uses **Default
Storage**, where there is no such location to name — the platform manages it.

**Fix.** Create the catalog through SQL instead, which understands Default
Storage. Via the Statement Execution API, so it's still CLI-driven and
scriptable rather than hand-clicked:

```powershell
$body = @{
  warehouse_id = "<warehouse id>"
  statement    = "CREATE CATALOG IF NOT EXISTS telematics"
  wait_timeout = "50s"
} | ConvertTo-Json -Compress

$tmp = Join-Path $env:TEMP 'stmt.json'
[System.IO.File]::WriteAllText($tmp, $body, (New-Object System.Text.UTF8Encoding $false))
databricks api post /api/2.0/sql/statements --json "@$tmp"
```

Get the warehouse id from `databricks warehouses list`. The warehouse can be
`STOPPED` — submitting a statement wakes it.

**Alternative.** Skip the catalog entirely and use one that exists:

```bash
databricks bundle deploy -t dev --var="catalog=workspace"
```

**Lesson.** The REST API and the SQL layer are not equally capable. When one
refuses, the other is worth trying before concluding it can't be done.

---

### 2.2 `PERSIST TABLE is not supported on serverless compute`

```
[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on serverless
compute. SQLSTATE: 0A000
```

Raised from inside `foreachBatch`, wrapped in a `StreamingQueryException`, so
the top-level message was the unhelpful `[STREAM_FAILED] … terminated with
exception`.

**Cause.** The `foreachBatch` function cached the micro-batch:

```python
batch_df = batch_df.persist()     # illegal on serverless
```

because the batch was scanned twice — once for clean rows, once for rejects.
Serverless compute rejects `persist`/`cache` outright.

**Fix.** Remove it, and remove the per-branch `count()` logging that had
justified it (each `count()` was another full pass, so keeping them without a
cache was the worst of both worlds):

```python
# No .persist(): serverless rejects it. The batch is evaluated once per branch.
# Correct either way — a micro-batch is a fixed set of input files, so
# recomputation yields identical rows.
```

**Lesson.** Serverless trades control for convenience. Caching, cluster config
and several Spark internals are simply unavailable. Recomputation is the default
answer, and at micro-batch scale it's usually fine — a wider micro-batch beats a
cache anyway.

---

## Part 3 — Bundle configuration

### 3.1 `mode: development` silently renamed the Unity Catalog schema

```
[SCHEMA_NOT_FOUND] The schema `telematics.dev` cannot be found.
```

…even though `databricks bundle deploy` had just reported `Created schemas.env`.

**Cause.** `mode: development` prefixes **every** resource name with
`[dev <user>] ` — and that includes UC schemas, where it is sanitised into a
legal identifier. The deploy created:

```
telematics.dev_godfreytobias7_dev        ← what actually got created
telematics.dev                           ← what the job parameters said
```

The attempted override didn't work either:

```yaml
presets:
  name_prefix: ""     # ignored — an empty string reads as "unset"
```

**Diagnosis.** Compare what the bundle *says* against what the workspace *has*:

```bash
databricks schemas list telematics
```

**Fix — two layers.**

1. Drop `mode: development` for the dev target and set its behaviours explicitly
   (paused schedule, `max_concurrent_runs: 1`). Nothing is lost, and the schema
   is `dev` in all three environments.

2. More importantly, stop letting the parameter and the deployed object drift.
   Reference the **resource**, not the variable:

   ```yaml
   # before — a literal that can disagree with reality
   schema: ${var.env_schema}

   # after — the deployed name, by construction
   schema: ${resources.schemas.env.name}
   ```

   Now whatever a target's presets do to names, the tasks are pointed at the
   object that actually exists.

**Lesson.** Fix #2 is the real fix. Whenever a bundle names a deployed object,
reference the resource rather than re-deriving the name — the class of bug
disappears instead of this instance of it.

---

### 3.2 Deploy refuses to proceed: "requires destructive actions"

```
This action will result in the deletion or recreation of the following volumes.
Error: the deployment requires destructive actions, but the current console
does not support prompting. To proceed, use --auto-approve.
```

**Cause.** Renaming the schema (fix 3.1) meant the volumes underneath it had to
be recreated. The CLI won't do that non-interactively without explicit consent.

**Fix.** Confirm what's actually at risk *before* reaching for the flag:

```bash
databricks fs ls dbfs:/Volumes/telematics/<schema>/landing
databricks tables list telematics <schema>
```

Both were empty — the failed run never wrote a byte — so `--auto-approve` was
safe. Then, and only then:

```bash
databricks bundle deploy -t dev --auto-approve
```

**Lesson.** This guard is doing its job. The habit worth keeping is *verify,
then approve* — never reflexively add the flag because a command failed. On a
non-empty prod volume the right answer would have been a migration, not a
recreate.

---

### 3.3 Prod bundle root warning

```
Warning: the bundle root path /Workspace/Shared/.bundle/telematics/prod is
writable by all workspace users
```

**Cause.** `/Workspace/Shared` was chosen to keep prod out of any individual
user's home. But Shared is world-writable, which for a deploy-only prod is
exactly backwards — anyone could edit the deployed notebooks.

**Fix.** Put prod under the deploying principal's restricted path:

```yaml
prod:
  mode: production
  workspace:
    root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
```

On a single-identity Free Edition workspace this *is* the restricted folder. In
a real workspace it would be a service principal's path, with humans holding
`SELECT` on tables and `RUN` on jobs but no `MODIFY` anywhere.

**Lesson.** "Shared" reads like the neutral, team-friendly choice. For
production artifacts it's the opposite of what you want.

---

## Part 4 — Pipeline code

### 4.1 `PARSE_SYNTAX_ERROR` from a semicolon inside a string literal

```
[PARSE_SYNTAX_ERROR] Syntax error at or near '''. SQLSTATE: 42601
```

**Cause.** The migration runner split files into statements with a naive split
on `;`. Migration 002 contains:

```sql
in_geofence BOOLEAN COMMENT 'Which geofence the flag refers to; comes from a bundle variable'
                                                              ^ here
```

The split tore the statement in half, leaving a fragment with one unbalanced
quote. The error points at the quote — nowhere near the actual cause.

Worse, the old code *documented* the constraint:

```python
# Migrations in this repo deliberately contain no semicolons or `--` inside
# string literals, which keeps the splitter this simple.
```

A rule like that survives exactly until someone writes a natural-sounding
comment.

**Diagnosis.** Reproduce the split locally — no cluster needed:

```python
stmts = raw.split(";")
for i, s in enumerate(stmts, 1):
    if s.count("'") % 2:
        print(f"[{i}] UNBALANCED QUOTES: {s[:80]}")
```

**Fix.** A character scanner that tracks string literals (single, double,
backtick, with `''` escaping) and comments, and breaks only on a *top-level*
semicolon — see `split_statements` in
[src/jobs/run_migrations.py](src/jobs/run_migrations.py). Migration 002 keeps
its semicolon deliberately, as a standing regression test.

**Lesson.** If correctness depends on a convention nobody is checking, it's a
latent bug with a delay fuse. Either enforce the rule or remove the need for it.

---

### 4.2 `.trigger()` after `.toTable()` has no effect

Caught by reading, before it ever ran — worth recording because it fails
*silently*.

```python
# wrong — toTable() starts the query and returns a StreamingQuery
writer = df.writeStream.option(...).toTable(TARGET)
run_available_now(writer, "bronze")      # .trigger() on a running query: no-op

# right — configure fully, then start
writer = df.writeStream.option(...)
writer.trigger(availableNow=True).queryName(name).toTable(TARGET)
```

**Lesson.** `DataStreamWriter` methods are builders and return the builder;
`start()` and `toTable()` are terminal and return a `StreamingQuery`. Anything
chained after a terminal call is operating on the wrong object.

---

### 4.3 Why Silver's MERGE has no `WHEN MATCHED` clause

Not a failure that occurred — a failure that was *designed out*, and the
reasoning is easy to lose.

If Silver used a full upsert, Delta would rewrite files, and Gold's stream over
Silver would eventually die with:

```
Detected a data update … This is currently not supported.
```

An **insert-only** MERGE (`WHEN NOT MATCHED THEN INSERT`, no MATCHED branch)
compiles to an append, so Silver stays a legal streaming source — and
first-write-wins is the correct semantic for immutable events anyway.

**If Silver ever does need in-place updates**, the escape hatch is
`.option("skipChangeCommits", "true")` on Gold's reader — with the caveat that
it skips whole commits, so an update mixed with inserts can drop the inserts.

---

## Part 5 — Tooling and scripting

### 5.1 The CLI's JSON parser rejects a BOM

```
Error: error decoding JSON at C:\…\dbsql.json:1:1: invalid character 'ï'
looking for beginning of value
```

**Cause.** PowerShell's `Out-File -Encoding utf8` writes a **BOM** in Windows
PowerShell 5.1. `ï` is the first byte of `EF BB BF` misread as Latin-1.

**Fix.**

```powershell
[System.IO.File]::WriteAllText($path, $json, (New-Object System.Text.UTF8Encoding $false))
```

**Lesson.** `-Encoding utf8` in PowerShell 5.1 means UTF-8-*with*-BOM. For
anything a non-Windows tool will read, write the file through .NET.

---

### 5.2 PowerShell 5.1 mangles multi-line JSON from native commands

Parsing `databricks jobs list --output json` produced a single object whose
properties were arrays — two jobs merged into one:

```
id=846230062742037 627287858894153  name=[dev …] telematics_medallion_dev [dev …] telematics_setup_dev
```

**Cause.** A native command's stdout arrives as a **string array**, one element
per line. `ConvertFrom-Json` in 5.1 processes pipeline input per element rather
than reassembling it.

**Fix.** Force a single string first:

```powershell
$parsed = databricks jobs list --output json | Out-String | ConvertFrom-Json
```

---

### 5.3 `--job-id $obj.property` doesn't bind

```
Error: invalid argument "--limit" for "--job-id" flag: strconv.ParseInt:
parsing "--limit": invalid syntax
```

…and later, once the object was an array:

```
Error: accepts 0 arg(s), received 1
```

**Cause.** Property access in native-command argument position is unreliable in
PowerShell 5.1. When it yields nothing, `--job-id` swallows the *next* flag —
hence the bizarre "`--limit` is not a valid integer".

**Fix.** Hoist into a plain typed variable first:

```powershell
$jobId = [int64]$job.job_id
databricks jobs list-runs --job-id $jobId --limit 1
```

Better still, avoid the lookup: `databricks bundle summary -t dev --output json`
returns one object keyed by bundle resource name, with no name matching and no
array to disambiguate. That's what
[scripts/last-error.ps1](scripts/last-error.ps1) uses.

**Lesson.** An error naming a flag you passed *correctly* usually means an
earlier argument consumed it.

---

### 5.4 `bundle run` hides the error you need

A failed task prints ~60 lines of Scala stack trace ending in:

```
Error: failed to reach TERMINATED or SKIPPED, got INTERNAL_ERROR: Task
apply_migrations failed with message: Workload failed, see run output for details.
```

The actual Python exception is nowhere in it.

**Fix.** Fetch the task output directly:

```powershell
.\scripts\last-error.ps1 -Target dev -JobKey medallion_job
```

which resolves the job via `bundle summary`, finds the latest run's failed task,
and prints `error`, the notebook output, and an ANSI-stripped trace tail. This
turned multi-minute guessing into a single command, three times over.

**Lesson.** When a tool buries the error, write the 30-line script that digs it
out — early. It pays for itself on the second failure.

---

## Patterns worth keeping

Themes that recur across the failures above.

**Separate "does it exist" from "can I see it".** §1.1 looked like a broken
install and was a stale environment. Check the two independently.

**Compare intent against reality.** §3.1's deploy said `Created schemas.env`
and the truth was a different name. `databricks schemas list` settled it in
seconds. Trust the workspace, not the deploy log.

**Reference resources, not names.** §3.1's real fix wasn't removing dev mode —
it was `${resources.schemas.env.name}` instead of `${var.env_schema}`, which
kills the whole class of drift.

**A documented constraint is not an enforced one.** §4.1's splitter documented
"no semicolons in string literals" and was broken by the next migration written.

**Verify before overriding a safety flag.** §3.2 — check the volumes are empty,
*then* `--auto-approve`.

**Reproduce locally when you can.** §4.1 was a pure string-processing bug.
Twenty seconds in local Python beat a five-minute cluster round trip.

**Build the diagnostic tool early.** §5.4 — the third time you dig an error out
by hand, you've already spent more than writing the script would have cost.
