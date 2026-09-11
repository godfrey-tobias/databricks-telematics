# Print the real error from the most recent run of a bundle job.
#
#   .\scripts\last-error.ps1 -Target dev -JobKey setup_job
#
# Why this exists: `databricks bundle run` surfaces a JVM stack trace that
# buries the actual Python error under 60 lines of Scala. This pulls the task
# output that the trace is hiding.
param(
    [string]$Target = 'dev',
    [ValidateSet('setup_job', 'medallion_job')]
    [string]$JobKey = 'medallion_job',
    [string]$TaskKey,
    [int]$LogLines = 40
)

$ErrorActionPreference = 'Stop'
$env:Path = [Environment]::GetEnvironmentVariable('Path', 'Machine') + ';' +
            [Environment]::GetEnvironmentVariable('Path', 'User')

# `bundle summary` is used rather than `jobs list` on purpose: it returns one
# object per bundle resource keyed by resource name, so there is no name
# matching and no array to disambiguate.
$summary = databricks bundle summary -t $Target --output json | Out-String | ConvertFrom-Json
$jobId = [int64]$summary.resources.jobs.$JobKey.id
if (-not $jobId) { throw "no deployed job '$JobKey' in target '$Target'" }

"job $JobKey ($jobId) in $Target"

# Hoist ids into plain variables before passing them: `--job-id $obj.prop` does
# not bind reliably as a native-command argument in PowerShell 5.1.
$run = databricks jobs list-runs --job-id $jobId --limit 1 --expand-tasks --output json |
       Out-String | ConvertFrom-Json | Select-Object -First 1

"run $($run.run_id) -> $($run.state.result_state)   $($run.run_page_url)"
$run.tasks | Sort-Object { $_.start_time } | ForEach-Object {
    "  {0,-20} {1}" -f $_.task_key, $_.state.result_state
}

$failed = $run.tasks |
          Where-Object { $_.state.result_state -eq 'FAILED' -and (-not $TaskKey -or $_.task_key -eq $TaskKey) } |
          Sort-Object { $_.start_time } | Select-Object -Last 1
if (-not $failed) { ''; 'no failed task in this run'; return }

$taskRunId = [int64]$failed.run_id
$out = databricks jobs get-run-output $taskRunId --output json | Out-String | ConvertFrom-Json

''
"=== $($failed.task_key): error ==="
$out.error

if ($out.notebook_output.result) {
    ''
    '=== notebook output (tail) ==='
    ($out.notebook_output.result -split "`n" | Select-Object -Last $LogLines) -join "`n"
}

''
'=== trace (tail, ANSI stripped) ==='
$clean = $out.error_trace -replace "$([char]27)\[[0-9;]*m", ''
($clean -split "`n" | Select-Object -Last $LogLines) -join "`n"
