# Run a SQL statement against a target's schema and print the rows.
#
#   .\scripts\query.ps1 -Target dev -Sql "SELECT count(*) FROM silver_pings"
#
# Uses the SQL Statement Execution API on the workspace's serverless warehouse,
# so verification queries can run from the CLI without opening the UI.
param(
    [string]$Target = 'dev',
    [Parameter(Mandatory = $true)][string]$Sql,
    [string]$Catalog = 'telematics',
    [string]$WarehouseId
)

$ErrorActionPreference = 'Stop'
$env:Path = [Environment]::GetEnvironmentVariable('Path', 'Machine') + ';' +
            [Environment]::GetEnvironmentVariable('Path', 'User')

if (-not $WarehouseId) {
    $wh = databricks warehouses list --output json | Out-String | ConvertFrom-Json |
          Select-Object -First 1
    $WarehouseId = $wh.id
}

$payload = @{
    warehouse_id = $WarehouseId
    catalog      = $Catalog
    schema       = $Target
    statement    = $Sql
    wait_timeout = '50s'
} | ConvertTo-Json -Compress

# UTF8Encoding($false) => no BOM. The CLI's JSON parser rejects a BOM with
# "invalid character 'ï'", which is a confusing way to learn this.
$tmp = Join-Path $env:TEMP "dbq_$([guid]::NewGuid().ToString('N')).json"
[System.IO.File]::WriteAllText($tmp, $payload, (New-Object System.Text.UTF8Encoding $false))

try {
    $res = databricks api post /api/2.0/sql/statements --json "@$tmp" | Out-String | ConvertFrom-Json
} finally {
    Remove-Item $tmp -ErrorAction SilentlyContinue
}

if ($res.status.state -ne 'SUCCEEDED') {
    "state: $($res.status.state)"
    $res.status.error.message
    return
}

$cols = $res.manifest.schema.columns | Sort-Object position | Select-Object -ExpandProperty name
$cols -join ' | '
'-' * 60
foreach ($row in $res.result.data_array) { ($row -join ' | ') }
"($($res.manifest.total_row_count) row(s))"
