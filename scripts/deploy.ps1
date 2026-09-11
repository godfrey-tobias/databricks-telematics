# Validate, deploy and run one target end to end.
#   .\scripts\deploy.ps1 dev           # deploy + run the medallion job
#   .\scripts\deploy.ps1 dev -Setup    # also seed the dimension and generate data
param(
    [Parameter(Mandatory = $true)]
    [ValidateSet('dev', 'test', 'prod')]
    [string]$Target,

    [switch]$Setup
)

$ErrorActionPreference = 'Stop'

Write-Host "==> validate  ($Target)" -ForegroundColor Cyan
databricks bundle validate -t $Target
if ($LASTEXITCODE -ne 0) { throw "validate failed" }

Write-Host "==> deploy    ($Target)" -ForegroundColor Cyan
databricks bundle deploy -t $Target
if ($LASTEXITCODE -ne 0) { throw "deploy failed" }

if ($Setup) {
    Write-Host "==> setup_job ($Target)   migrations + dimension seed + ping generation" -ForegroundColor Cyan
    databricks bundle run setup_job -t $Target
    if ($LASTEXITCODE -ne 0) { throw "setup_job failed" }
}

Write-Host "==> medallion_job ($Target)   bronze -> silver -> gold" -ForegroundColor Cyan
databricks bundle run medallion_job -t $Target
if ($LASTEXITCODE -ne 0) { throw "medallion_job failed" }

Write-Host "==> done ($Target)" -ForegroundColor Green
