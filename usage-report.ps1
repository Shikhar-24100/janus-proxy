$ErrorActionPreference = 'Stop'

# Local WSL development database: peer authentication, no password in arguments.
$queryPath = Join-Path $PSScriptRoot 'queries\daily_usage.sql'
Get-Content -Raw -LiteralPath $queryPath | wsl.exe -d Ubuntu -u postgres -- psql -X -v ON_ERROR_STOP=1 -d janus_usage
if ($LASTEXITCODE -ne 0) { throw 'Usage report failed; start PostgreSQL and Janus first.' }
