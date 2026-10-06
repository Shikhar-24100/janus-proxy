param(
    [ValidateSet('core', 'full')][string]$Mode = 'full',
    [ValidateRange(100, 5000)][int]$Requests = 500,
    [int[]]$Concurrency = @(1, 16),
    [switch]$Profile,
    [ValidateRange(1, 128)][int]$GoCPUs = 2
)
$ErrorActionPreference = 'Stop'
foreach ($level in $Concurrency) {
    if ($level -lt 1 -or $level -gt 128) { throw 'Concurrency must be between 1 and 128.' }
}
if ($Concurrency.Count -eq 0) { throw 'Specify at least one concurrency level.' }

# Read only benchmark service settings; never load a real provider key.
$settings = @{}
if (Test-Path -LiteralPath (Join-Path $PSScriptRoot '.env')) {
    foreach ($line in Get-Content -LiteralPath (Join-Path $PSScriptRoot '.env')) {
        $line = $line.Trim()
        if ($line -eq '' -or $line.StartsWith('#')) { continue }
        $parts = $line.Split('=', 2)
        if ($parts.Length -eq 2 -and $parts[0].Trim() -in @('REDIS_URL','USAGE_REDIS_URL','DATABASE_URL')) {
            $settings[$parts[0].Trim()] = $parts[1].Trim()
        }
    }
}
if ($Mode -eq 'full') {
    foreach ($name in @('REDIS_URL','USAGE_REDIS_URL','DATABASE_URL')) {
        if (-not $settings[$name]) { throw "Full mode requires $name in .env." }
    }
}
$goPath = Join-Path $PSScriptRoot '.tools\go\bin\go.exe'
if (-not (Test-Path -LiteralPath $goPath)) { $goPath = (Get-Command go).Source }
$variableNames = @('JANUS_PERF','JANUS_PERF_MODE','JANUS_PERF_REQUESTS','JANUS_PERF_CONCURRENCY','JANUS_PERF_REDIS_URL','JANUS_PERF_USAGE_REDIS_URL','JANUS_PERF_DATABASE_URL','JANUS_PERF_PROFILE','GOCACHE','GOMODCACHE','GOMAXPROCS')
$previous = @{}
foreach ($name in $variableNames) { $previous[$name] = [Environment]::GetEnvironmentVariable($name, 'Process') }
Push-Location $PSScriptRoot
try {
	$env:GOMAXPROCS = [string]$GoCPUs
    $env:JANUS_PERF = '1'
    $env:JANUS_PERF_MODE = $Mode
    $env:JANUS_PERF_REQUESTS = [string]$Requests
    $env:JANUS_PERF_CONCURRENCY = $Concurrency -join ','
    $env:JANUS_PERF_PROFILE = if ($Profile) { '1' } else { '0' }
    $env:JANUS_PERF_REDIS_URL = $settings['REDIS_URL']
    $env:JANUS_PERF_USAGE_REDIS_URL = $settings['USAGE_REDIS_URL']
    $env:JANUS_PERF_DATABASE_URL = $settings['DATABASE_URL']
    $env:GOCACHE = Join-Path $PSScriptRoot '.cache\go-build'
    $env:GOMODCACHE = Join-Path $PSScriptRoot '.cache\go-mod'
    $arguments = @('test','-p','1','-count=1','-run','^TestGatewayPerformance$','-timeout','20m','-v')
    if ($Profile) {
        $null = New-Item -ItemType Directory -Force -Path (Join-Path $PSScriptRoot '.cache\perf')
        $arguments += @('-cpuprofile', ".cache/perf/$Mode-cpu.pprof", '-memprofile', ".cache/perf/$Mode-heap.pprof", '-o', ".cache/perf/$Mode.test.exe")
    }
    Write-Host "Local fake provider benchmark: $Mode mode, $Requests requests per scenario, concurrency $($Concurrency -join ','), Go CPUs $GoCPUs."
    & $goPath @arguments .
    if ($LASTEXITCODE -ne 0) { throw 'Benchmark failed; inspect its errors above.' }
    $reportName = "latest-$Mode"
    if ($Profile) { $reportName += '-profiled' }
    Write-Host "Reports: .cache/perf/$reportName.md, .json and .csv"
} finally {
    Pop-Location
    foreach ($name in $variableNames) { [Environment]::SetEnvironmentVariable($name, $previous[$name], 'Process') }
}
