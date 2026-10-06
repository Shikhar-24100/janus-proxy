param(
    [ValidateRange(1, 300)][int]$DurationSeconds = 10,
    [ValidatePattern('^[0-9]+(,[0-9]+)*$')][string]$Rates = '50,200,500',
    [ValidateRange(1, 4096)][int]$MaxInflight = 256,
    [switch]$AllowOverload
)
$ErrorActionPreference = 'Stop'
foreach ($rate in $Rates.Split(',')) {
    if ([int]$rate -lt 1 -or [int]$rate -gt 10000) { throw 'Rates must be between 1 and 10000.' }
}
$workspacePath = $PSScriptRoot.Replace('\', '/')
$linuxPath = (& wsl.exe -d Ubuntu -u root -- wslpath -a $workspacePath).Trim()
if ($LASTEXITCODE -ne 0) { throw 'Could not resolve the workspace in Ubuntu WSL.' }
$overloadMode = if ($AllowOverload) { 'true' } else { 'false' }
& wsl.exe -d Ubuntu -u root -- sh "$linuxPath/scripts/loadtest.sh" "$linuxPath" $DurationSeconds $Rates $MaxInflight $overloadMode
if ($LASTEXITCODE -ne 0) { throw 'Load test failed. Inspect .cache/loadtest reports.' }
Write-Host 'Reports: .cache/loadtest/results.md, results.json, samples.csv, resources.jsonl'
