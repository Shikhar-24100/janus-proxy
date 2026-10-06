$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'check-wsl.ps1')
Assert-JanusWsl

wsl.exe -d Ubuntu -- bash -lc 'redis-cli -p 6380 ping >/dev/null 2>&1'
if ($LASTEXITCODE -eq 0) {
    Write-Host 'Quota/cache Redis is already responding on localhost:6380. Keep its original terminal open.'
    return
}

# Keep Redis in the foreground so WSL and its localhost forwarding stay active.
# Use a dedicated development port, leaving any existing Redis on 6379 alone.
Write-Host 'Starting Janus development Redis at localhost:6380. Keep this terminal open.'
wsl.exe -d Ubuntu -- bash -lc 'exec redis-server --bind 127.0.0.1 --port 6380 --save "" --appendonly no'
if ($LASTEXITCODE -ne 0) { throw 'Redis exited with an error. Check the output above.' }
