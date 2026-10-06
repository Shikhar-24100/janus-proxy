$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'check-wsl.ps1')
Assert-JanusWsl

# PostgreSQL installation/setup is documented in docs/usage-storage.md.
wsl.exe -d Ubuntu -u root -- pg_ctlcluster 16 main start
if ($LASTEXITCODE -notin @(0, 2)) { throw 'Cannot start PostgreSQL 16/main.' }
wsl.exe -d Ubuntu -- pg_isready -h 127.0.0.1 -p 5432
if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL started but is not accepting connections on localhost:5432.' }

wsl.exe -d Ubuntu -- bash -lc 'redis-cli -p 6381 ping >/dev/null 2>&1'
if ($LASTEXITCODE -eq 0) {
    $settings = @(wsl.exe -d Ubuntu -- redis-cli -p 6381 --raw CONFIG GET appendonly appendfsync maxmemory-policy)
    if ($LASTEXITCODE -ne 0 -or $settings.Count -ne 6) {
        throw 'Cannot verify usage Redis persistence/eviction settings on 6381.'
    }
    $redisConfig = @{}
    for ($index = 0; $index -lt $settings.Count; $index += 2) { $redisConfig[$settings[$index]] = $settings[$index + 1] }
    if ($redisConfig['appendonly'] -ne 'yes' -or $redisConfig['appendfsync'] -ne 'always' -or $redisConfig['maxmemory-policy'] -ne 'noeviction') {
        throw 'Redis on 6381 must use appendonly=yes, appendfsync=always and maxmemory-policy=noeviction. Existing settings were left unchanged.'
    }
    Write-Host 'PostgreSQL and persistent usage Redis are already responding. Keep the Redis terminal open.'
    return
}

# Dedicated persistent queue, separate from the ephemeral quota/cache Redis.
# Leave this terminal open. Data stays under the WSL user's home directory.
Write-Host 'PostgreSQL: localhost:5432. Starting persistent usage Redis: localhost:6381.'
wsl.exe -d Ubuntu -- bash -lc 'mkdir -p "$HOME/.local/share/janus-usage-redis" && exec redis-server --bind 127.0.0.1 --port 6381 --dir "$HOME/.local/share/janus-usage-redis" --save "" --appendonly yes --appendfsync always --maxmemory 64mb --maxmemory-policy noeviction'
if ($LASTEXITCODE -ne 0) { throw 'Usage Redis exited with an error.' }
