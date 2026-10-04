$ErrorActionPreference = 'Stop'

# PostgreSQL installation/setup is documented in docs/usage-storage.md.
wsl.exe -d Ubuntu -u root -- pg_ctlcluster 16 main start
if ($LASTEXITCODE -notin @(0, 2)) { throw 'Cannot start PostgreSQL 16/main.' }

# Dedicated persistent queue, separate from the ephemeral quota/cache Redis.
# Leave this terminal open. Data stays under the WSL user's home directory.
Write-Host 'PostgreSQL: localhost:5432. Starting persistent usage Redis: localhost:6381.'
wsl.exe -d Ubuntu -- bash -lc 'mkdir -p "$HOME/.local/share/janus-usage-redis" && exec redis-server --bind 127.0.0.1 --port 6381 --dir "$HOME/.local/share/janus-usage-redis" --save "" --appendonly yes --appendfsync always --maxmemory 64mb --maxmemory-policy noeviction'
if ($LASTEXITCODE -ne 0) { throw 'Usage Redis exited with an error.' }
