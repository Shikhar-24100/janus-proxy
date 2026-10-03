$ErrorActionPreference = 'Stop'

# Keep Redis in the foreground so WSL and its localhost forwarding stay active.
# Use a dedicated development port, leaving any existing Redis on 6379 alone.
Write-Host 'Starting Janus development Redis at localhost:6380. Keep this terminal open.'
wsl.exe -d Ubuntu -- bash -lc 'exec redis-server --bind 127.0.0.1 --port 6380 --save "" --appendonly no'
if ($LASTEXITCODE -ne 0) { throw 'Redis exited with an error. Check the output above.' }
