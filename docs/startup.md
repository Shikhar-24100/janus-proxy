# Local startup and WSL failures

In three separate PowerShell terminals, from the project directory:

```powershell
.\start-redis.ps1
.\start-usage.ps1
.\run.ps1
```

Keep the Redis terminals open. Quota/cache Redis uses port 6380; the persistent
usage Redis uses 6381. PostgreSQL uses 5432 and Janus uses 8080.
The scripts first check whether Ubuntu can start, with a 60-second limit. They
reuse responding Redis instances instead of starting duplicates. Usage startup
also verifies PostgreSQL readiness and Redis append-only persistence.
The initial Go build may take time before Janus prints its listening message.

## Distinguish the failure

| Symptom | Meaning / next check |
| --- | --- |
| `Wsl/Service/E_UNEXPECTED` before Linux starts | Windows WSL startup failure; not a Go handler or Redis configuration error |
| Connection refused on 6380/6381 | Redis isn't listening; start its project script |
| PostgreSQL not accepting connections | Check `wsl.exe -d Ubuntu -- pg_lsclusters` |
| Janus cannot initialize usage storage | Check both configured usage services and local DATABASE_URL credentials |
| Address already in use on 8080 | Another Janus process may still be running; stop its original terminal before restarting |

For the WSL error, first try:

```powershell
wsl.exe --list --verbose
wsl.exe -d Ubuntu -- true
```

On October 6, Ubuntu started successfully on a later retry. PostgreSQL data and
the persistent usage queue loaded successfully; the two project Redis processes
needed starting. The earlier E_UNEXPECTED failure's root cause was not established.
Do not interpret a successful retry as a permanent Windows-level repair.

If startup still fails, inspect the error before making changes. `wsl.exe
--shutdown` stops all WSL distributions, so use it only after ensuring no other
WSL work needs to keep running. Restarting Windows' WSL service requires an
Administrator PowerShell and also affects WSL sessions:

```powershell
Restart-Service WslService
```

This project's scripts do not automatically restart system services, reboot
Windows, unregister Ubuntu, or delete any data directories. Unregistering a
distribution removes its data and is not a routine startup fix.
See [Microsoft's WSL troubleshooting guide](https://learn.microsoft.com/en-us/windows/wsl/troubleshooting)
for further diagnosis.

Once Janus starts, check:

```powershell
Invoke-RestMethod http://127.0.0.1:8080/health
.\usage-report.ps1
.\benchmark.ps1 -Mode full -Requests 500 -Concurrency 1,16
```

The benchmark uses a local fake provider and its own temporary quota/cache/usage
state. It does not spend provider credits or consume normal usage events.
