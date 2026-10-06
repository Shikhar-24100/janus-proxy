$ErrorActionPreference = 'Stop'

# Read local configuration as data, never as executable PowerShell code.
$configPath = Join-Path $PSScriptRoot '.env'
if (Test-Path -LiteralPath $configPath) {
    foreach ($line in Get-Content -LiteralPath $configPath) {
        $line = $line.Trim()
        if ($line -eq '' -or $line.StartsWith('#')) { continue }
        $parts = $line.Split('=', 2)
        $settingName = $parts[0].Trim()
        if ($parts.Length -ne 2 -or ($settingName -notin @('OPENAI_BASE_URL', 'OPENAI_API_KEY', 'JANUS_API_KEY', 'REDIS_URL', 'RPM_LIMIT', 'TPM_LIMIT', 'MAX_OUTPUT_TOKENS', 'FALLBACK_API_KEY', 'FALLBACK_BASE_URL', 'FALLBACK_MODEL', 'CACHE_TTL_SECONDS', 'TENANTS_CONFIG', 'DATABASE_URL', 'USAGE_REDIS_URL') -and $settingName -cnotmatch '^[A-Z][A-Z0-9_]*_JANUS_API_KEY$')) {
            throw 'The .env file contains an unsupported setting or invalid NAME=value format.'
        }
        [Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim(), 'Process')
    }
}

$goPath = Join-Path $PSScriptRoot '.tools\go\bin\go.exe'
if (-not (Test-Path -LiteralPath $goPath)) {
    $goCommand = Get-Command go -ErrorAction SilentlyContinue
    if (-not $goCommand) { throw 'Go was not found. Install Go 1.24 or newer.' }
    $goPath = $goCommand.Source
}

$env:GOCACHE = Join-Path $PSScriptRoot '.cache\go-build'
$env:GOMODCACHE = Join-Path $PSScriptRoot '.cache\go-mod'
Push-Location $PSScriptRoot
try {
    Write-Host 'Building and starting Janus. The first Go build can take a while; startup diagnostics follow below.'
    & $goPath run -p 1 .
    if ($LASTEXITCODE -ne 0) { throw 'Janus exited with an error. Check the server output above.' }
} finally {
    Pop-Location
}
