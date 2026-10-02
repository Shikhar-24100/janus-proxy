$ErrorActionPreference = 'Stop'

# Read local configuration as data, never as executable PowerShell code.
$configPath = Join-Path $PSScriptRoot '.env'
if (Test-Path -LiteralPath $configPath) {
    foreach ($line in Get-Content -LiteralPath $configPath) {
        $line = $line.Trim()
        if ($line -eq '' -or $line.StartsWith('#')) { continue }
        $parts = $line.Split('=', 2)
        if ($parts.Length -ne 2 -or $parts[0].Trim() -notin @('OPENAI_BASE_URL', 'OPENAI_API_KEY')) {
            throw 'The .env file only supports OPENAI_BASE_URL and OPENAI_API_KEY in NAME=value format.'
        }
        [Environment]::SetEnvironmentVariable($parts[0].Trim(), $parts[1].Trim(), 'Process')
    }
}

$goPath = Join-Path $PSScriptRoot '.tools\go\bin\go.exe'
if (-not (Test-Path -LiteralPath $goPath)) {
    $goCommand = Get-Command go -ErrorAction SilentlyContinue
    if (-not $goCommand) { throw 'Go was not found. Install Go 1.22 or newer.' }
    $goPath = $goCommand.Source
}

$env:GOCACHE = Join-Path $PSScriptRoot '.cache\go-build'
Push-Location $PSScriptRoot
try {
    & $goPath run .
    if ($LASTEXITCODE -ne 0) { throw 'Janus exited with an error. Check the server output above.' }
} finally {
    Pop-Location
}
