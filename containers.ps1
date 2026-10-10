param(
    [ValidateSet('init', 'up', 'down', 'ps', 'logs', 'report', 'test', 'demo', 'benchmark')][string]$Action = 'up',
    [ValidateRange(100, 5000)][int]$Requests = 500,
    [ValidateRange(1, 128)][int]$GoCPUs = 2,
    [int[]]$Concurrency = @(1, 16)
)
$ErrorActionPreference = 'Stop'
foreach ($level in $Concurrency) {
    if ($level -lt 1 -or $level -gt 128) { throw 'Concurrency must be between 1 and 128.' }
}
if ($Concurrency.Count -eq 0) { throw 'Specify at least one concurrency level.' }
if ($Action -eq 'demo') {
    & (Join-Path $PSScriptRoot 'demo.ps1')
    return
}

if ($Action -eq 'init') {
    # Never overwrite rotated credentials or a password belonging to an existing DB.
    foreach ($name in @('.env.compose', '.env.container', '.container/tenants.json')) {
        if (Test-Path -LiteralPath (Join-Path $PSScriptRoot $name)) {
            throw "Container configuration already exists ($name). Edit the existing files; init never overwrites them."
        }
    }
    $source = Join-Path $PSScriptRoot '.env'
    if (-not (Test-Path -LiteralPath $source)) { $source = Join-Path $PSScriptRoot '.env.example' }
    $settings = @{}
    foreach ($line in Get-Content -LiteralPath $source) {
        $line = $line.Trim()
        if ($line -eq '' -or $line.StartsWith('#')) { continue }
        $parts = $line.Split('=', 2)
        if ($parts.Length -ne 2) { throw 'Configuration must use NAME=value lines.' }
        $settings[$parts[0].Trim()] = $parts[1].Trim()
    }
    $tenantJSON = $null
    if ($settings['TENANTS_CONFIG']) {
        $tenantSource = $settings['TENANTS_CONFIG']
        if (-not [IO.Path]::IsPathRooted($tenantSource)) { $tenantSource = Join-Path $PSScriptRoot $tenantSource }
        $tenantJSON = Get-Content -LiteralPath $tenantSource -Raw
        $null = $tenantJSON | ConvertFrom-Json
    } else {
        $rpm = if ($settings['RPM_LIMIT']) { [int]$settings['RPM_LIMIT'] } else { 60 }
        $tpm = if ($settings['TPM_LIMIT']) { [int]$settings['TPM_LIMIT'] } else { 60000 }
        $outputLimit = if ($settings['MAX_OUTPUT_TOKENS']) { [int]$settings['MAX_OUTPUT_TOKENS'] } else { 1024 }
        $tenantJSON = @{ tenants = @(@{ id='default'; api_key_env='JANUS_API_KEY'; rpm=$rpm; tpm=$tpm; max_output_tokens=$outputLimit; enabled=$true }) } | ConvertTo-Json -Depth 5
    }
    $random = [Security.Cryptography.RandomNumberGenerator]::Create()
    try {
        $bytes = New-Object byte[] 32
        $random.GetBytes($bytes)
        $password = [BitConverter]::ToString($bytes).Replace('-', '').ToLowerInvariant()
    } finally { $random.Dispose() }
    $null = New-Item -ItemType Directory -Force -Path (Join-Path $PSScriptRoot '.container')
    [IO.File]::WriteAllText((Join-Path $PSScriptRoot '.container/tenants.json'), $tenantJSON, (New-Object Text.UTF8Encoding $false))
    Copy-Item -LiteralPath $source -Destination (Join-Path $PSScriptRoot '.env.container')
    [IO.File]::WriteAllText((Join-Path $PSScriptRoot '.env.compose'), "JANUS_HTTP_PORT=8081`nPOSTGRES_PASSWORD=$password`n", (New-Object Text.UTF8Encoding $false))
    Write-Host 'Created ignored container configuration from local settings. HTTP port: 8081. Credentials were not printed.'
    return
}

foreach ($name in @('.env.compose', '.env.container', '.container/tenants.json')) {
    if (-not (Test-Path -LiteralPath (Join-Path $PSScriptRoot $name))) { throw 'Run .\containers.ps1 -Action init first.' }
}
if ($Action -in @('up', 'test', 'demo', 'benchmark')) {
    $allowlist = Get-Content -LiteralPath (Join-Path $PSScriptRoot '.dockerignore')
    foreach ($sourceDirectory in @('internal/gateway','cmd/janus','cmd/loadtest')) {
        if ("!$sourceDirectory/*.go" -cnotin $allowlist) {
            throw "Add !$sourceDirectory/*.go to .dockerignore's source allowlist before building."
        }
    }
}
$docker = Get-Command docker -ErrorAction SilentlyContinue
if ($docker) {
    $prefix = @()
    $executable = $docker.Source
    $directory = $PSScriptRoot
} else {
    $executable = (Get-Command wsl.exe -ErrorAction Stop).Source
    $windowsDirectory = $PSScriptRoot.Replace('\', '/')
    $directory = & $executable -d Ubuntu -- wslpath -a $windowsDirectory
    if ($LASTEXITCODE -ne 0) { throw 'Cannot access Ubuntu WSL. Check WSL startup.' }
    $directory = ([string]$directory).Trim()
    $prefix = @('-d', 'Ubuntu', '-u', 'root', '--', 'docker')
}
$compose = @('compose', '--project-directory', $directory, '--env-file', "$directory/.env.compose", '-f', "$directory/compose.yaml")
# Validate quietly: expanded compose config includes secrets, so never print it.
& $executable @prefix @compose config --quiet
if ($LASTEXITCODE -ne 0) { throw 'Docker Compose configuration is invalid or Docker is unavailable.' }

switch ($Action) {
    'report' {
        Get-Content -LiteralPath (Join-Path $PSScriptRoot 'queries/daily_usage.sql') -Raw |
            & $executable @prefix @compose exec -T postgres psql -U janus -d janus -v ON_ERROR_STOP=1
        if ($LASTEXITCODE -ne 0) { throw 'Container usage report failed.' }
        return
    }
    'up' { $arguments = @('up', '-d', '--build', '--wait', '--wait-timeout', '180') }
    'down' { $arguments = @('down', '--timeout', '15') }
    'ps' { $arguments = @('ps') }
    'logs' { $arguments = @('logs', '--tail', '100', 'janus') }
    'test' { $arguments = @('run', '--rm', '--build', 'tools', 'go', 'test', '-p', '1', '-count=1', '-timeout', '2m', './...') }
    'demo' { $arguments = @('run', '--rm', '--build', 'tools', 'sh', 'scripts/demo.sh') }
    'benchmark' {
        $null = New-Item -ItemType Directory -Force -Path (Join-Path $PSScriptRoot '.cache/perf-linux')
        $arguments = @('run', '--rm', '--build', '-e', 'JANUS_PERF=1', '-e', 'JANUS_PERF_MODE=full', '-e', "JANUS_PERF_REQUESTS=$Requests", '-e', "JANUS_PERF_CONCURRENCY=$($Concurrency -join ',')", '-e', "GOMAXPROCS=$GoCPUs", 'tools', 'go', 'test', '-p', '1', '-count=1', '-run', '^TestGatewayPerformance$', '-timeout', '20m', '-v', './...')
    }
}
& $executable @prefix @compose @arguments
if ($LASTEXITCODE -ne 0) { throw "Container action '$Action' failed. Check the diagnostics above." }
