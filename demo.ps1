# Disposable demo services, fake providers, and no local credential files.
$ErrorActionPreference = 'Stop'
$docker = Get-Command docker -ErrorAction SilentlyContinue
if ($docker) {
    $executable = $docker.Source
    $prefix = @()
    $directory = $PSScriptRoot
} else {
    $executable = (Get-Command wsl.exe -ErrorAction Stop).Source
    $directory = (& $executable -d Ubuntu -u root -- wslpath -a $PSScriptRoot.Replace('\','/')).Trim()
    if ($LASTEXITCODE -ne 0) { throw 'Cannot access Ubuntu WSL.' }
    $prefix = @('-d','Ubuntu','-u','root','--','docker')
}
$compose = @('compose','--project-directory',$directory,'-f',"$directory/compose.demo.yaml")
try {
    & $executable @prefix @compose run --rm --build demo
    if ($LASTEXITCODE -ne 0) { throw 'Demo failed; inspect the scenario output above.' }
} finally {
    # Only this disposable demo stack is stopped; normal Janus volumes are separate.
    & $executable @prefix @compose down --volumes --timeout 15
    if ($LASTEXITCODE -ne 0) { Write-Warning 'Demo cleanup failed. Stop the janus-demo stack manually.' }
}
