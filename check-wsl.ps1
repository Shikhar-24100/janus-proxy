function Assert-JanusWsl {
    $wslCommand = Get-Command wsl.exe -ErrorAction SilentlyContinue
    if (-not $wslCommand) { throw 'WSL is missing. Install Ubuntu WSL before starting the local Redis/PostgreSQL services.' }

    Write-Host 'Checking Ubuntu WSL startup (up to 60 seconds)...'
    $probe = Start-Process -FilePath $wslCommand.Source -ArgumentList @('-d','Ubuntu','--','true') -WindowStyle Hidden -PassThru
    try {
        if (-not $probe.WaitForExit(60000)) {
            $probe.Kill()
            throw 'Ubuntu WSL startup timed out. Run wsl.exe -d Ubuntu -- true in a terminal to inspect it. No distribution or database data was removed.'
        }
        if ($probe.ExitCode -ne 0) {
            throw 'Ubuntu WSL could not start. Run wsl.exe -d Ubuntu -- true to see the Windows error. For E_UNEXPECTED, see docs/startup.md before changing Redis or PostgreSQL.'
        }
    } finally {
        $probe.Dispose()
    }
}
