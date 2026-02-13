$ErrorActionPreference = 'Stop'

# Start/restart this repo's Compose services once Docker is ready.
# Intended for use from Windows Task Scheduler at logon/startup.

$repoRoot = Split-Path -Parent $PSScriptRoot

function Wait-DockerReady {
  param(
    [int]$TimeoutSeconds = 180
  )

  $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
  while ((Get-Date) -lt $deadline) {
    try {
      docker info *> $null
      return
    } catch {
      Start-Sleep -Seconds 2
    }
  }

  throw "Docker did not become ready within ${TimeoutSeconds}s."
}

Wait-DockerReady -TimeoutSeconds 180

Push-Location $repoRoot
try {
  docker compose up -d
} finally {
  Pop-Location
}

