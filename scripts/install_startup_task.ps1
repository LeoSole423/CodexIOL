$ErrorActionPreference = 'Stop'

# Installs a per-user Scheduled Task that runs scripts/startup.ps1 at logon.

$taskName = 'CodexIOL - Docker Startup'
$startupScript = Join-Path $PSScriptRoot 'startup.ps1'

if (-not (Test-Path -LiteralPath $startupScript)) {
  throw "Missing script: $startupScript"
}

$ps = (Get-Command powershell.exe).Source

$action = New-ScheduledTaskAction `
  -Execute $ps `
  -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$startupScript`""

$trigger = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME

$settings = New-ScheduledTaskSettingsSet `
  -StartWhenAvailable `
  -AllowStartIfOnBatteries `
  -DontStopIfGoingOnBatteries `
  -ExecutionTimeLimit (New-TimeSpan -Minutes 10)

Register-ScheduledTask `
  -TaskName $taskName `
  -Action $action `
  -Trigger $trigger `
  -Settings $settings `
  -Description 'Starts CodexIOL docker compose services after logon.' `
  -Force | Out-Null

Write-Host "Installed Scheduled Task: $taskName"

