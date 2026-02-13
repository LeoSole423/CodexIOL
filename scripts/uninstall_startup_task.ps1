$ErrorActionPreference = 'Stop'

$taskName = 'CodexIOL - Docker Startup'

Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue

Write-Host "Removed Scheduled Task (if it existed): $taskName"

