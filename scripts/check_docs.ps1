$ErrorActionPreference = "Stop"

function Test-Utf8Bom {
  param(
    [byte[]]$Bytes
  )
  if ($Bytes.Length -lt 3) { return $false }
  return ($Bytes[0] -eq 0xEF -and $Bytes[1] -eq 0xBB -and $Bytes[2] -eq 0xBF)
}

function Get-TrackedMarkdownFiles {
  $files = @()
  $tracked = git ls-files -- "*.md"
  foreach ($line in $tracked) {
    if (-not [string]::IsNullOrWhiteSpace($line)) {
      $files += $line.Trim()
    }
  }
  if ((Test-Path "AGENT.md") -and (-not ($files -contains "AGENT.md"))) {
    $files += "AGENT.md"
  }
  return $files
}

function Test-RelativeLinks {
  param(
    [string]$FilePath,
    [string]$Text,
    [System.Collections.Generic.List[string]]$Errors
  )
  $dir = Split-Path -Parent $FilePath
  $pattern = "\[[^\]]+\]\(([^)]+)\)"
  $matches = [System.Text.RegularExpressions.Regex]::Matches($Text, $pattern)
  foreach ($m in $matches) {
    $target = $m.Groups[1].Value.Trim()
    if ([string]::IsNullOrWhiteSpace($target)) { continue }
    if ($target.StartsWith("<") -and $target.EndsWith(">")) {
      $target = $target.Substring(1, $target.Length - 2)
    }

    $target = $target.Split(" ")[0].Trim()
    if ($target -match "^(https?:|mailto:|#)") { continue }
    if ($target -match "^[a-zA-Z][a-zA-Z0-9+.-]*:") { continue }

    $pathPart = $target
    if ($pathPart.Contains("#")) {
      $pathPart = $pathPart.Split("#")[0]
    }
    if ($pathPart.Contains("?")) {
      $pathPart = $pathPart.Split("?")[0]
    }
    if ([string]::IsNullOrWhiteSpace($pathPart)) { continue }

    $resolved = Join-Path $dir $pathPart
    if (-not (Test-Path $resolved)) {
      $rel = [System.IO.Path]::GetRelativePath((Get-Location).Path, $FilePath)
      $Errors.Add("${rel}: broken relative link '$target'")
    }
  }
}

function Test-AgentRequiredSections {
  param(
    [string]$AgentPath,
    [System.Collections.Generic.List[string]]$Errors
  )
  if (-not (Test-Path $AgentPath)) {
    $Errors.Add("AGENT.md: file not found")
    return
  }
  $text = [System.IO.File]::ReadAllText($AgentPath, [System.Text.UTF8Encoding]::new($false, $true))
  $required = @(
    "## 3) Pre-flight 90s (obligatorio)",
    "## 4) Go/No-Go gates",
    "## 5) Matriz warning -> accion",
    "## 6) Definition of Done por flujo",
    "## 7) Interfaz operativa estable (bloques de salida)",
    "## 8) Post-run verification (obligatorio)",
    "## 9) Quick triage (fallas recurrentes)"
  )
  foreach ($section in $required) {
    if ($text -notmatch [System.Text.RegularExpressions.Regex]::Escape($section)) {
      $Errors.Add("AGENT.md: missing section '$section'")
    }
  }
}

$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
  $errors = [System.Collections.Generic.List[string]]::new()
  $files = Get-TrackedMarkdownFiles

  foreach ($file in $files) {
    $fullPath = Join-Path $repoRoot $file
    if (-not (Test-Path $fullPath)) {
      $errors.Add("${file}: tracked markdown file not found")
      continue
    }

    $bytes = [System.IO.File]::ReadAllBytes($fullPath)
    if (Test-Utf8Bom -Bytes $bytes) {
      $errors.Add("${file}: UTF-8 BOM detected")
    }

    try {
      $text = [System.IO.File]::ReadAllText($fullPath, [System.Text.UTF8Encoding]::new($false, $true))
    } catch {
      $errors.Add("${file}: invalid UTF-8 encoding")
      continue
    }

    $lineNo = 0
    foreach ($line in ($text -split "`r?`n")) {
      $lineNo++
      if ($line -match "\s+$") {
        $errors.Add("${file}:${lineNo}: trailing whitespace")
      }
      if ($line -match "Ã|�") {
        $errors.Add("${file}:${lineNo}: suspicious mojibake character")
      }
    }

    Test-RelativeLinks -FilePath $fullPath -Text $text -Errors $errors
  }

  Test-AgentRequiredSections -AgentPath (Join-Path $repoRoot "AGENT.md") -Errors $errors

  if ($errors.Count -gt 0) {
    Write-Host "Doc checks failed ($($errors.Count) issues):" -ForegroundColor Red
    foreach ($e in $errors) {
      Write-Host " - $e" -ForegroundColor Red
    }
    exit 1
  }

  Write-Host "Doc checks passed for $($files.Count) markdown files." -ForegroundColor Green
  exit 0
}
finally {
  Pop-Location
}
