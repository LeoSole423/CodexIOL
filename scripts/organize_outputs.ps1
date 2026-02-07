param(
  [string]$Date = "",
  [switch]$OrganizeEvidence = $true
)

$ErrorActionPreference = "Stop"

function Ensure-Dir([string]$Path) {
  New-Item -ItemType Directory -Force -Path $Path | Out-Null
}

function Safe-Move([string]$From, [string]$To) {
  if (Test-Path $From) {
    Ensure-Dir (Split-Path -Parent $To)
    Move-Item -Force $From $To
  }
}

function Safe-Copy([string]$From, [string]$To) {
  if (Test-Path $From) {
    Ensure-Dir (Split-Path -Parent $To)
    Copy-Item -Force $From $To
  }
}

if (-not $Date) {
  $Date = (Get-Date).ToString("yyyy-MM-dd")
}

# Reports structure
Ensure-Dir "reports/analisis"
Ensure-Dir "reports/macro"
Ensure-Dir "reports/rebalance"
Ensure-Dir "reports/rebalance/archive"
Ensure-Dir "reports/dca"
Ensure-Dir "reports/latest"

# Known report filenames (repo conventions)
Safe-Move "AnalisisPortafolio_$Date.md" "reports/analisis/AnalisisPortafolio_$Date.md"
Safe-Move "Macro_$Date.md" "reports/macro/Macro_$Date.md"
Safe-Move "ResumenRebalanceo_$Date.md" "reports/rebalance/ResumenRebalanceo_$Date.md"
Safe-Move "ResumenRebalanceo_${Date}_prev.md" "reports/rebalance/archive/ResumenRebalanceo_${Date}_prev.md"
Safe-Move "PlanDCA_2026.md" "reports/dca/PlanDCA_2026.md"
Safe-Move "PlanDCA_$Date.md" "reports/dca/PlanDCA_$Date.md"
Safe-Move "Seguimiento.md" "reports/latest/Seguimiento.md"

# Refresh latest pointers (copies)
Safe-Copy "reports/analisis/AnalisisPortafolio_$Date.md" "reports/latest/AnalisisPortafolio.md"
Safe-Copy "reports/macro/Macro_$Date.md" "reports/latest/Macro.md"
Safe-Copy "reports/rebalance/ResumenRebalanceo_$Date.md" "reports/latest/ResumenRebalanceo.md"
if (Test-Path "reports/dca/PlanDCA_$Date.md") {
  Safe-Copy "reports/dca/PlanDCA_$Date.md" "reports/latest/PlanDCA.md"
} elseif (Test-Path "reports/dca/PlanDCA_2026.md") {
  Safe-Copy "reports/dca/PlanDCA_2026.md" "reports/latest/PlanDCA.md"
}

if ($OrganizeEvidence) {
  Ensure-Dir "data/evidence/$Date"
  Ensure-Dir "data/evidence/latest"

  # Evidence files produced by the workflow
  Safe-Move "data/evidence/portfolio_api_$Date.json" "data/evidence/$Date/portfolio_api.json"
  Safe-Move "data/evidence/cedears_fixed_income_candidates.json" "data/evidence/$Date/cedears_fixed_income_candidates.json"

  Safe-Copy "data/evidence/$Date/portfolio_api.json" "data/evidence/latest/portfolio_api.json"
  Safe-Copy "data/evidence/$Date/cedears_fixed_income_candidates.json" "data/evidence/latest/cedears_fixed_income_candidates.json"
}

Write-Host "OK: reports organized for $Date"

