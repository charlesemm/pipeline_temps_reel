<#
.SYNOPSIS
    Stops the pipeline at the end of a work session.

.DESCRIPTION
    Uses `stop`, never `down`: containers are stopped but not removed, so
    Kafka topics and the analytics database survive. `down` would require a
    full `up -d` (and a schema reload) on the next session.

    See docs/guides/demarrage_arret.md.
#>

$ErrorActionPreference = "Stop"

$PipelineDir  = "C:\Users\charles.nguessan\OneDrive - IPSCNAM\Documents\pipeline_temps_reel"
$SimulatorDir = "C:\Users\charles.nguessan\OneDrive - IPSCNAM\Documents\simulateur_V5"

Write-Host ""
Write-Host "==> Arret du pipeline" -ForegroundColor Cyan
Push-Location $PipelineDir
podman compose stop
Pop-Location

Write-Host ""
Write-Host "==> Arret de simulateur_V5" -ForegroundColor Cyan
Push-Location $SimulatorDir
podman compose stop
Pop-Location

Write-Host ""
Write-Host "==> Arret de la VM Podman" -ForegroundColor Cyan
podman machine stop

Write-Host ""
Write-Host "Stack arretee. Les donnees sont conservees." -ForegroundColor Green
Write-Host "Redemarrage : .\scripts\start-stack.ps1"
Write-Host ""
