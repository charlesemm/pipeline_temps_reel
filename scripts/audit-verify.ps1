<#
.SYNOPSIS
    Verifie l'integrite des archives d'audit (chaine de hachage).

.DESCRIPTION
    Relit audit/manifest.log, recalcule la somme SHA-256 de chaque fichier et
    la chaine complete. Code de sortie 0 = archives intactes, 1 = fichier
    modifie, manquant ou chaine rompue. Voir docs/guides/etape7c_audit.md.
#>
$ErrorActionPreference = "Continue"

$PipelineDir  = "C:\Users\charles.nguessan\OneDrive - IPSCNAM\Documents\pipeline_temps_reel"
$AuditDir     = Join-Path $PipelineDir "audit"
$Manifest     = Join-Path $AuditDir "manifest.log"
$GenesisChain = ("0" * 64)

function Get-Sha256Text($text) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($text)
    return ([BitConverter]::ToString($sha.ComputeHash($bytes)) -replace "-", "").ToLower()
}

if (-not (Test-Path $Manifest)) { Write-Host "Aucun manifeste : rien a verifier."; exit 0 }

$previous = $GenesisChain
$failed   = $false
$count    = 0

foreach ($line in Get-Content $Manifest) {
    $day, $name, $fileHash, $recordedPrev, $recordedChain = $line -split "\s+"
    $count++
    $path = Join-Path $AuditDir $name

    if (-not (Test-Path $path)) { Write-Host "MANQUANT   $name"; $failed = $true; $previous = $recordedChain; continue }

    $actualHash = (Get-FileHash -Algorithm SHA256 $path).Hash.ToLower()
    if ($actualHash -ne $fileHash) { Write-Host "ALTERE     $name (somme SHA-256 differente)"; $failed = $true }

    if ($recordedPrev -ne $previous) { Write-Host "CHAINE ROMPUE avant $name (ligne supprimee ou reordonnee)"; $failed = $true }

    $expectedChain = Get-Sha256Text ($recordedPrev + $fileHash + $name)
    if ($expectedChain -ne $recordedChain) { Write-Host "CHAINE INVALIDE a $name (manifeste modifie)"; $failed = $true }

    $previous = $recordedChain
}

if ($failed) { Write-Host "Audit : NON CONFORME ($count entrees)"; exit 1 }
Write-Host "Audit : conforme ($count entrees, chaine intacte)"
