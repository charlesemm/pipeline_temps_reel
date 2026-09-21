<#
.SYNOPSIS
    Applique le controle d'acces par role (sql/analytics/009_roles_acces.sql)
    sur la base analytique en cours d'execution.

.DESCRIPTION
    Lit FLINK_WRITER_PASSWORD et SGD_QUALITE_PASSWORD dans .env et les passe
    a psql comme variables (jamais ecrites dans un fichier versionne).
    Idempotent. Voir docs/guides/etape7d_roles.md.
#>
$ErrorActionPreference = "Continue"

$PipelineDir = "C:\Users\charles.nguessan\OneDrive - IPSCNAM\Documents\pipeline_temps_reel"
$Container   = "pipeline_temps_reel-postgres-analytics-1"
$SqlFile     = Join-Path $PipelineDir "sql\analytics\009_roles_acces.sql"

function Get-EnvValue($name) {
    $match = Select-String -Path (Join-Path $PipelineDir ".env") -Pattern "^$name=(.*)$" | Select-Object -First 1
    if (-not $match) { Write-Host "$name absent de .env" -ForegroundColor Red; exit 1 }
    return $match.Matches[0].Groups[1].Value.Trim()
}

$flinkPw = Get-EnvValue "FLINK_WRITER_PASSWORD"
$sgdPw   = Get-EnvValue "SGD_QUALITE_PASSWORD"

podman cp $SqlFile "${Container}:/tmp/009_roles_acces.sql"
podman exec $Container psql -U dprest -d dprest_analytics -v ON_ERROR_STOP=1 `
    -v "flink_pw=$flinkPw" -v "sgd_qualite_pw=$sgdPw" -f /tmp/009_roles_acces.sql
$code = $LASTEXITCODE
podman exec $Container rm -f /tmp/009_roles_acces.sql

if ($code -ne 0) { Write-Host "Application des roles : ECHEC (code $code)" -ForegroundColor Red; exit 1 }
Write-Host "Application des roles : OK" -ForegroundColor Green
