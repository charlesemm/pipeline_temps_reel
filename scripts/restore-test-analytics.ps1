<#
.SYNOPSIS
    Test de restauration de la derniere sauvegarde de postgres-analytics.

.DESCRIPTION
    Verifie la somme de controle, dechiffre, restaure dans une base
    TEMPORAIRE (dprest_restore_test, jamais la base de production), compare
    le comptage de chaque table au manifeste .counts.csv de la sauvegarde,
    puis supprime la base temporaire. Code de sortie 0 = restauration
    conforme, 1 = ecart ou echec.

    -TolerancePct : ecart relatif accepte par table (defaut 0.5 %). Les
    tables KPI sont reecrites en continu par Flink entre le dump et le
    comptage de reference ; une tolerance nulle produirait de faux positifs
    tant que le job tourne. Voir docs/guides/etape7b_sauvegarde.md.
#>
param(
    [double]$TolerancePct = 0.5
)

# "Continue" : PowerShell 5.1 traite tout message stderr d'un exe natif (NOTICE psql, info gpg)
# comme une erreur fatale avec "Stop". Les echecs sont detectes via $LASTEXITCODE.
$ErrorActionPreference = "Continue"

$PipelineDir = "C:\Users\charles.nguessan\OneDrive - IPSCNAM\Documents\pipeline_temps_reel"
$Container   = "pipeline_temps_reel-postgres-analytics-1"
$BackupDir   = Join-Path $PipelineDir "backups"
$SqlCounts   = Join-Path $PSScriptRoot "sql\comptages_tables.sql"
$TestDb      = "dprest_restore_test"
$stamp       = Get-Date -Format "yyyyMMdd_HHmmss"
$passFile    = Join-Path $env:TEMP "rt_$stamp.pass"
$plain       = Join-Path $env:TEMP "rt_$stamp.dump"

function Write-Log($message) {
    Write-Host ("{0}  {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $message)
}

function Get-EnvValue($name) {
    $match = Select-String -Path (Join-Path $PipelineDir ".env") -Pattern "^$name=(.*)$" | Select-Object -First 1
    if (-not $match) { throw "$name absent de .env" }
    return $match.Matches[0].Groups[1].Value.Trim()
}

function Find-Gpg {
    $cmd = Get-Command gpg -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    $git = "C:\Program Files\Git\usr\bin\gpg.exe"
    if (Test-Path $git) { return $git }
    throw "gpg introuvable"
}

$failed = $false
try {
    $latest = Get-ChildItem $BackupDir -Filter "analytics_*.dump.gpg" | Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if (-not $latest) { throw "aucune sauvegarde dans $BackupDir" }
    Write-Log "Sauvegarde testee : $($latest.Name)"

    $expected = (Get-Content "$($latest.FullName).sha256").Split(" ")[0]
    $actual   = (Get-FileHash -Algorithm SHA256 $latest.FullName).Hash.ToLower()
    if ($expected -ne $actual) { throw "somme de controle SHA-256 invalide (fichier altere ou corrompu)" }
    Write-Log "Somme de controle : OK"

    [IO.File]::WriteAllText($passFile, (Get-EnvValue "BACKUP_PASSPHRASE"))
    & (Find-Gpg) --batch --yes --pinentry-mode loopback --passphrase-file $passFile --output $plain --decrypt $latest.FullName
    if ($LASTEXITCODE -ne 0) { throw "dechiffrement echoue" }

    # Base temporaire a nom fixe, recreee a chaque test : jamais la base reelle.
    podman exec $Container psql -U dprest -d postgres -c "DROP DATABASE IF EXISTS $TestDb;"
    podman exec $Container psql -U dprest -d postgres -c "CREATE DATABASE $TestDb;"
    if ($LASTEXITCODE -ne 0) { throw "creation de $TestDb echouee" }

    podman cp $plain "${Container}:/tmp/restore.dump"
    podman exec $Container pg_restore -U dprest -d $TestDb --no-owner --exit-on-error /tmp/restore.dump
    if ($LASTEXITCODE -ne 0) { throw "pg_restore a echoue (code $LASTEXITCODE)" }
    Write-Log "pg_restore : OK"

    podman cp $SqlCounts "${Container}:/tmp/comptages_tables.sql"
    $restored = podman exec $Container psql -U dprest -d $TestDb -At -F "," -f /tmp/comptages_tables.sql
    if ($LASTEXITCODE -ne 0) { throw "comptage sur la base restauree echoue" }

    $reference = @{}
    $countsFile = $latest.FullName -replace "\.dump\.gpg$", ".counts.csv"
    foreach ($line in Get-Content $countsFile) {
        $parts = $line.Split(",")
        $reference[$parts[0]] = [int64]$parts[1]
    }

    foreach ($line in $restored) {
        $parts = $line.Split(",")
        $table = $parts[0]
        $rows  = [int64]$parts[1]
        if (-not $reference.ContainsKey($table)) {
            Write-Log ("  {0,-40} ABSENTE du manifeste" -f $table)
            $failed = $true
            continue
        }
        $ref = $reference[$table]
        if ($ref -eq 0) {
            $deltaPct = if ($rows -eq 0) { 0 } else { 100 }
        } else {
            $deltaPct = [math]::Abs($rows - $ref) * 100.0 / $ref
        }
        $ok = $deltaPct -le $TolerancePct
        $verdict = if ($ok) { "OK" } else { "ECART" }
        Write-Log ("  {0,-40} reference {1,10}  restaure {2,10}  ecart {3,6:N2} %  {4}" -f $table, $ref, $rows, $deltaPct, $verdict)
        if (-not $ok) { $failed = $true }
    }
    foreach ($table in $reference.Keys) {
        if (-not ($restored | Where-Object { $_.StartsWith("$table,") })) {
            Write-Log "  $table MANQUANTE apres restauration"
            $failed = $true
        }
    }
}
catch {
    Write-Log "Test de restauration : ECHEC - $($_.Exception.Message)"
    $failed = $true
}
finally {
    podman exec $Container psql -U dprest -d postgres -c "DROP DATABASE IF EXISTS $TestDb;" | Out-Null
    podman exec $Container rm -f /tmp/restore.dump | Out-Null
    foreach ($f in @($plain, $passFile)) {
        if (Test-Path $f) { Remove-Item $f -Force }
    }
}

if ($failed) {
    Write-Log "Test de restauration : NON CONFORME"
    exit 1
}
Write-Log "Test de restauration : CONFORME"
