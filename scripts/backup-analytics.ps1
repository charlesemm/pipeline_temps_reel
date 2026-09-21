<#
.SYNOPSIS
    Sauvegarde chiffree de la base analytique (postgres-analytics).

.DESCRIPTION
    pg_dump (format custom) execute dans le conteneur, copie sur l'hote puis
    chiffre (GnuPG, AES256, passphrase BACKUP_PASSPHRASE lue dans .env).
    Le dump en clair est supprime des que le chiffrement est termine : les
    KPI contiennent des donnees de sante derivees, elles ne doivent pas
    trainer en clair. Produit dans backups/ :
      analytics_<horodatage>.dump.gpg          dump chiffre
      analytics_<horodatage>.dump.gpg.sha256   somme de controle du fichier chiffre
      analytics_<horodatage>.counts.csv        comptage par table (reference du test de restauration)
    Purge ensuite les sauvegardes plus vieilles que -RetentionDays.
    Voir docs/guides/etape7b_sauvegarde.md.
#>
param(
    [int]$RetentionDays = 7
)

# "Continue" : PowerShell 5.1 traite tout message stderr d'un exe natif (NOTICE psql, info gpg)
# comme une erreur fatale avec "Stop". Les echecs sont detectes via $LASTEXITCODE.
$ErrorActionPreference = "Continue"

$PipelineDir = "C:\Users\charles.nguessan\OneDrive - IPSCNAM\Documents\pipeline_temps_reel"
$Container   = "pipeline_temps_reel-postgres-analytics-1"
$BackupDir   = Join-Path $PipelineDir "backups"
$SqlCounts   = Join-Path $PSScriptRoot "sql\comptages_tables.sql"
$LogFile     = Join-Path $BackupDir "backup.log"

function Write-Log($message) {
    $line = "{0}  {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $message
    Write-Host $line
    Add-Content -Path $LogFile -Value $line -Encoding UTF8
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
    throw "gpg introuvable (installer Gpg4win ou Git for Windows)"
}

New-Item -ItemType Directory -Force $BackupDir | Out-Null
$stamp     = Get-Date -Format "yyyyMMdd_HHmmss"
$plain     = Join-Path $BackupDir "analytics_$stamp.dump"
$encrypted = "$plain.gpg"
$counts    = Join-Path $BackupDir "analytics_$stamp.counts.csv"
$passFile  = Join-Path $env:TEMP "bk_$stamp.pass"
$started   = Get-Date

try {
    $state = podman ps --filter "name=$Container" --format "{{.Status}}"
    if (-not $state -or $state -notmatch "Up") {
        throw "conteneur $Container non demarre (lancer scripts\start-stack.ps1)"
    }

    Write-Log "Sauvegarde : debut"
    podman exec $Container pg_dump -U dprest -d dprest_analytics -Fc -f /tmp/analytics.dump
    if ($LASTEXITCODE -ne 0) { throw "pg_dump a echoue (code $LASTEXITCODE)" }

    podman cp "${Container}:/tmp/analytics.dump" $plain
    if ($LASTEXITCODE -ne 0) { throw "podman cp a echoue" }
    podman exec $Container rm -f /tmp/analytics.dump

    # Comptage juste apres le dump : reference du test de restauration.
    podman cp $SqlCounts "${Container}:/tmp/comptages_tables.sql"
    podman exec $Container psql -U dprest -d dprest_analytics -At -F "," -f /tmp/comptages_tables.sql |
        Set-Content -Path $counts -Encoding ASCII
    if ($LASTEXITCODE -ne 0) { throw "comptage par table echoue" }

    # Passphrase dans un fichier temporaire sans saut de ligne, supprime dans finally.
    [IO.File]::WriteAllText($passFile, (Get-EnvValue "BACKUP_PASSPHRASE"))
    & (Find-Gpg) --batch --yes --pinentry-mode loopback --passphrase-file $passFile `
        --symmetric --cipher-algo AES256 --output $encrypted $plain
    if ($LASTEXITCODE -ne 0) { throw "chiffrement gpg echoue" }

    $hash = (Get-FileHash -Algorithm SHA256 $encrypted).Hash.ToLower()
    "$hash  $(Split-Path $encrypted -Leaf)" | Set-Content -Path "$encrypted.sha256" -Encoding ASCII

    $sizeMb  = [math]::Round((Get-Item $encrypted).Length / 1MB, 2)
    $tables  = @(Get-Content $counts).Count
    $seconds = [math]::Round(((Get-Date) - $started).TotalSeconds, 1)
    Write-Log "Sauvegarde : OK - $(Split-Path $encrypted -Leaf), $sizeMb Mo, $tables tables, $seconds s"

    $cutoff = (Get-Date).AddDays(-$RetentionDays)
    $old = Get-ChildItem $BackupDir -Filter "analytics_*" | Where-Object { $_.LastWriteTime -lt $cutoff }
    foreach ($file in $old) { Remove-Item $file.FullName -Force }
    Write-Log "Purge : $(@($old).Count) fichier(s) de plus de $RetentionDays jours supprime(s)"
}
catch {
    Write-Log "Sauvegarde : ECHEC - $($_.Exception.Message)"
    exit 1
}
finally {
    foreach ($f in @($plain, $passFile)) {
        if (Test-Path $f) { Remove-Item $f -Force }
    }
}
