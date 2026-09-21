<#
.SYNOPSIS
    Archive le journal d'audit d'une journee et l'ajoute a une chaine de hachage.

.DESCRIPTION
    Extrait pour le jour -Day (defaut : hier) :
      audit/postgres-analytics_<jour>.log   journal PostgreSQL analytique (connexions, DDL, refus)
      audit/superset_<jour>.csv             evenements Superset (table logs, avec l'utilisateur)
    puis inscrit dans audit/manifest.log une ligne par fichier :
      jour  fichier  sha256_fichier  chaine_precedente  chaine
    ou chaine = SHA256(chaine_precedente + sha256_fichier + fichier).
    Modifier, supprimer ou reordonner un fichier archive casse la chaine ;
    scripts/audit-verify.ps1 le detecte. Les fichiers sont ensuite passes en
    lecture seule. Un jour deja archive n'est jamais reecrit (idempotent).
    Voir docs/guides/etape7c_audit.md.
#>
param(
    [string]$Day = (Get-Date).AddDays(-1).ToString("yyyy-MM-dd")
)

$ErrorActionPreference = "Continue"

$PipelineDir  = "C:\Users\charles.nguessan\OneDrive - IPSCNAM\Documents\pipeline_temps_reel"
$AuditDir     = Join-Path $PipelineDir "audit"
$Manifest     = Join-Path $AuditDir "manifest.log"
$PgContainer  = "pipeline_temps_reel-postgres-analytics-1"
$SsContainer  = "pipeline_temps_reel-superset-db-1"
$GenesisChain = ("0" * 64)

function Write-Log($message) {
    Write-Host ("{0}  {1}" -f (Get-Date -Format "yyyy-MM-dd HH:mm:ss"), $message)
}

function Get-Sha256Text($text) {
    $sha = [System.Security.Cryptography.SHA256]::Create()
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($text)
    return ([BitConverter]::ToString($sha.ComputeHash($bytes)) -replace "-", "").ToLower()
}

if ($Day -notmatch '^\d{4}-\d{2}-\d{2}$') { Write-Log "Format de jour invalide : $Day (attendu yyyy-MM-dd)"; exit 1 }

New-Item -ItemType Directory -Force $AuditDir | Out-Null
if (-not (Test-Path $Manifest)) { New-Item -ItemType File $Manifest | Out-Null }

$dayStart = [datetime]::ParseExact($Day, "yyyy-MM-dd", $null)
$since    = $dayStart.ToString("yyyy-MM-ddTHH:mm:ss")
$until    = $dayStart.AddDays(1).ToString("yyyy-MM-ddTHH:mm:ss")

$pgFile = Join-Path $AuditDir "postgres-analytics_$Day.log"
$ssFile = Join-Path $AuditDir "superset_$Day.csv"

if (Select-String -Path $Manifest -Pattern "^$Day " -Quiet) {
    Write-Log "Jour $Day deja archive : rien a faire."
    exit 0
}

# 1. Journal PostgreSQL analytique (stdout + stderr du conteneur, fenetre du jour).
podman logs --since $since --until $until $PgContainer 2>&1 |
    ForEach-Object { "$_" } | Set-Content -Path $pgFile -Encoding UTF8

# 2. Evenements Superset du jour, avec le nom d'utilisateur (requete parametree par
#    la seule valeur $Day, validee par l'expression reguliere ci-dessus).
$query = "SELECT l.dttm, COALESCE(u.username, 'anonyme') AS utilisateur, l.action, l.dashboard_id, l.slice_id, l.duration_ms " +
         "FROM logs AS l LEFT JOIN ab_user AS u ON u.id = l.user_id " +
         "WHERE l.dttm >= '$Day'::date AND l.dttm < '$Day'::date + 1 ORDER BY l.dttm"
podman exec $SsContainer psql -U superset -d superset --csv -c $query | Set-Content -Path $ssFile -Encoding UTF8
if ($LASTEXITCODE -ne 0) { Write-Log "Extraction Superset echouee"; Remove-Item $pgFile, $ssFile -Force -ErrorAction SilentlyContinue; exit 1 }

# 3. Chaine de hachage.
$lines = @(Get-Content $Manifest)
$previous = if ($lines.Count -eq 0) { $GenesisChain } else { ($lines[-1] -split "\s+")[4] }

foreach ($file in @($pgFile, $ssFile)) {
    $name     = Split-Path $file -Leaf
    $fileHash = (Get-FileHash -Algorithm SHA256 $file).Hash.ToLower()
    $chain    = Get-Sha256Text ($previous + $fileHash + $name)
    Add-Content -Path $Manifest -Value "$Day $name $fileHash $previous $chain" -Encoding ASCII
    Set-ItemProperty -Path $file -Name IsReadOnly -Value $true
    $rows = @(Get-Content $file).Count
    Write-Log "Archive : $name ($rows lignes), chaine $($chain.Substring(0,12))..."
    $previous = $chain
}
