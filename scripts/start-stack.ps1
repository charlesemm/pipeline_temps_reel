<#
.SYNOPSIS
    Starts the whole real-time pipeline after a machine reboot.

.DESCRIPTION
    Three things never come back on their own after the PC is switched off:
    the Podman VM, the containers, and the Flink job (no HA storage
    configured, so the job dies with the JobManager).

    Order matters: simulateur_V5 must be up before Kafka Connect, otherwise
    the Debezium tasks start in FAILED state.

    See docs/guides/demarrage_arret.md.
#>

# PAS de "$ErrorActionPreference = 'Stop'" ici : podman/podman-compose écrit
# des messages d'information tout à fait normaux sur le flux d'erreur (ex.
# "Executing external compose provider..."). Sous PowerShell 5.1, avec
# $ErrorActionPreference = "Stop", ce genre de sortie stderr anodine est
# transformée en erreur fatale (NativeCommandError) qui interrompt le
# script en plein milieu — vécu concrètement le 2026-09-11 : le script
# s'arrêtait après l'étape 2, avant même d'atteindre la resoumission du
# job Flink. Les échecs réels sont détectés explicitement plus bas via
# $LASTEXITCODE.
$ErrorActionPreference = "Continue"

$PipelineDir   = "C:\Users\charles.nguessan\OneDrive - IPSCNAM\Documents\pipeline_temps_reel"
$SimulatorDir  = "C:\Users\charles.nguessan\OneDrive - IPSCNAM\Documents\simulateur_V5"
$FlinkJob      = "flink/sql/kpi_prestations.sql"
$JobManager    = "pipeline_temps_reel-flink-jobmanager-1"
$KafkaConnect  = "pipeline_temps_reel-kafka-connect-1"
# Un seul connecteur : la version Avro (dprest-postgres-source) a été
# retirée le 2026-09-11, non utilisée par Flink — voir docs/decisions.md.
$Connectors    = @("dprest-postgres-source-json")

# Étape 7 : charge .env (non versionné) dans des variables PowerShell —
# nécessaire pour injecter FLINK_WRITER_PASSWORD dans le job Flink au moment
# de sa soumission, sans jamais l'écrire dans flink/sql/kpi_prestations.sql
# (voir docs/decisions.md).
$EnvVars = @{}
Get-Content "$PipelineDir\.env" -Encoding UTF8 | ForEach-Object {
    if ($_ -match '^\s*#' -or $_ -match '^\s*$') { return }
    $parts = $_.Split('=', 2)
    if ($parts.Count -eq 2) { $EnvVars[$parts[0].Trim()] = $parts[1].Trim() }
}

function Write-Step($message) {
    Write-Host ""
    Write-Host "==> $message" -ForegroundColor Cyan
}

function Wait-Healthy($containerName, $timeoutSeconds = 120) {
    $deadline = (Get-Date).AddSeconds($timeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $status = podman ps --filter "name=$containerName" --format "{{.Status}}"
        if ($status -match "healthy") {
            Write-Host "    $containerName : pret" -ForegroundColor Green
            return $true
        }
        Start-Sleep -Seconds 5
    }
    Write-Host "    $containerName : toujours pas 'healthy' apres $timeoutSeconds s" -ForegroundColor Yellow
    Write-Host "    -> podman logs $containerName --tail 50" -ForegroundColor Yellow
    return $false
}

# -- 1. Podman VM -------------------------------------------------------
Write-Step "Demarrage de la VM Podman"
$machineState = podman machine list --format "{{.Running}}"
if ($machineState -match "true") {
    Write-Host "    Deja demarree."
} else {
    podman machine start
}

# -- 2. Base source (simulateur_V5) -------------------------------------
Write-Step "Demarrage de simulateur_V5 (base source)"
Push-Location $SimulatorDir
try {
    podman compose start
    if ($LASTEXITCODE -ne 0) {
        Write-Host "    ATTENTION : 'podman compose start' a retourne le code $LASTEXITCODE" -ForegroundColor Yellow
        Write-Host "    (souvent sans gravite : simple message sur stderr. On continue.)" -ForegroundColor Yellow
    }
} finally {
    Pop-Location
}
Wait-Healthy "simulateur_v5-postgres-1" | Out-Null

# -- 3. Pipeline --------------------------------------------------------
Write-Step "Demarrage du pipeline (Kafka, Schema Registry, Connect, Flink, PostgreSQL analytique)"
Push-Location $PipelineDir
podman compose start
if ($LASTEXITCODE -ne 0) {
    Write-Host "    ATTENTION : 'podman compose start' a retourne le code $LASTEXITCODE" -ForegroundColor Yellow
    Write-Host "    (souvent sans gravite : simple message sur stderr. On continue.)" -ForegroundColor Yellow
}

Wait-Healthy "pipeline_temps_reel-kafka-1"              | Out-Null
Wait-Healthy "pipeline_temps_reel-schema-registry-1"    | Out-Null
Wait-Healthy "pipeline_temps_reel-kafka-connect-1"      | Out-Null
Wait-Healthy "pipeline_temps_reel-postgres-analytics-1" | Out-Null
Wait-Healthy "pipeline_temps_reel-flink-jobmanager-1"   | Out-Null
Wait-Healthy "pipeline_temps_reel-prometheus-1"         | Out-Null
Wait-Healthy "pipeline_temps_reel-grafana-1"            | Out-Null
Wait-Healthy "pipeline_temps_reel-superset-db-1"        | Out-Null
Wait-Healthy "pipeline_temps_reel-superset-redis-1"     | Out-Null
Wait-Healthy "pipeline_temps_reel-superset-1"           | Out-Null
Wait-Healthy "pipeline_temps_reel-reverse-proxy-1"      | Out-Null

# Grafana n'a pas de volume persistant : le mot de passe admin défini
# par variable d'environnement ne s'applique qu'a la toute premiere
# creation de sa base -> reinitialise systematiquement pour eviter la
# surprise "Invalid username or password" apres un redemarrage complet.
podman exec pipeline_temps_reel-grafana-1 grafana-cli admin reset-admin-password $($EnvVars['GRAFANA_ADMIN_PASSWORD']) 2>&1 | Out-Null

# -- 4. Connecteurs Debezium --------------------------------------------
# Une tache part en FAILED si la base source a redemarre apres Kafka
# Connect : on relance systematiquement, l'operation est sans effet sur
# une tache deja saine.
Write-Step "Verification des connecteurs Debezium"
foreach ($connector in $Connectors) {
    $status = podman exec $KafkaConnect curl -s "http://localhost:8083/connectors/$connector/status"
    if ($status -match '"tasks":\[\{"id":0,"state":"RUNNING"') {
        Write-Host "    $connector : RUNNING" -ForegroundColor Green
    } else {
        Write-Host "    $connector : tache a relancer" -ForegroundColor Yellow
        podman exec $KafkaConnect curl -s -X POST "http://localhost:8083/connectors/$connector/tasks/0/restart" | Out-Null
        Start-Sleep -Seconds 10
        $status = podman exec $KafkaConnect curl -s "http://localhost:8083/connectors/$connector/status"
        if ($status -match '"tasks":\[\{"id":0,"state":"RUNNING"') {
            Write-Host "    $connector : RUNNING apres relance" -ForegroundColor Green
        } else {
            Write-Host "    $connector : TOUJOURS EN ECHEC" -ForegroundColor Red
            Write-Host "    $status"
        }
    }
}

# -- 5. Job Flink -------------------------------------------------------
Write-Step "Job Flink"
$jobs = podman exec $JobManager curl -s http://localhost:8081/jobs
if ($jobs -match '"status":"RUNNING"') {
    Write-Host "    Un job tourne deja : pas de resoumission." -ForegroundColor Green
    Write-Host "    (Soumettre une seconde fois ferait tourner deux jobs en parallele.)"
} else {
    Write-Host "    Aucun job actif : soumission de $FlinkJob"
    podman cp $FlinkJob "${JobManager}:/tmp/kpi_prestations.sql"
    # Étape 7 : le fichier versionné ne contient que le jeton
    # __FLINK_WRITER_PASSWORD__ ; la vraie valeur (depuis .env) est
    # injectée ici, dans la copie temporaire à l'intérieur du
    # conteneur — jamais écrite sur disque en clair côté hôte ni dans Git.
    podman exec $JobManager sed -i "s/__FLINK_WRITER_PASSWORD__/$($EnvVars['FLINK_WRITER_PASSWORD'])/g" /tmp/kpi_prestations.sql
    podman exec $JobManager sed -i "s/__FLINK_KAFKA_PASSWORD__/$($EnvVars['KAFKA_FLINK_PASSWORD'])/g" /tmp/kpi_prestations.sql
    podman exec $JobManager ./bin/sql-client.sh -f /tmp/kpi_prestations.sql

    # Un job mal configure echoue dans les 30 a 60 premieres secondes :
    # verifier apres coup, pas au moment de la soumission.
    Write-Host "    Verification de la stabilite du job (60 s)..."
    Start-Sleep -Seconds 60
    $jobs = podman exec $JobManager curl -s http://localhost:8081/jobs
    if ($jobs -match '"status":"RUNNING"') {
        Write-Host "    Job RUNNING et stable." -ForegroundColor Green
    } else {
        Write-Host "    Le job n'est pas RUNNING : $jobs" -ForegroundColor Red
        Write-Host "    -> UI Flink, onglet 'Completed Jobs', pour lire l'exception." -ForegroundColor Red
    }
}

Pop-Location

# -- 6. Recapitulatif ---------------------------------------------------
# L'IP de la VM change a chaque demarrage et 'localhost' est injoignable
# en mode rootful : c'est la seule adresse utilisable depuis Windows.
$ipLine = podman machine ssh "ip -4 -o addr show eth0"
$vmIp = [regex]::Match($ipLine, 'inet (\d+\.\d+\.\d+\.\d+)').Groups[1].Value

Write-Host ""
Write-Host "=======================================================" -ForegroundColor Cyan
Write-Host " Stack demarree - IP de la VM Podman : $vmIp" -ForegroundColor Cyan
Write-Host "=======================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "  Dashboard simulateur   http://${vmIp}:8000"
Write-Host "  Flink                  http://${vmIp}:8082"
Write-Host "  Grafana (supervision)  https://${vmIp}:3443  (admin / voir .env GRAFANA_ADMIN_PASSWORD)"
Write-Host "  Superset (DPREST)      https://${vmIp}:8443  (identifiants dans .env ; compte 'dprest' en lecture seule)"
Write-Host "  (certificat auto-signe : le navigateur affichera un avertissement, normal - voir docs/guides/etape7_securite.md)"
Write-Host "  Prometheus             http://${vmIp}:9091"
Write-Host "  AKHQ (Kafka)           http://${vmIp}:8085"
Write-Host "  Base KPI (pgAdmin)     ${vmIp}:15433  /  dprest_analytics"
Write-Host ""
Write-Host "  Etat des conteneurs :"
podman ps --format "    {{.Names}}: {{.Status}}"
Write-Host ""
