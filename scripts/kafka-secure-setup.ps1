<#
.SYNOPSIS
    Cree les utilisateurs SCRAM et les ACL de Kafka (etape 7e).

.DESCRIPTION
    S'execute via l'ecouteur LOCAL du broker (localhost:9094, en clair, non
    joignable hors du conteneur kafka). Idempotent : rejouer le script
    met a jour les mots de passe et ne duplique pas les ACL.

    Utilisateurs (mots de passe KAFKA_<NOM>_PASSWORD dans .env) :
      admin            super-utilisateur (operations depuis l'hote, port 29092)
      connect          Kafka Connect / Debezium : ecrit dprest-json.*, topics internes _connect-*
      flink            job kpi-continu : lit dprest-json.* uniquement
      akhq             interface d'inspection : lecture seule
      schema-registry  topic _schemas

    Voir docs/guides/etape7e_kafka_securise.md.
#>
$ErrorActionPreference = "Continue"

$PipelineDir = "C:\Users\charles.nguessan\OneDrive - IPSCNAM\Documents\pipeline_temps_reel"
$Kafka       = "pipeline_temps_reel-kafka-1"
$Local       = "localhost:9094"
$Bin         = "/opt/kafka/bin"

function Get-EnvValue($name) {
    $match = Select-String -Path (Join-Path $PipelineDir ".env") -Pattern "^$name=(.*)$" | Select-Object -First 1
    if (-not $match) { Write-Host "$name absent de .env" -ForegroundColor Red; exit 1 }
    return $match.Matches[0].Groups[1].Value.Trim()
}

function Set-KafkaUser($name, $envName) {
    $password = Get-EnvValue $envName
    podman exec $Kafka "$Bin/kafka-configs.sh" --bootstrap-server $Local --alter `
        --add-config "SCRAM-SHA-512=[password=$password]" --entity-type users --entity-name $name | Out-Null
    if ($LASTEXITCODE -ne 0) { Write-Host "Utilisateur $name : ECHEC" -ForegroundColor Red; exit 1 }
    Write-Host "  utilisateur $name : OK"
}

function Add-Acl {
    podman exec $Kafka "$Bin/kafka-acls.sh" --bootstrap-server $Local --add @args | Out-Null
    if ($LASTEXITCODE -ne 0) { Write-Host "ACL en echec : $args" -ForegroundColor Red; exit 1 }
}

Write-Host "==> Utilisateurs SCRAM"
Set-KafkaUser "admin"           "KAFKA_ADMIN_PASSWORD"
Set-KafkaUser "connect"         "KAFKA_CONNECT_PASSWORD"
Set-KafkaUser "flink"           "KAFKA_FLINK_PASSWORD"
Set-KafkaUser "akhq"            "KAFKA_AKHQ_PASSWORD"
Set-KafkaUser "schema-registry" "KAFKA_SCHEMA_REGISTRY_PASSWORD"

Write-Host "==> ACL"
# connect : ecrit les evenements CDC et gere ses topics internes.
Add-Acl --allow-principal User:connect --operation Write --operation Create --operation Describe --operation DescribeConfigs --topic "dprest-json." --resource-pattern-type prefixed
Add-Acl --allow-principal User:connect --operation Write --operation Create --operation Describe --topic "__debezium-heartbeat." --resource-pattern-type prefixed
Add-Acl --allow-principal User:connect --operation All --topic "_connect-" --resource-pattern-type prefixed
Add-Acl --allow-principal User:connect --operation Read --operation Describe --group "pipeline-connect"
Add-Acl --allow-principal User:connect --operation Describe --cluster

# flink : lecture seule des topics CDC, groupes flink-kpi-*.
Add-Acl --allow-principal User:flink --operation Read --operation Describe --topic "dprest-json." --resource-pattern-type prefixed
Add-Acl --allow-principal User:flink --operation Read --operation Describe --group "flink-kpi-" --resource-pattern-type prefixed

# akhq : inspection en lecture seule (ne peut ni ecrire ni supprimer).
Add-Acl --allow-principal User:akhq --operation Read --operation Describe --operation DescribeConfigs --topic "*"
Add-Acl --allow-principal User:akhq --operation Describe --group "*"
Add-Acl --allow-principal User:akhq --operation Describe --cluster

# schema-registry : son topic _schemas et son groupe.
Add-Acl --allow-principal User:schema-registry --operation All --topic "_schemas"
Add-Acl --allow-principal User:schema-registry --operation All --group "schema-registry"
Add-Acl --allow-principal User:schema-registry --operation Describe --operation DescribeConfigs --cluster

Write-Host "Kafka securise : utilisateurs et ACL en place." -ForegroundColor Green
