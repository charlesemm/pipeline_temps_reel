<#
.SYNOPSIS
    Enregistre la sauvegarde quotidienne dans le Planificateur de taches Windows.

.DESCRIPTION
    La stack n'etant pas allumee en permanence, la tache s'execute chaque
    jour a l'heure choisie ; si le conteneur est arrete, backup-analytics.ps1
    echoue proprement (code 1, trace dans backups/backup.log) et la tache
    reessaie a la prochaine echeance. A lancer une seule fois par l'utilisateur.
    Retrait : Unregister-ScheduledTask -TaskName "pipeline-dprest-sauvegarde"
#>
param(
    [string]$Time = "13:00"
)

$ErrorActionPreference = "Stop"

$script = Join-Path $PSScriptRoot "backup-analytics.ps1"
$action = New-ScheduledTaskAction -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$script`""
$trigger  = New-ScheduledTaskTrigger -Daily -At $Time
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -ExecutionTimeLimit (New-TimeSpan -Hours 1)

Register-ScheduledTask -TaskName "pipeline-dprest-sauvegarde" -Action $action -Trigger $trigger `
    -Settings $settings -Description "Sauvegarde chiffree quotidienne de postgres-analytics" -Force | Out-Null

Write-Host "Tache 'pipeline-dprest-sauvegarde' enregistree, tous les jours a $Time." -ForegroundColor Green
