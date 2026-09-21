# Étape 7b — Sauvegarde et restauration de `postgres-analytics`

## Pourquoi

Le volume `analytics_data` est la seule copie des KPI et des tables d'anomalies. Le CDC permet de
recalculer les KPI par relecture des topics Kafka, mais seulement tant que la rétention Kafka et le
slot de réplication le permettent : la sauvegarde évite de dépendre de ce rejeu pour une simple
corruption du volume.

## Ce qui est en place

| Élément | Fichier | Rôle |
|---|---|---|
| Sauvegarde | `scripts/backup-analytics.ps1` | `pg_dump -Fc`, chiffrement GnuPG (AES256), SHA-256, manifeste de comptages, purge à 7 jours |
| Test de restauration | `scripts/restore-test-analytics.ps1` | Vérifie le SHA-256, restaure dans `dprest_restore_test`, compare les comptages, supprime la base temporaire |
| Planification | `scripts/register-backup-task.ps1` | Tâche quotidienne du Planificateur Windows (13:00 par défaut) |
| Comptages | `scripts/sql/comptages_tables.sql` | Comptage exact des lignes par table |
| Secret | `BACKUP_PASSPHRASE` dans `.env` | Passphrase de chiffrement, non versionnée |

Les sauvegardes sont dans `backups/` (exclu de Git). Journal : `backups/backup.log`.

## Utilisation

```powershell
.\scripts\backup-analytics.ps1                 # sauvegarde immédiate
.\scripts\restore-test-analytics.ps1           # test sur la dernière sauvegarde (code 0 = conforme)
.\scripts\register-backup-task.ps1 -Time 13:00 # planification quotidienne (une seule fois)
```

## Restauration réelle (retour arrière après incident)

1. Arrêter Flink et les consommateurs d'écriture : `podman compose stop flink-jobmanager flink-taskmanager`.
2. Déchiffrer : `gpg --output analytics.dump --decrypt backups/analytics_<horodatage>.dump.gpg`
   (passphrase = `BACKUP_PASSPHRASE`).
3. Copier dans le conteneur puis restaurer dans une base vide :
   `pg_restore -U dprest -d <base_vide> --no-owner analytics.dump`.
4. Contrôler les comptages avec `backups/analytics_<horodatage>.counts.csv`.
5. Redémarrer Flink : les UPSERT réconcilient l'écart entre la sauvegarde et l'état courant.
6. Supprimer `analytics.dump` en clair.

## Limites assumées

- **Tolérance de comptage (0,5 %)** : Flink réécrit les tables KPI en continu entre le `pg_dump` et
  le comptage de référence. Un test à tolérance nulle donnerait de faux échecs tant que le job
  tourne. Pour une vérification stricte, arrêter Flink avant la sauvegarde.
- **Sauvegarde sur le même poste** : elle protège contre la corruption du volume, pas contre la
  perte du poste. En production, copier `backups/` vers un stockage distant.
- **Passphrase unique dans `.env`** : pas de rotation automatisée.
- **Si la stack est arrêtée à l'heure planifiée**, la sauvegarde échoue (trace dans le journal) et
  reprend à l'échéance suivante.
- **Base source non sauvegardée** : elle appartient au projet ÉCHO.
