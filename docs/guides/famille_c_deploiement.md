# Guide — Famille C (prescriptions et pathologies) : déploiement et vérification

Objectif : mettre en service les KPI 25, 27, 30, 31, 32 et 33 (voir `docs/kpi.md`, famille C).
Conception et décisions : `docs/decisions.md`, entrée du 2026-09-21.

**Déjà fait (préalables, rejouables)**

- `sql/analytics/010_kpi_clinique.sql` appliqué sur `dprest_analytics` (tables, vues, droits).
- `sql/source/001_replica_identity_cdc.sql` appliqué sur `echo_db` (18 tables en `REPLICA IDENTITY FULL`).
- `connectors/debezium-postgres-json.json` mis à jour (18 tables suivies) — **pas encore poussé à Kafka Connect**.
- `flink/sql/kpi_prestations.sql` mis à jour (9 nouveaux sinks) — **pas encore soumis**.

**Reste à faire : les étapes 1 à 8 ci-dessous.** Elles suppriment les topics `dprest-json.*` et les
reconstruisent (instantané complet). Aucune donnée n'est perdue : la source est intacte, et Flink écrit en UPSERT.

> **Pourquoi un instantané complet ?** Ajouter des tables à `table.include.list` ne relit pas leur contenu
> existant. Le connecteur ne publierait que les nouvelles modifications. Même procédure que
> l'incident du 2026-09-11 (`docs/decisions.md`).

## Avant de commencer

1. Stack démarrée, tous les conteneurs `healthy`, simulation **arrêtée** (sinon la source bouge pendant l'instantané).
2. **Sauvegarde de la base analytique** : `.\scripts\backup-analytics.ps1`.
3. Note l'identifiant du job Flink en cours :

```powershell
podman exec pipeline_temps_reel-flink-jobmanager-1 curl -s http://localhost:8081/jobs/overview
```

Les commandes `podman exec ... kafka-*.sh` passent par l'écouteur local non authentifié du broker
(`localhost:9094`, joignable seulement depuis le conteneur — voir `scripts/kafka-secure-setup.ps1`).
Sous Git-Bash, préfixer par `MSYS_NO_PATHCONV=1` ; sous PowerShell, rien à faire.

## 1. Annuler le job Flink

```powershell
$jid = "<identifiant du job RUNNING>"
podman exec pipeline_temps_reel-flink-jobmanager-1 curl -s -X PATCH "http://localhost:8081/jobs/${jid}?mode=cancel"
```

## 2. Arrêter le connecteur

```powershell
$k = "pipeline_temps_reel-kafka-connect-1"; $u = "http://localhost:8083/connectors/dprest-postgres-source-json"
podman exec $k curl -s -X PUT "$u/stop"
```

## 3. Supprimer les topics de données

```powershell
$b = "pipeline_temps_reel-kafka-1"
$topics = podman exec $b /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9094 --list | Where-Object { $_ -like 'dprest-json.public.*' }
foreach ($t in $topics) { podman exec $b /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9094 --delete --topic $t }
```

Ne pas toucher aux topics `_connect-*`, `_schemas` ni `__debezium-heartbeat.*`.

## 4. Réinitialiser les offsets, pousser la nouvelle configuration, reprendre

```powershell
podman exec $k curl -s -X DELETE "$u/offsets"
$cfg = (Get-Content connectors\debezium-postgres-json.json -Raw -Encoding UTF8 | ConvertFrom-Json).config | ConvertTo-Json -Compress
$cfg | podman exec -i $k curl -s -X PUT -H "Content-Type: application/json" --data-binary "@-" "$u/config"
podman exec $k curl -s -X PUT "$u/resume"
```

## 5. Attendre la fin de l'instantané

Compter les messages des nouveaux topics jusqu'à ce que le nombre se stabilise (attendu : 68 271
prescriptions, 200 430 pathologies, 918 médicaments, 100 pathologies de référence, 149 DCI, à l'instant de la mesure du 2026-09-22 après réduction du volume) :

```powershell
podman exec $b /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9094 --topic dprest-json.public.TB_FACTURES_PATHOLOGIES
```

L'état du connecteur doit rester `RUNNING` (ligne `tasks[0].state`) :
`podman exec $k curl -s "$u/status"`.

## 6. Resoumettre le job Flink

```powershell
.\scripts\start-stack.ps1
```

Le script détecte l'absence de job actif, injecte les mots de passe (`.env`) dans une copie temporaire à
l'intérieur du conteneur et soumet `flink/sql/kpi_prestations.sql`. Il vérifie ensuite que le job est stable après 60 s.

## 7. Vérifier (comptages source / analytique)

```powershell
# Source
Get-Content evaluation\controle_source.sql -Raw | podman exec -i simulateur_v5-postgres-1 psql -U echo -d echo_db
# Analytique (compte propriétaire : les vues internes et les tables SGD)
Get-Content evaluation\controle_analytics.sql -Raw | podman exec -i pipeline_temps_reel-postgres-analytics-1 psql -U dprest -d dprest_analytics
```

Comparer les sections **20 à 24** deux à deux : elles doivent donner exactement les mêmes chiffres. La section
**25** doit renvoyer 0 doublon. Contrôles minimaux, attendus à l'instant de la mesure (2026-09-21) :

| Contrôle | Valeur attendue |
|---|---|
| Prescriptions (somme de `kpi_prescriptions_medicament_jour`) | 68 271 |
| Pathologies (somme de `kpi_pathologies_jour`) | 200 430 |
| Ententes avec prescription associée (`v_ep_clinique_detail`) | 37 159 sur 57 928 |
| Médicaments sur ententes acceptées | 29 324 |

Si les chiffres de la source ont évolué depuis, compare deux à deux plutôt qu'aux valeurs ci-dessus.

Vérifier aussi : job `RUNNING`, aucune exception dans l'UI Flink, `qualite_anomalies` non alimentée à tort,
et les droits : `dprest_lecture` lit `v_top10_medicaments` mais reçoit `permission denied` sur `fait_prescriptions`
(`tests/test_securite.py` et le contrôle ci-dessous).

## 8. Surveiller la mémoire du TaskManager

**État en tas JVM, pas RocksDB (décision du 2026-09-22).** Mesuré sur ce poste : RocksDB tombe à ~5 lignes/s
(disque de la VM à 6 ms par écriture synchrone), alors que l'état en tas rattrape ~100 000 factures en **une dizaine
de minutes**. Réglages en place : `SET 'state.backend.type' = 'hashmap'` dans le fichier SQL,
`taskmanager.memory.managed.fraction: 0.1` et `taskmanager.memory.process.size: 3840m` dans `docker-compose.yml`
(tas de ~2,4 Go). Voir `docs/decisions.md`, entrée du 2026-09-22.

**Libérer la mémoire pendant le rattrapage.** La VM (7 Go) tombe en pénurie (swap épuisé) si tout tourne : le
TaskManager cesse alors de répondre (« Heartbeat timed out ») et le job tombe. Avant de soumettre le job, arrêter
temporairement ce qui ne sert pas au calcul (aucune donnée ne change pendant ce temps), puis le remettre à la fin :

```powershell
$stop = "pipeline_temps_reel-akhq-1","pipeline_temps_reel-grafana-1","pipeline_temps_reel-prometheus-1","pipeline_temps_reel-blackbox-exporter-1","pipeline_temps_reel-kafka-connect-1"
$stop | ForEach-Object { podman stop $_ }
# ... soumettre le job, attendre la fin du rattrapage (étape 7) ...
$stop | Where-Object { $_ -notmatch 'akhq' } | ForEach-Object { podman start $_ }   # AKHQ est facultatif
```

`scripts\start-stack.ps1` redémarre tous les conteneurs (`podman compose start`) : pour garder cette marge, soumettre le
job à la main (copie du fichier SQL dans le JobManager, remplacement des jetons `__FLINK_WRITER_PASSWORD__` et
`__FLINK_KAFKA_PASSWORD__` par les valeurs du `.env`, puis `./bin/sql-client.sh -f`), comme le fait ce script.

Si le job retombe malgré tout : réduire le volume de la source (voir la limite de capacité dans `decisions.md`) plutôt
que d'agrandir le tas.

**Ne jamais lancer deux fois le script de démarrage** : il soumet un job si aucun ne tourne, deux exécutions
simultanées créent deux jobs identiques qui se partagent la VM.

## Retour arrière

1. Annuler le job Flink (étape 1).
2. Remettre dans `connectors/debezium-postgres-json.json` la liste de 13 tables (retirer les 5 tables cliniques) et
   refaire les étapes 2 à 4 (le retrait d'une table de `table.include.list` suffit à arrêter sa capture).
3. Retirer du fichier Flink les blocs « Famille C » (sources, cibles, `INSERT`) et la colonne `FACTURE_NUMERO` de `ep_src`,
   puis resoumettre (étape 6).
4. Les tables et vues de `010` peuvent rester en place : elles sont inertes sans alimentation. Pour les supprimer,
   sauvegarde d'abord (`.\scripts\backup-analytics.ps1`) puis `DROP VIEW` / `DROP TABLE` explicites, après validation.

## Ce qui n'est pas dans ce guide

- Le dashboard Superset « DPREST - Clinique » (datasets sur les vues `v_top10_*` et `v_kpi_ep_*`), à créer
  puis à exporter dans `superset/exports/`. Tant que la DPREST n'a pas validé les KPI cliniques, le réserver aux administrateurs du SGD.
- Un test automatisé de bout en bout (Kafka + Flink + PostgreSQL) : les tests actuels (`tests/test_famille_c.py`)
  contrôlent la cohérence des fichiers, pas le flux réel.
