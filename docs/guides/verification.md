# Guide — Comment vérifier soi-même que le pipeline fonctionne

Ce guide permet de contrôler, sans avoir à me faire confiance, que le pipeline fait bien ce qu'il
prétend faire. Chaque vérification indique **la commande**, **ce qu'on doit obtenir**, et surtout
**ce que ça prouve** — cette dernière colonne est celle qui sert en soutenance.

Toutes les commandes sont données pour **PowerShell**, depuis le dossier
`C:\Users\charles.nguessan\Documents\pipeline_temps_reel`.

> Si tu utilises Git Bash au lieu de PowerShell, préfixe chaque commande contenant un chemin
> commençant par `/` avec `MSYS_NO_PATHCONV=1` (voir `docs/guides/demarrage_arret.md`).

---

## Étape préalable : tout démarrer

```powershell
podman machine start

cd "C:\Users\charles.nguessan\OneDrive - IPSCNAM\Documents\simulateur_V5"
podman compose start

cd "C:\Users\charles.nguessan\Documents\pipeline_temps_reel"
podman compose start
```

Puis resoumettre le job Flink (il ne survit pas à un redémarrage) :

```powershell
podman cp flink/sql/kpi_prestations.sql pipeline_temps_reel-flink-jobmanager-1:/tmp/kpi_prestations.sql
podman exec pipeline_temps_reel-flink-jobmanager-1 ./bin/sql-client.sh -f /tmp/kpi_prestations.sql
```

Laisser tourner 2 à 3 minutes avant de vérifier : le job doit rattraper l'historique complet.

---

## Vérification 1 — Tous les composants tournent

```powershell
podman ps --format "{{.Names}}: {{.Status}}"
```

**Attendu** : 8 conteneurs, tous `Up` et `(healthy)` sauf `flink-taskmanager` (pas de sonde de santé
définie) :

```
pipeline_temps_reel-kafka-1              Up (healthy)
pipeline_temps_reel-schema-registry-1    Up (healthy)
pipeline_temps_reel-kafka-connect-1      Up (healthy)
pipeline_temps_reel-postgres-analytics-1 Up (healthy)
pipeline_temps_reel-flink-jobmanager-1   Up (healthy)
pipeline_temps_reel-flink-taskmanager-1  Up
simulateur_v5-postgres-1                 Up (healthy)
simulateur_v5-api-1                      Up (healthy)
```

**Ce que ça prouve** : rien du tout sur la justesse des données — seulement que les processus tournent.
C'est la vérification la plus superficielle, et c'est justement le piège : un pipeline « tout vert »
peut produire des résultats entièrement faux (ça nous est arrivé, voir `docs/decisions.md`).

---

## Vérification 2 — La capture CDC est active

```powershell
podman exec pipeline_temps_reel-kafka-connect-1 curl -s http://localhost:8083/connectors/dprest-postgres-source-json/status
```

**Attendu** : `"state":"RUNNING"` **deux fois** — pour le connecteur *et* pour sa tâche :

```json
{"name":"dprest-postgres-source-json","connector":{"state":"RUNNING",...},"tasks":[{"id":0,"state":"RUNNING",...}]}
```

**Ce que ça prouve** : Debezium lit bien le journal de transactions de PostgreSQL. Si la tâche est
`FAILED`, la relancer :

```powershell
podman exec pipeline_temps_reel-kafka-connect-1 curl -s -X POST http://localhost:8083/connectors/dprest-postgres-source-json/tasks/0/restart
```

---

## Vérification 3 — Le job Flink tourne et tient

```powershell
podman exec pipeline_temps_reel-flink-jobmanager-1 curl -s http://localhost:8081/jobs
```

**Attendu** : au moins un job en `"status":"RUNNING"`.

**Ce que ça prouve** : le traitement est actif. **Attention** : un job qui échoue le fait généralement
dans les 30 à 60 premières secondes. Un job `RUNNING` depuis plusieurs minutes est un bon signe ; un
job qu'on vient de soumettre ne prouve rien. Vérifier aussi visuellement sur **http://localhost:8082**
(interface Flink), onglet *Running Jobs*.

---

## Vérification 4 — LA vérification décisive : les chiffres sont-ils justes ?

C'est la seule qui prouve vraiment quelque chose. Le principe : recalculer les mêmes KPI
**indépendamment**, directement sur la base source, et comparer.

Deux scripts sont fournis, dont les sections portent les mêmes numéros pour faciliter la comparaison.

### 4a. Calcul de contrôle sur la base source

```powershell
podman cp evaluation/controle_source.sql simulateur_v5-postgres-1:/tmp/controle_source.sql
podman exec simulateur_v5-postgres-1 psql -U echo -d echo_db -f /tmp/controle_source.sql
```

### 4b. KPI calculés par le pipeline

```powershell
podman cp evaluation/controle_analytics.sql pipeline_temps_reel-postgres-analytics-1:/tmp/controle_analytics.sql
podman exec pipeline_temps_reel-postgres-analytics-1 psql -U dprest -d dprest_analytics -f /tmp/controle_analytics.sql
```

### 4c. Comparer

Mets les deux sorties côte à côte. Exemple réel obtenu lors de la validation :

| Section 4 — activité par jour | Source | Pipeline | Verdict |
|---|---|---|---|
| 2026-09-07 | 7 122 | 7 122 | ✅ identique |
| 2026-09-08 | 47 486 | 47 486 | ✅ identique |
| 2026-09-09 | 1 873 | 1 873 | ✅ identique |
| 2026-09-10 (jour en cours) | 6 594 | 6 586 | ⚠️ écart de 8 |

**Comment lire ce résultat** — et c'est le point important :

- Les **jours passés correspondent exactement**. C'est la preuve que le calcul est juste.
- Seul le **jour en cours** présente un écart, parce que le simulateur écrit en continu pendant la
  mesure. C'est la **latence du pipeline**, pas une erreur.

### 4d. Distinguer une latence d'un bug

Si tu constates un écart, refais les deux mesures 30 secondes plus tard :

| L'écart… | Conclusion |
|---|---|
| se **réduit** pendant que les volumes augmentent | Latence temps réel — normal, le pipeline rattrape |
| reste **constant ou grandit** | Bug — le pipeline perd ou fausse des données |

Mesure obtenue lors de la validation : écart passé de **11 à 1 ligne** en 30 secondes, pendant que le
volume augmentait de 59 lignes. Le pipeline suit donc la source à environ une ligne près.

---

## Vérification 5 — Le temps réel, en direct

C'est la démonstration la plus convaincante, notamment devant un jury.

**1.** Note le total actuel :

```powershell
podman exec pipeline_temps_reel-postgres-analytics-1 psql -U dprest -d dprest_analytics -c "SELECT SUM(nombre_prestations) FROM kpi_prestations_jour;"
```

**2.** Lance une simulation depuis le dashboard de `simulateur_V5` (**http://localhost:8000**) ou en
ligne de commande :

```powershell
podman exec simulateur_v5-api-1 python run_simulation.py --nombre-passages 20 --vitesse 100
```

**3.** Relance la même requête quelques secondes plus tard.

**Attendu** : le total a augmenté, **sans aucune intervention** — ni script SQL à écrire, ni export,
ni e-mail.

**Ce que ça prouve** : c'est exactement ce que le circuit manuel actuel ne peut pas faire. Une donnée
saisie dans l'application métier se retrouve dans les KPI en quelques secondes.

---

## Vérification 6 — L'idempotence (relancer ne fausse rien)

Règle non négociable de `CLAUDE.md` : relancer un traitement ne doit ni dupliquer ni corrompre.

**1.** Note le nombre de lignes des tables KPI (section 10 du script `controle_analytics.sql`).

**2.** Resoumets le job Flink — il relira **tout** le topic depuis le début :

```powershell
podman exec pipeline_temps_reel-flink-jobmanager-1 ./bin/sql-client.sh -f /tmp/kpi_prestations.sql
```

**3.** Attends 2 minutes, puis recompte.

**Attendu** : le nombre de lignes est **identique** (aux nouvelles données près), et les totaux n'ont
pas doublé.

**Pourquoi ça marche** : les écritures se font en `UPSERT` sur la clé primaire (jour + dimensions).
Réécrire la même clé remplace la ligne au lieu d'en ajouter une.

> Note : chaque soumission crée un **nouveau job** Flink, l'ancien continue de tourner. Pour éviter
> que deux jobs écrivent en parallèle, annuler l'ancien :
> `podman exec pipeline_temps_reel-flink-jobmanager-1 ./bin/flink cancel <JOB_ID>`

---

## Vérification 7 — Consulter les KPI sur la période de son choix

C'est la raison d'être de la base analytique : n'importe quelle période, en SQL standard.

```powershell
podman exec pipeline_temps_reel-postgres-analytics-1 psql -U dprest -d dprest_analytics -c "SELECT DATE_TRUNC('week', jour)::date AS semaine, SUM(nombre_prestations) AS prestations, SUM(montant_depense) AS montant FROM kpi_prestations_jour GROUP BY 1 ORDER BY 1;"
```

Changer `'week'` en `'month'`, ou ajouter un `WHERE jour BETWEEN '2026-09-01' AND '2026-09-15'` :
la réponse est immédiate, sans relire Kafka ni relancer le moindre calcul.

**Ce que ça prouve** : le grain journalier suffit à produire toutes les périodes. C'est ce que fera
Superset à l'étape 6 quand on déplacera le sélecteur de dates.

---

## Que faire si quelque chose ne va pas

| Symptôme | Piste |
|---|---|
| Un conteneur n'est pas `healthy` | `podman logs <nom_du_conteneur> --tail 50` |
| Tâche Debezium `FAILED` | La base source a probablement redémarré après Kafka Connect → relancer la tâche (vérification 2) |
| Job Flink absent de la liste | Il a été perdu au redémarrage : le resoumettre (étape préalable) |
| Job Flink qui échoue en boucle | `podman exec pipeline_temps_reel-flink-jobmanager-1 curl -s "http://localhost:8081/jobs/<JOB_ID>/exceptions?maxExceptions=1"` |
| Tables KPI vides | Vérifier d'abord que le job tourne, puis que les topics Kafka contiennent des messages |
| `localhost` injoignable | VM Podman en mode rootful → récupérer son IP : `podman machine ssh "ip -4 -o addr show eth0"` |
| Machine qui rame | Arrêter les services non essentiels : `podman compose stop akhq` |

---

## En résumé : les trois questions qui comptent

1. **Est-ce que ça tourne ?** → vérifications 1 à 3 (nécessaire, mais très insuffisant)
2. **Est-ce que les chiffres sont justes ?** → vérification 4 (la seule qui prouve quelque chose)
3. **Est-ce vraiment temps réel ?** → vérification 5 (la plus démonstrative)
