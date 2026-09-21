# Guide — Tout démarrer, tout arrêter, tout consulter

Le guide de référence à ouvrir **après chaque extinction du PC**. Il couvre l'état actuel du projet :
étapes 0 à 4 (CDC Debezium → Kafka → Flink → PostgreSQL analytique).

Toutes les commandes sont pour **PowerShell**. Si tu utilises Git Bash, voir la section « Pièges ».

---

# 1. Démarrage après extinction du PC

Rien ne redémarre tout seul : la VM Podman est éteinte, donc les conteneurs aussi. Trois choses à
relancer, **dans cet ordre** :

1. la VM Podman,
2. les conteneurs (simulateur d'abord, pipeline ensuite),
3. le job Flink — qui, lui, ne survit jamais à un redémarrage.

## Option A — le script (recommandé)

```powershell
cd "C:\Users\charles.nguessan\Documents\pipeline_temps_reel"
.\scripts\start-stack.ps1
```

Le script fait les trois étapes, attend que les services soient prêts, resoumet le job Flink et
affiche à la fin **l'IP de la VM et les URL des interfaces web**. Compter 2 à 3 minutes.

> Si Windows refuse d'exécuter le script (« l'exécution de scripts est désactivée ») :
> `powershell -ExecutionPolicy Bypass -File .\scripts\start-stack.ps1`

## Option B — à la main, étape par étape

### 1. La VM Podman

```powershell
podman machine start
```

### 2. La base source (simulateur_V5)

Toujours **avant** le pipeline : Kafka Connect a besoin de sa base au démarrage, sinon ses tâches
partent en échec.

```powershell
cd "C:\Users\charles.nguessan\OneDrive - IPSCNAM\Documents\simulateur_V5"
podman compose start
```

### 3. Le pipeline

```powershell
cd "C:\Users\charles.nguessan\Documents\pipeline_temps_reel"
podman compose start
podman ps --format "{{.Names}}: {{.Status}}"
```

Attendre que `kafka-connect`, `flink-jobmanager` et `postgres-analytics` soient `(healthy)` — 30 à
60 secondes, davantage si la machine est chargée.

> **`start` et non `up -d`** : les conteneurs existent déjà, on les redémarre sans les recréer, donc
> les données de Kafka et de la base analytique sont conservées. Si un conteneur a été **supprimé**
> (et pas seulement arrêté), `start` échoue avec « missing dependency » → utiliser `up -d`.

### 4. Vérifier le connecteur Debezium

> Un seul connecteur depuis le 2026-09-11 : la version Avro (`dprest-postgres-source`) a été retirée,
> non utilisée par Flink (bug de désérialisation documenté dans `docs/decisions.md`). C'est la version
> JSON ci-dessous qui alimente tout le pipeline.

```powershell
podman exec pipeline_temps_reel-kafka-connect-1 curl -s http://localhost:8083/connectors/dprest-postgres-source-json/status
```

Il faut `"state":"RUNNING"` **deux fois** : pour le connecteur *et* pour sa tâche. Une tâche `FAILED`
après un redémarrage est le cas le plus courant (la base source est repartie après Kafka Connect) :

```powershell
podman exec pipeline_temps_reel-kafka-connect-1 curl -s -X POST http://localhost:8083/connectors/dprest-postgres-source-json/tasks/0/restart
```

### 5. Resoumettre le job Flink — OBLIGATOIRE à chaque fois

Le cluster Flink n'a pas de stockage haute disponibilité : **le job est perdu dès que le JobManager
s'arrête**. C'est l'oubli le plus fréquent — tout paraît vert, et les KPI ne bougent plus.

```powershell
cd "C:\Users\charles.nguessan\Documents\pipeline_temps_reel"
podman cp flink/sql/kpi_prestations.sql pipeline_temps_reel-flink-jobmanager-1:/tmp/kpi_prestations.sql
podman exec pipeline_temps_reel-flink-jobmanager-1 ./bin/sql-client.sh -f /tmp/kpi_prestations.sql
```

La commande affiche un `Job ID` puis rend la main : normal, le job continue sur le cluster. Vérifier
qu'il **tient dans la durée** — une erreur de configuration fait échouer un job dans les 30 à 60
premières secondes, donc un job qu'on vient de soumettre ne prouve rien :

```powershell
podman exec pipeline_temps_reel-flink-jobmanager-1 curl -s http://localhost:8081/jobs
```

→ attendu : `{"jobs":[{"id":"...","status":"RUNNING"}]}`

> Le job relit le topic depuis le début. Les écritures se faisant en `UPSERT` sur la clé primaire
> (jour + dimensions), les lignes sont **écrasées et non dupliquées** : relancer ne fausse rien.
> C'est l'idempotence exigée par `CLAUDE.md`.

---

# 2. Les interfaces web

## D'abord : récupérer l'IP de la VM

Sur ce poste, la VM Podman tourne en mode *rootful* : **`localhost` ne fonctionne pas** depuis
Windows. Il faut passer par l'IP interne de la VM, qui **change à chaque `podman machine start`** :

```powershell
podman machine ssh "ip -4 -o addr show eth0"
```

→ actuellement `172.31.104.114`. Le script `start-stack.ps1` l'affiche automatiquement à la fin.

## Les adresses

| Interface | URL | Ce qu'on y fait |
|---|---|---|
| **Dashboard simulateur_V5** | `http://<IP_VM>:8000` | Lancer des simulations, suivre la génération de données |
| **Flink** | `http://<IP_VM>:8082` | Voir les jobs, leur état, le nombre d'enregistrements traités, les métriques |
| **Grafana** (étape 5/7) | `https://<IP_VM>:3443` | Tableau de bord de supervision, alertes — identifiants `admin` / voir `.env` (`GRAFANA_ADMIN_PASSWORD`) |
| **Prometheus** (étape 5) | `http://<IP_VM>:9091` | Métriques brutes de Flink, état des cibles scrapées |
| **Superset** (étape 6/7) | `https://<IP_VM>:8443` | Tableaux de bord KPI pour la DPREST — compte admin dans `.env` ; compte `dprest` en lecture seule pour un usage DPREST réel |

> **Depuis l'étape 7, Grafana et Superset sont en HTTPS** (certificat auto-signé, voir
> `docs/guides/etape7_securite.md`) — le navigateur affiche un avertissement de sécurité au premier
> accès, c'est normal, cliquer sur « Avancé » / « Continuer vers le site ». Les anciennes adresses
> `http://<IP_VM>:3000` et `http://<IP_VM>:8088` répondent toujours en interne mais ne sont plus
> publiées à l'extérieur du réseau Podman.
| **AKHQ** (Kafka) | `http://<IP_VM>:8085` | Topics, contenu des messages, schémas Avro, état des connecteurs Debezium |
| **pgAdmin** (base KPI) | application Windows | Voir les tables KPI (paramètres dans `consulter_les_donnees.md`) |
| Kafka Connect (API) | `http://<IP_VM>:8083` | API REST brute — utilisée par les commandes ci-dessus |
| Schema Registry (API) | `http://<IP_VM>:8081` | Schémas Avro enregistrés |

> **AKHQ et Grafana font largement double emploi** (tous deux montrent l'état des connecteurs
> Debezium) : sur cette machine contrainte en RAM, ne pas faire tourner les deux en permanence.
> `podman compose stop akhq` libère ~400 Mo si Grafana est démarré ; à l'inverse,
> `podman compose stop prometheus grafana` libère de la RAM si on n'a besoin que d'AKHQ.

---

# 3. « Est-ce que je peux tout lancer depuis les interfaces web ? »

Réponse courte : **non, pas le démarrage — mais oui pour une bonne partie du pilotage quotidien.**

## Ce qui est impossible depuis une UI

**Démarrer la VM Podman et les conteneurs.** C'est le problème de l'œuf et de la poule : les
interfaces web *sont* servies par les conteneurs. Tant qu'ils ne tournent pas, il n'y a aucune page à
ouvrir. Le démarrage passe donc forcément par PowerShell (ou le script).

**Soumettre le job Flink SQL.** L'interface Flink possède bien un bouton *Submit New Job*, mais il
n'accepte que des **fichiers JAR** (jobs Java/Scala compilés). Notre job est un script SQL exécuté par
le client SQL — il n'y a pas de moyen de le soumettre depuis le navigateur. C'est une limite connue de
Flink, pas un défaut de notre configuration.

## Ce qui est possible depuis une UI

| Action | Où | Comment |
|---|---|---|
| Lancer une simulation (générer des données) | Dashboard simulateur_V5 `:8000` | C'est sa fonction principale |
| Arrêter / annuler un job Flink | UI Flink `:8082` | Ouvrir le job → bouton **Cancel Job** |
| Surveiller le débit, les checkpoints, la contre-pression | UI Flink `:8082` | Onglets du job |
| Redémarrer / mettre en pause un connecteur Debezium | AKHQ `:8085` | Section **Connects** → le connecteur → *Restart* |
| Lire les messages d'un topic Kafka | AKHQ `:8085` | Topic → onglet **Data** |
| Consulter les schémas Avro | AKHQ `:8085` | Section **Schema Registry** |
| Interroger les KPI en SQL | pgAdmin | Voir `docs/guides/consulter_les_donnees.md` |

Donc : **PowerShell pour allumer et pour soumettre le job Flink ; les UI pour tout le reste** — et
c'est d'ailleurs cette répartition qu'on retrouvera en production, où le démarrage est géré par le
système (systemd / orchestrateur) et l'exploitation par les interfaces.

> En production, le job Flink serait resoumis automatiquement : Flink sait le faire avec le mode
> *haute disponibilité* (état du job stocké dans un système de fichiers persistant). On ne l'active
> pas ici parce que ça suppose un stockage partagé et ~200 Mo de RAM de plus — point à mentionner en
> soutenance comme différence assumée entre le simulateur et une vraie production.

---

# 4. Arrêt (fin de session)

```powershell
cd "C:\Users\charles.nguessan\Documents\pipeline_temps_reel"
podman compose stop

cd "C:\Users\charles.nguessan\OneDrive - IPSCNAM\Documents\simulateur_V5"
podman compose stop

podman machine stop
```

Ou, plus court :

```powershell
cd "C:\Users\charles.nguessan\Documents\pipeline_temps_reel"
.\scripts\stop-stack.ps1
```

`stop` arrête les conteneurs **sans les supprimer** : les données restent intactes. N'utiliser `down`
que pour repartir volontairement de zéro — ça supprime les conteneurs, et `up -d` sera alors
nécessaire au prochain démarrage.

Si tu éteins simplement le PC sans rien arrêter, ce n'est pas grave : les volumes sont sur disque. Il
faudra juste reprendre la section 1 à la prochaine session, et probablement redémarrer les tâches
Debezium (section 1.4).

---

# 5. Pièges connus

| Symptôme | Cause / solution |
|---|---|
| `localhost` injoignable dans le navigateur | VM en mode rootful → utiliser l'IP de la VM (section 2) |
| L'IP de la VM ne répond plus | Elle a changé au dernier `podman machine start` → la relire |
| Tâche Debezium `FAILED` | La base source a redémarré après Kafka Connect → relancer la tâche (section 1.4) |
| Les KPI ne bougent plus alors que tout est vert | Le job Flink n'a pas été resoumis (section 1.5) |
| `start` échoue : « missing dependency » | Le conteneur a été supprimé → `podman compose up -d` |
| Machine qui rame | `podman compose stop akhq` libère ~400 Mo |
| Deux jobs Flink dans la liste | Une double soumission → annuler l'ancien : `podman exec pipeline_temps_reel-flink-jobmanager-1 ./bin/flink cancel <JOB_ID>` |

## Git Bash et les chemins

Depuis **Git Bash** (pas PowerShell), tout chemin Unix passé à `podman exec` / `podman cp` est
traduit en chemin Windows et casse la commande. Préfixer par `MSYS_NO_PATHCONV=1` :

```bash
MSYS_NO_PATHCONV=1 podman exec pipeline_temps_reel-flink-jobmanager-1 ./bin/sql-client.sh -f /tmp/kpi_prestations.sql
```

Depuis PowerShell, ce préfixe est inutile.

---

# 6. Contrôle rapide « est-ce que ça marche vraiment ? »

```powershell
podman exec pipeline_temps_reel-postgres-analytics-1 psql -U dprest -d dprest_analytics -c "SELECT MAX(jour), SUM(nombre_prestations) FROM kpi_prestations_jour;"
```

Relancer la même commande 30 secondes plus tard pendant qu'une simulation tourne : le total doit avoir
augmenté, **sans aucune intervention**. C'est la démonstration du temps réel.

Pour la vérification complète — celle qui prouve que les **chiffres sont justes** et pas seulement que
les voyants sont verts — voir `docs/guides/verification.md`.
