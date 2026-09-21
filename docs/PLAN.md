# Plan du projet — Simulateur pipeline temps réel CNAM/DPREST

Suivi des étapes du mémoire (chapitre 6 : simulateur, chapitre 7 : évaluation). Statut mis à jour à
la fin de chaque étape. Le plan détaillé (objectifs, livrables, critères de validation, risques) est
consigné ici section par section au fur et à mesure ; voir aussi `docs/decisions.md` pour les choix
techniques justifiés.

## Contexte

Remplacement du circuit manuel DPREST (e-mail → script SQL Oracle sur MIRKA → export Excel → KPI
calculés à la main) par un pipeline temps réel Kappa : Debezium (CDC) → Kafka + Schema Registry →
Flink → PostgreSQL → Grafana (supervision) + Superset (KPI métier).

**Source des données** : le simulateur de flux existe déjà, dans un dépôt séparé
`simulateur_V5` (alias « ÉCHO »), qui écrit des factures/prestations/ententes préalables dans une
base PostgreSQL dont le schéma reproduit MIRKA. Ce dépôt (`pipeline_temps_reel`) ne recrée rien : il
capture cette base existante et construit le reste du pipeline jusqu'aux dashboards.

**Environnement** : Podman + `podman compose` (voir `docs/decisions.md`), machine Podman WSL
redimensionnée à 6 Go RAM / 4 CPU.

## Statut des étapes

| # | Étape | Statut |
|---|---|---|
| 0 | Vérification de l'environnement et squelette du projet | Terminé |
| 1 | Raccordement à la base source existante (simulateur_V5) | Terminé |
| 2 | Kafka, Schema Registry, capture CDC avec Debezium | Terminé |
| 3 | Jobs Flink (agrégations, fenêtres, contrôles qualité) | Terminé |
| 4 | Schéma analytique PostgreSQL et écriture des KPI | Terminé |
| 5 | Supervision avec Grafana | Terminé |
| 6 | Dashboards métier DPREST avec Superset | Terminé |
| 7 | Sécurité, chiffrement, rôles, traçabilité | Terminé |
| 8 | Scripts d'évaluation et comparaison avec l'existant | À faire |

## Étape 0 — Vérification de l'environnement et squelette du projet

**Objectif** : s'assurer que Podman peut faire tourner les services nécessaires sur cette machine, et
poser la structure de dossiers du dépôt.

**Fait** :
- Machine Podman (`podman-machine-default`, backend WSL) redimensionnée via `.wslconfig`
  (`C:\Users\charles.nguessan\.wslconfig`, 6 Go RAM / 4 CPU / 2 Go swap) — `podman machine set` ne
  supporte pas le redimensionnement direct sur backend WSL, d'où le passage par `.wslconfig` global
  (redémarrage WSL avec `wsl --shutdown` puis `podman machine start`). Vérifié via
  `podman machine ssh "free -h"` : ~5,8 Go RAM et 4 CPU réellement disponibles dans la VM.
- Test de fonctionnement : `podman run --rm postgres:16-alpine postgres --version` → succès
  (téléchargement d'image + exécution de conteneur OK).
- Arborescence créée : `sql/analytics/`, `connectors/`, `flink/`, `monitoring/`, `superset/`,
  `evaluation/`, `tests/`, `docs/`. Pas de `sql/source/` ni `generator/` : la base source et son
  générateur existent déjà dans `simulateur_V5` (voir étape 1), rien à créer ici.
- `.gitignore`, `docs/PLAN.md` (ce fichier), `docs/decisions.md` créés.

- `README.md` du projet (description réelle + commandes de base).
- `docker-compose.yml` squelette (réseau externe vers `simulateur_v5_net`, services ajoutés
  progressivement à partir de l'étape 2).
- Validation finale : `podman compose up -d` / `podman compose ps` / `podman compose down` testés
  avec un service temporaire (`postgres:16-alpine`) — healthcheck `(healthy)` confirmé, réseau créé
  puis supprimé proprement. Service retiré du fichier une fois le test fait.

**Étape 0 terminée.**

## Étape 1 — Raccordement à la base source existante (simulateur_V5)

**Fait** :
- Réseau Podman partagé créé : `podman network create simulateur_v5_net`.
- `simulateur_V5/compose.yaml` modifié : `command: ["postgres", "-c", "wal_level=logical"]` sur le
  service `postgres`, et `networks.default.name: simulateur_v5_net` (`external: true`, le réseau
  ayant été créé manuellement au préalable).
- `simulateur_V5` redémarré (`podman compose down && up -d`) pour appliquer les deux changements.
- Validation (voir `docs/guides/etape1_raccordement_simulateur_v5.md`) :
  - `SHOW wal_level;` → `logical`.
  - Connectivité confirmée : un conteneur externe au projet `simulateur_V5`, lancé sur
    `simulateur_v5_net`, atteint `postgres` par son nom de service et lit `TB_FACTURES`
    (56 488 lignes au moment du test).
- `docs/dictionnaire_donnees.md` rédigé à partir de `simulateur_V5/schema_initial.sql`.

**Point de vigilance noté en cours de route** : sur ce poste, passer un `-c "SELECT ... FROM
\"TB_FACTURES\";"` contenant des guillemets doubles échappés depuis PowerShell vers `podman.exe`
perd les guillemets en route (bug de reconstruction de ligne de commande, pas un problème du
pipeline) — contournement : lancer la même commande depuis Git Bash, ou utiliser le jeton `--%` de
PowerShell, ou `psql` en mode interactif (`-it`, sans `-c`).

**Étape 1 terminée.**

## Étape 2 — Kafka, Schema Registry, capture CDC avec Debezium

**Fait** :
- Services ajoutés à `docker-compose.yml` : `kafka` (KRaft, nœud unique), `schema-registry`,
  `kafka-connect` (image construite avec `debezium-connector-postgresql:2.5.4` + convertisseur Avro),
  `akhq` (interface web, outillage facultatif).
- Connecteur `dprest-postgres-source` enregistré et `RUNNING`, capturant 8 tables (`TB_FACTURES*`,
  `TB_ENTENTES_PREALABLES*`).
- Snapshot initial confirmé (~56 500 lignes sur `TB_FACTURES`) puis test à chaud confirmé : après une
  nouvelle simulation côté `simulateur_V5`, l'offset du topic est passé de 56 533 à 56 972 (+439
  messages) — le CDC capte bien les changements en continu, pas seulement l'état initial.
- Schémas Avro enregistrés dans Schema Registry pour toutes les tables capturées.
- Guides créés : `docs/guides/etape2_kafka_debezium.md`, `docs/guides/demarrage_arret.md`.

**Problèmes rencontrés et corrigés** (détail dans `docs/decisions.md`) :
- `docker-compose.exe` (fournisseur Compose utilisé par `podman compose` sur cette machine) attend un
  fichier nommé `Dockerfile` par défaut → précisé `dockerfile: Containerfile` explicitement.
- `debezium-connector-postgresql:latest` exige Java 17, incompatible avec le Java 11 de l'image de
  base `cp-kafka-connect-base` → épinglé en version `2.5.4`.
- Git Bash traduit automatiquement les chemins Unix (`/tmp/...`) en chemins Windows avant de les
  transmettre à `podman exec`/`podman run`, ce qui cassait les commandes avec un chemin en argument →
  contournement avec `MSYS_NO_PATHCONV=1`.
- VM Podman parfois en mode rootful : `localhost` injoignable depuis Windows pour les ports publiés,
  contournement avec l'IP interne de la VM (`podman machine ssh "ip -4 -o addr show eth0"`).

**Étape 2 terminée.**

## Étape 3 — Jobs Flink (agrégations, fenêtres, contrôles qualité)

**Fait** :
- Services `flink-jobmanager` + `flink-taskmanager` ajoutés (`flink/Containerfile` : image Flink 1.19.1
  + connecteurs Kafka et Avro/Schema Registry).
- Job Flink SQL (`flink/sql/pipeline_kpi_hebdo.sql`) : lecture du flux CDC, agrégat hebdomadaire par
  fenêtre `TUMBLE` de 7 jours (nombre de prestations + montant total par statut de remboursement),
  et contrôle qualité isolant les lignes anormales dans un topic dédié. Les deux écritures partagent
  une seule lecture du topic source (`STATEMENT SET`).
- Sinks en `upsert-kafka` (idempotence : un recalcul de fenêtre met à jour la ligne au lieu d'empiler
  des doublons).

**Validation (le point qui compte)** : l'agrégat produit par Flink correspond **exactement** à une
requête de contrôle indépendante sur la base source, pour la semaine du 2026-09-03 :

| Source | Nombre de prestations | Montant total |
|---|---|---|
| Flink (flux temps réel) | 56 481 | 564 810 000 |
| Requête SQL directe sur `TB_FACTURES_PRESTATIONS` | 56 481 | 564 810 000,00 |

**Problèmes rencontrés et corrigés** (détail et justification dans `docs/decisions.md`) :
1. Mémoire Flink : `jobmanager.memory.process.size` à 512m empêche le démarrage → 1024m minimum.
2. Bug du connecteur `debezium-avro-confluent` de Flink 1.19.x sur l'enveloppe Avro de Debezium
   (`AvroTypeException`, reproduit sur 3 schémas et 2 versions) → contournement : second connecteur
   Debezium en JSON dédié à Flink, le flux Avro/Schema Registry restant la référence architecturale.
3. Sink `kafka` classique incompatible avec un flux de changelog CDC → `upsert-kafka`.
4. **Piège de nommage JSON** : colonne physique nommée `DATE_CREATION_RAW` alors que le champ du
   message s'appelle `DATE_CREATION` → NULL sur 100 % des lignes, sans erreur visible. Détecté par un
   contrôle de cohérence de volume (nombre de rejets qualité == nombre de lignes source). Corrigé :
   colonne physique `DATE_CREATION` + colonne calculée `event_time` portant le watermark.

**Exploration des données faite au passage (préparation de l'étape 4)** :
- `TB_FACTURES_PRESTATIONS` : `STATUT_REMBOURSEMENT` = `couvert` et `STATUT_CODE` = `servie` pour
  **100 %** des lignes, `MOTIF_REJET_CODE` toujours nul ; `TB_FACTURES_REJETS` est vide.
  → **Le KPI « taux de rejet » n'est pas calculable en l'état** : le simulateur ne génère aucun rejet
  de prestation. À traiter à l'étape 4 (activer le module d'anomalies de `simulateur_V5` ou requalifier
  le KPI).
- `TB_FACTURES_STATUTS.STATUT_CODE` : `ouverte` (60 110), `cloturee` (60 002).
- `TB_ENTENTES_PREALABLES.TYPE_DEMANDE_CODE` : `acte` (9 923), `hospitalisation` (2 148).
- `TB_ENTENTES_PREALABLES_STATUTS.STATUT_CODE` : `acceptee` (9 592), `refusee` (1 641),
  `validee_office` (835). → **La famille de KPI « ententes préalables » est, elle, pleinement
  calculable.**

**Point de vigilance** : `simulateur_V5` avait été basculé en PostgreSQL natif Windows entre deux
sessions, ce qui a cassé le CDC (les connecteurs pointent vers le conteneur sur `simulateur_v5_net`).
Remis en conteneur, données intactes (volume `pgdata` préservé). Retenir : le mode conteneur est celui
sur lequel tout le pipeline est câblé.

**Étape 3 terminée.**

## Étape 4 — Schéma analytique PostgreSQL et écriture des KPI

**Fait** :
- `docs/kpi.md` : définitions des 22 KPI retenus (formule, périmètre, granularité, source), avec les
  5 hypothèses assumées et les limites connues. Document de référence pour le mémoire.
- Service `postgres-analytics` (base cible distincte de la source, port 15433 côté Windows) et
  `sql/analytics/001_schema.sql` : 6 tables KPI appliquées automatiquement.
- Connecteur JDBC + pilote PostgreSQL ajoutés à l'image Flink.
- `flink/sql/kpi_prestations.sql` : job d'agrégat **continu** (pas de fenêtre) qui joint les flux CDC
  prestations × factures, agrège au jour et écrit en UPSERT dans trois tables.

**Conception validée avec Mathieu** : grain de base au **jour**, mis à jour en temps réel (la ligne du
jour est réécrite à chaque nouvelle prestation). Semaine, mois et plages libres se dérivent par simple
`GROUP BY DATE_TRUNC(...)` — donc la semaine et le mois en cours sont eux aussi temps réel, et toute
période est consultable instantanément sans relire Kafka. Les KPI réglementaires mensuels des ententes
préalables auront en plus une version certifiée par fenêtre fermée.

**Validation** : totaux comparés à une requête de contrôle indépendante sur la source, pendant que le
simulateur produisait des données. Écart passé de 11 à **1 ligne** en 30 secondes alors que le volume
augmentait de 59 lignes → c'est la latence temps réel du pipeline, pas une erreur de calcul.
Mesure directement réutilisable au chapitre 7 (fraîcheur).

**Problèmes rencontrés et corrigés** :
- Ne reconstruire que l'image du JobManager donne un job qui se soumet puis échoue à l'exécution
  (`ClassNotFoundException` JDBC) : c'est le **TaskManager** qui exécute. Reconstruire les deux.
- `OutOfMemoryError` sur la jointure des deux flux CDC : avec 1024 Mo, le TaskManager n'avait que
  25 Mo de tas applicatif. Corrigé à 1536 Mo **et** `taskmanager.memory.managed.fraction: 0.1` (la
  mémoire managée sert à RocksDB, non utilisé ici) → 614 Mo de tas pour +512 Mo de RAM seulement.
- Debezium sérialise les `DATE` en entier (jours depuis 1970) : conversion vérifiée sur une valeur
  connue avant intégration.
- **2026-09-11 — doublons Debezium** : +47 prestations sur le 10/09, détectées par la comparaison jour
  par jour (le pipeline comptait *plus* que la source). Cause : livraison « au moins une fois » de
  Debezium après un incident. Corrigé par `table.exec.source.cdc-events-duplicate` (dédoublonnage par
  clé primaire). Incident associé : deux jobs simultanés → écritures concurrentes et `OutOfMemoryError`.
  Règle retenue : un seul job par ensemble de tables. Détail dans `docs/decisions.md`.

**Ajouts du 2026-09-11** (validés avec Mathieu) :
- KPI 2 et 4 fusionnés en « nombre de passages » (H6 : un passage = une facture ; un assuré revenu
  compte à chaque venue) → table `kpi_factures_jour`.
- KPI 23 et 24 : top 10 des assurés par prestations servies, global et par centre, ex-aequo inclus
  (H7) → table `kpi_prestations_assure_jour` + vues `v_top10_*`. Le classement se calcule à la lecture
  pour rester valable sur toute période.
- Schéma : `sql/analytics/002_kpi_passages_assures.sql` (créations uniquement). Ajouté au job existant
  plutôt qu'en job séparé, pour partager la lecture et l'état de jointure (contrainte mémoire).
- **Validation** : sources et pipeline identiques sur totaux, activité par jour, passages par jour et
  empreintes des deux classements (sections 11 à 13 des scripts `evaluation/controle_*.sql`).
  Tas du TaskManager : 68 %.

**Idempotence — constatée** : le job a été resoumis quatre fois depuis le début du topic le 2026-09-11 ;
après chaque relecture complète, les tables KPI sont identiques à la source, sans aucune ligne dupliquée
(la clé primaire l'interdit, l'UPSERT réécrit). Limite constatée : l'idempotence protège le rejeu d'**un**
job, pas deux jobs simultanés.

**Étape 4 close (2026-09-11)** :
- Job « ententes préalables » ajouté au job existant (KPI 16-21, agrégat continu par jour et par
  agent). KPI 22 (montant engagé) inclus dans le grain jour.
- KPI mensuel certifié (16-19, 21) implémenté en **vue SQL** dérivée du grain jour plutôt qu'en
  fenêtre Flink fermée — décision et justification dans `docs/decisions.md` (évite de reproduire le
  bug de watermark de l'étape 3, sans bénéfice démontrable avant un premier mois complet de données).
- Idempotence testée explicitement sur les 4 tables continues : job annulé, resoumis, relecture
  complète des topics — lignes et totaux identiques avant/après.
- Anomalie récurrente corrigée en cours de route : les topics des ententes préalables contenaient
  encore l'historique d'avant le reseed du matin (non nettoyés lors du premier incident, qui n'avait
  traité que les topics factures) — même procédure de nettoyage appliquée, avec cette fois les
  connecteurs arrêtés *avant* la suppression des topics (sinon ils les recréent aussitôt).
- Étape 4 entièrement terminée : les deux familles de KPI (prestations et ententes préalables)
  tournent dans un seul job Flink, vérifiées contre la source.

## Étape 5 — Supervision avec Grafana

**Objectif** : donner au SGD une vue sur la santé du pipeline (job Flink, connecteurs Debezium,
fraîcheur des KPI), avec des alertes qui se déclenchent visiblement en cas d'incident.

**Fait (2026-09-11)** :
- Services `prometheus` (v2.53.1) et `grafana` (grafana-oss 11.1.4) ajoutés au `docker-compose.yml`,
  entièrement provisionnés par fichiers (`monitoring/`) — aucun réglage fait à la main dans
  l'interface.
- Flink expose ses métriques nativement : `flink-metrics-prometheus` ajouté à `flink/Containerfile`,
  port 9249 sur JobManager et TaskManager, scrapé par Prometheus (`monitoring/prometheus/prometheus.yml`).
- Trois sources de données Grafana : **Prometheus** (métriques Flink), **PostgreSQL analytique**
  (fraîcheur des KPI en SQL direct) et une source **JSON générique** (plugin communautaire
  `yesoreyeram-infinity-datasource`) pointée sur l'API REST de Kafka Connect — choisi plutôt qu'un
  exportateur JMX supplémentaire pour l'état des connecteurs Debezium (économie de RAM, voir
  `docs/decisions.md`).
- Un tableau de bord (`monitoring/grafana/dashboards/pipeline-overview.json`) : état du job, débit,
  mémoire du TaskManager, fraîcheur des KPI, état des deux connecteurs Debezium, historique.
- **6 alertes** (`monitoring/grafana/provisioning/alerting/alerts.yml`), chacune liée à un incident
  réellement rencontré ce jour : job Flink arrêté, job en boucle de redémarrage, mémoire TaskManager
  > 90 %, fraîcheur des KPI dégradée (> 5 min), connecteur Debezium en échec, base analytique
  injoignable.

**Test de validation effectué** : connecteur Debezium (JSON) mis en pause volontairement
(`PUT .../pause`) → alerte « Connecteur Debezium (JSON) en échec » passée à `firing` en environ
1 minute (délai `for: 1m` de la règle) → connecteur relancé (`PUT .../resume`) → alerte revenue à
`inactive`. Cycle complet déclenchement/résolution confirmé. Ce même geste (couper un composant,
observer la reprise) est directement réutilisable à l'étape 8.

**Problèmes rencontrés et corrigés** :
- Répertoire `monitoring/grafana/dashboards/` inexistant au premier démarrage → conteneur Grafana en
  échec de création (volume introuvable). Corrigé en créant le dossier avant `podman compose up`.
- Panneau « État des connecteurs » vide malgré une requête qui semblait correcte : il manquait
  `"parser": "backend"` dans la requête Infinity — sans ce paramètre, le connecteur ne parse pas la
  réponse JSON en colonnes exploitables. Vérifié requête par requête via l'API `/api/ds/query` avant
  d'écrire le JSON du tableau de bord (même discipline que pour Flink : ne jamais deviner un nom de
  champ ou un paramètre, toujours le tester).
- Pour transformer l'état texte (`RUNNING`/`FAILED`) en valeur numérique alertable, Infinity supporte
  des `computed_columns` avec une syntaxe d'expression conditionnelle (`selector: "etat == \"RUNNING\"
  ? 1 : 0"`) — vérifié en direct avant intégration aux règles d'alerte.

**Simplification assumée** : pas d'alerte automatisée sur la RAM de la VM Podman hôte, malgré la
contrainte connue (~1,4 Go disponibles au moment du déploiement). Ajouter un exportateur dédié
(node_exporter ou équivalent) aurait consommé de la RAM supplémentaire pour surveiller un manque de
RAM — contradictoire sur cette machine. Le contrôle manuel (`podman machine ssh "free -m"`, déjà dans
`docs/guides/demarrage_arret.md`) reste la méthode retenue pour ce point précis.

**Non fait, à noter pour le mémoire** : aucun canal de notification (e-mail, Slack) configuré — les
alertes sont visibles dans l'interface Grafana mais ne sont pas transmises activement. Suffisant pour
la démonstration ; à mentionner comme limite du simulateur face à une exploitation réelle 24/7.

**Étape 5 terminée.**

## Étape 6 — Dashboards métier DPREST avec Superset

**Objectif** : donner à la DPREST une restitution des KPI équivalente — et plus fraîche — que
l'export Excel actuel, via des tableaux de bord navigables, sans écrire de SQL.

**Fait (2026-09-11/12)** :
- Services ajoutés : `superset-db` (métadonnées, base dédiée), `superset-redis` (cache, sans worker
  Celery asynchrone), `superset-init` (migrations + compte admin, tourne une fois), `superset`
  (serveur web). Architecture proche production choisie explicitement (voir `docs/decisions.md`).
- Image dérivée (`superset/Containerfile`) : l'image officielle `apache/superset` ne contient pas de
  pilote PostgreSQL par défaut — `psycopg2-binary` ajouté.
- Secrets (clé secrète, mots de passe) dans `.env`, non versionné — seul service du dépôt à appliquer
  cette règle stricte de `CLAUDE.md`, car Superset gère de vrais comptes utilisateurs.
- Connexion à `postgres-analytics` créée et testée (11 tables/vues détectées).
- **10 jeux de données** : les 7 tables KPI + les 3 vues de classement/certification.
- **14 graphiques**, tous vérifiés par requête directe avant intégration :
  grands nombres (prestations totales, montant facturé, taux de couverture CMU, montant engagé EP),
  camemberts (type d'acte, régime, statut EP, type de demande EP), tableaux (top centres, top
  praticiens, passages par jour, top assurés, activité par agent, KPI mensuel certifié).
- **2 tableaux de bord** assemblés par API (position_json construit programmatiquement, pas de clic
  dans l'interface) : « DPREST - Prestations et facturation » (9 graphiques, famille A) et
  « DPREST - Ententes préalables » (5 graphiques, famille B) — reprennent le découpage de `kpi.md`.

**Validation** : plusieurs graphiques recoupés avec le SQL de contrôle — tous identiques (ex. 4894
prestations totales, taux de couverture 82,1 %, répartition EP par statut incluant un cas réel
« sans_reponse » qui confirme l'hypothèse H4 en conditions réelles, montant engagé 17 169 685 F).

**Problèmes rencontrés et corrigés** :
- Image `apache/superset` sans `psycopg2` → `ModuleNotFoundError` dès `superset-init`, y compris pour
  la propre base de métadonnées de Superset. Corrigé par une image dérivée.
- Automatisation par API (construction des tableaux de bord) : un script Python a échoué avec « The
  CSRF session token is missing » — le fichier de cookies curl préfixe les cookies `HttpOnly` par
  `#HttpOnly_`, faisant croire à un commentaire et les faisant ignorer par un filtre naïf sur `#`.
- Machine relancée en RAM 7 Go (au lieu de 6) pour cette étape, décision prise avec Mathieu après
  vérification de la RAM disponible côté Windows (voir `docs/decisions.md`) : l'architecture proche
  production (base + cache dédiés) demandait plus de marge que l'option SQLite envisagée au départ.
- Grafana redémarré `unhealthy` une fois après un arrêt/relance complet de la machine : échec réseau
  transitoire au (re)téléchargement du plugin Infinity, pas de volume persistant — symptôme déjà
  documenté à l'étape 5, résolu par un simple redémarrage du conteneur.

**Non fait, à noter pour le mémoire** : mise en page des tableaux de bord fonctionnelle mais générée
par script plutôt qu'ajustée visuellement (pas de fignolage manuel des couleurs/tailles) ; pas de
export PDF/planifié (aurait nécessité un worker Celery, écarté pour la RAM) ; pas encore de contrôle
d'accès par rôle pour la DPREST (comptes utilisateurs distincts, permissions par dashboard) — prévu à
l'étape 7.

**Étape 6 terminée.**

## Étape 7 — Sécurité, chiffrement, rôles, traçabilité

**Objectif** : répondre à l'exigence légale (loi n°2013-450, contrôle ARTCI) : chiffrement des
échanges, authentification par composant, contrôle d'accès par rôle, traçabilité.

**Périmètre validé avec Mathieu avant de commencer** : secrets hors du code, comptes par service à
moindre privilège, rôle DPREST en lecture seule dans Superset, traçabilité, et HTTPS **limité aux
interfaces web** (Grafana, Superset) — le reste du pipeline reste en clair sur le réseau Podman
interne, qui ne sort jamais de cette machine. Limite assumée et documentée dans `docs/decisions.md`.

**Fait (2026-09-12)**, 5 points :

1. **Secrets hors du code** : tous les mots de passe en clair de `docker-compose.yml` migrés vers
   `.env` (obligatoire, plus de valeur par défaut publique) ; le connecteur Debezium référence son
   mot de passe via le mécanisme FileConfigProvider natif de Kafka Connect
   (`connectors/secrets/`, non versionné) ; le job Flink reçoit son mot de passe injecté au moment
   de la soumission (jamais écrit dans le `.sql` versionné) ; Grafana utilise la substitution de
   variable d'environnement officielle (`$__env{...}`), revérifiée fonctionnelle après un doute
   injustifié à l'étape 5.
2. **Comptes par service, moindre privilège** : deux nouveaux rôles PostgreSQL en lecture seule
   (`dprest_lecture` sur la base analytique, `monitoring_ro` avec le rôle intégré `pg_monitor` sur la
   base source) — testés : lecture autorisée, écriture/accès aux données métier refusés.
3. **Rôle DPREST en lecture seule dans Superset** : compte `dprest` (rôle Gamma), accès accordé aux
   10 jeux de données KPI, les 2 tableaux de bord publiés. Testé : voit exactement ces 2 tableaux,
   écriture refusée.
4. **Traçabilité** : journal d'accès Superset déjà actif nativement (vérifié) ; connexions et
   déconnexions journalisées sur la base analytique, avec l'identité de l'utilisateur.
5. **HTTPS sur Grafana et Superset** : un reverse proxy nginx (léger) termine le TLS pour les deux,
   certificat auto-signé régénérable. Ports en clair retirés de la publication vers l'hôte.

**Validation finale** : les 6 points testés individuellement puis le pipeline entier revérifié de bout
en bout après toutes les rotations de mots de passe et recréations de conteneurs — source et cible
toujours identiques, job Flink `RUNNING` sans exception.

**Problèmes rencontrés et corrigés** (détail complet dans `docs/decisions.md`) :
- Image nginx figée jamais récupérée (pull bloqué côté client Podman/Windows) → basculé sur une
  version déjà en cache.
- Le reverse proxy tombait en erreur après toute recréation de Grafana ou Superset (résolution DNS
  figée au démarrage) → résolveur DNS dynamique, avec l'adresse propre au réseau Podman (pas la
  convention Docker `127.0.0.11`, qui ne fonctionne pas ici). Validé : reconnexion automatique après
  recréation, sans intervention.
- Tableaux de bord Superset invisibles pour le compte DPREST malgré les permissions accordées :
  ils étaient en brouillon, pas publiés.

**Découverte annexe, corrigée au passage** : `docs/` était dans `.gitignore` depuis le début du
projet — toute la documentation du mémoire n'avait jamais été commitée. Retiré immédiatement.

**Non fait, à noter pour le mémoire** :
- Compte Debezium toujours partagé avec l'application source (`echo`), pas de compte CDC dédié —
  changer le propriétaire de la publication de réplication sur un connecteur déjà actif est jugé trop
  risqué pour être tenté sans fenêtre de test dédiée.
- Pas de TLS entre les services internes (Kafka, Flink, PostgreSQL, Kafka Connect) — limite assumée,
  justifiée par le fait que tout tourne sur une seule machine, dans un réseau qui ne sort jamais à
  l'extérieur. À revoir pour une vraie mise en production distribuée.
- Pas d'agrégation centralisée des journaux.

**Étape 7 terminée.**

**Complément (2026-09-12/13)** — supervision étendue, une fois les 7 étapes en place : taille des
bases PostgreSQL, connexions actives par compte, activité Superset (audit visible dans Grafana, pas
seulement en ligne de commande), et métriques Kafka (débit du broker, taille des topics) via l'agent
JMX intégré au broker — aucun service supplémentaire. Découverte au passage : Kafka n'avait jamais eu
de stockage persistant (`/tmp/kraft-combined-logs` par défaut) ; corrigé avec un volume dédié, au prix
d'une remise à zéro complète de Kafka (procédure déjà rodée). Détail complet et pièges rencontrés dans
`docs/decisions.md`.
