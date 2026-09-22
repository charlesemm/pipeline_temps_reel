# Décisions techniques

Journal des choix techniques du projet et de leur justification, pour la rédaction du mémoire.

## 2026-09-10 — Podman plutôt que Docker

**Décision** : l'environnement de référence du projet est Podman (`podman compose`), pas Docker
Desktop, même si Docker Desktop reste installé sur la machine de développement.

**Pourquoi** :
- Le serveur de production visé par la CNAM pour un déploiement réel utilise Podman. Développer
  directement dessus évite de découvrir tardivement des différences de comportement (rootless par
  défaut, absence de daemon central, gestion des volumes/réseaux, labels SELinux) au moment de
  porter le projet vers un environnement proche de la prod.
- Le fichier `docker-compose.yml` reste au format **Compose spec**, agnostique du moteur : il
  s'exécute aussi bien avec `podman compose` qu'avec `docker compose`, donc ce choix ne coûte rien en
  portabilité.
- Cohérent avec l'exigence de `CLAUDE.md` (« outils open source uniquement, pas de licence payante ») :
  Podman est entièrement open source, alors que Docker Desktop impose une licence payante pour un
  usage en organisation au-delà d'une petite structure.

## 2026-09-10 — Redimensionnement de la machine Podman via `.wslconfig`

**Décision** : la RAM/CPU allouée à la VM Podman (backend WSL) est pilotée par
`C:\Users\<utilisateur>\.wslconfig` (global à WSL2), pas par `podman machine set`.

**Pourquoi** : `podman machine set --memory` / `--cpus` renvoie explicitement
`changing memory/CPUs not supported for WSL machines` — sur ce backend, la VM Podman partage
l'infrastructure WSL2 standard, dont les ressources se configurent au niveau de Windows
(`.wslconfig`), pas au niveau de Podman lui-même. Un redémarrage de WSL (`wsl --shutdown`) est requis
pour appliquer le changement. Valeur retenue : 6 Go RAM / 4 CPU / 2 Go swap, un compromis validé avec
Mathieu compte tenu des 16 Go de RAM totaux de la machine et des autres usages du poste.

## 2026-09-10 — Pas de schéma source ni de générateur à construire dans ce dépôt

**Décision** : `pipeline_temps_reel` ne contient pas de dossier `sql/source/` ni `generator/`.

**Pourquoi** : la base source (PostgreSQL, schéma reproduisant MIRKA) et le générateur de flux
existent déjà dans un projet séparé, `simulateur_V5`. Le pipeline commence directement au CDC
(Debezium) sur cette base existante. Voir `docs/PLAN.md` étape 1 pour le détail du raccordement.

## 2026-09-10 — Kafka en KRaft (pas de Zookeeper)

**Décision** : `apache/kafka:3.7.0` en mode KRaft, nœud unique combinant les rôles broker et
contrôleur (`KAFKA_PROCESS_ROLES: broker,controller`).

**Pourquoi** : Zookeeper est déprécié dans l'écosystème Kafka (retiré par défaut à partir de Kafka 4)
et ajoute un conteneur supplémentaire (donc de la RAM) sans bénéfice pour un cluster à un seul nœud.
KRaft est le mode recommandé depuis Kafka 3.3+ pour les nouveaux déploiements. Facteurs de réplication
forcés à 1 partout (topics internes inclus) : avec un seul broker, une valeur plus élevée empêcherait
Kafka de démarrer.

## 2026-09-10 — Schema Registry et Kafka Connect : images Confluent

**Décision** : `confluentinc/cp-schema-registry:7.6.1` pour le Schema Registry, et une image Kafka
Connect construite sur `confluentinc/cp-kafka-connect-base:7.6.1` (voir `kafka-connect/Containerfile`)
avec le connecteur Debezium PostgreSQL et le convertisseur Avro installés via `confluent-hub`.

**Pourquoi / limite à documenter** : il n'existe pas d'implémentation « Apache » du Schema Registry —
c'est un composant propre à l'écosystème Confluent. Les images `cp-*` sont sous **Confluent Community
License** : gratuites et à code source disponible, mais pas reconnues OSI comme « open source » au
sens strict (restriction : ne pas les revendre en SaaS concurrent de Confluent). Ça reste cohérent
avec l'esprit de la contrainte de `CLAUDE.md` (« pas de licence payante ») mais mérite d'être signalé
tel quel dans le mémoire plutôt que présenté comme purement open source. Alternative 100 % Apache 2.0
si cette nuance doit être évitée : **Karapace** (Aiven), compatible avec l'API du Schema Registry
Confluent — non retenue ici pour rester sur la documentation/API la plus répandue et la plus simple à
justifier techniquement dans un mémoire, mais à reconsidérer si la contrainte de licence est stricte.

## 2026-09-10 — CDC via `pgoutput` (pas de plugin de décodage logique tiers)

**Décision** : `plugin.name: pgoutput` dans la config du connecteur Debezium.

**Pourquoi** : `pgoutput` est le plugin de décodage logique **natif** de PostgreSQL (10+), utilisé par
la réplication logique standard — pas besoin d'installer une extension serveur tierce (`wal2json`,
`decoderbufs`) sur l'image `postgres:16-alpine` de `simulateur_V5`, ce qui aurait demandé de modifier
son `Containerfile`. `publication.autocreate.mode: filtered` : Debezium crée lui-même la publication
PostgreSQL, limitée aux tables listées dans `table.include.list`.

## 2026-09-10 — Connecteur Debezium PostgreSQL épinglé en version 2.5.4

**Décision** : `kafka-connect/Containerfile` installe explicitement
`debezium/debezium-connector-postgresql:2.5.4` (pas `:latest`).

**Pourquoi** : à partir de la version 3.0.x, le connecteur Debezium exige Java 17, alors que l'image
de base `confluentinc/cp-kafka-connect-base:7.6.1` embarque Java 11. Avec `:latest`, le plugin
échoue silencieusement au chargement (`UnsupportedClassVersionError` dans les logs du conteneur,
mais aucune erreur au moment du `build`) et n'apparaît pas dans `GET /connector-plugins` — piège
découvert en pratique lors du premier démarrage de l'étape 2. `2.5.4` est la dernière version encore
publiée sur Confluent Hub qui cible Java 11, compatible avec l'image de base choisie.

## 2026-09-10 — Flink SQL (pas PyFlink) pour le premier job

**Décision** : le job de l'étape 3 est écrit en Flink SQL (`flink/sql/pipeline_kpi_hebdo.sql`),
soumis via `sql-client.sh`, plutôt qu'en PyFlink (API Python/DataStream).

**Pourquoi** : les traitements identifiés (fenêtrage temporel, agrégations, filtrage pour le contrôle
qualité) s'expriment naturellement en SQL déclaratif, sans logique impérative complexe qui
justifierait PyFlink. Moins de code, plus lisible pour la revue et la rédaction du mémoire. Les
connecteurs source/format sont ceux d'Apache Flink lui-même (`flink-sql-connector-kafka`,
`flink-sql-avro-confluent-registry`, publiés sur Maven Central, licence Apache 2.0), pas de nuance de
licence comme pour Schema Registry/Kafka Connect. Réévaluer PyFlink si un job futur a besoin d'une
logique impossible à exprimer proprement en SQL (ex. UDF complexe avec état personnalisé).

**Format `debezium-avro-confluent`** : déballe automatiquement l'enveloppe CDC Debezium
(`before`/`after`/`op`) pour retrouver la table telle qu'elle existe côté source, avec les
UPDATE/DELETE traduits en évènements de changelog Flink cohérents (pas de double comptage sur les
lignes mises à jour) — plutôt que de parser manuellement l'enveloppe en SQL.

**Sink `upsert-kafka` pour l'agrégat** : chaque recalcul de fenêtre met à jour la ligne existante
(clé = semaine + statut) au lieu d'empiler des doublons — idempotence par construction, cohérent avec
la règle de `CLAUDE.md` (« relancer deux fois ne doit ni dupliquer ni corrompre »).

**Mémoire process.size à 1024m minimum (JobManager et TaskManager)** : essayé d'abord à 512m/768m
pour rester léger, mais le JobManager refuse de démarrer en dessous d'un certain seuil
(`IllegalConfigurationException: Total Flink Memory is less than the configured Off-heap Memory`) —
les frais fixes de Flink (overhead JVM, mémoire off-heap, métaspace) dépassent déjà ce total à 512m.
1024m est le minimum pratique constaté qui démarre proprement.

## 2026-09-10 — Second connecteur Debezium en JSON, dédié à Flink (contournement d'un bug Avro)

**Décision** : ajout d'un deuxième connecteur Debezium (`connectors/debezium-postgres-json.json`,
`topic.prefix: dprest-json`) qui capture les mêmes tables, mais avec `JsonConverter` (JSON, sans
Schema Registry) au lieu de l'Avro Confluent. Flink lit désormais ces topics JSON
(`format = 'debezium-json'`) plutôt que les topics Avro de l'étape 2.

**Pourquoi (bug rencontré, pas une préférence)** : le connecteur Flink `debezium-avro-confluent`
(testé en versions `1.19.1`, `1.19.3`, puis `2.2.1` — cette dernière incompatible avec le runtime
Flink 1.19, `ClassNotFoundException`) échoue systématiquement à désérialiser les messages Avro de
Debezium avec exactement la même erreur (`AvroTypeException: Found ...Value, expecting union`),
quel que soit le schéma de table déclaré côté Flink (essayé : colonnes réduites, colonnes complètes
alignées champ à champ sur le schéma réel). La cause identifiée : l'enveloppe Avro de Debezium
réutilise le type nommé `Value` par référence (pas par redéfinition complète) entre les champs
`before` et `after` — un schéma Avro valide, mais que le convertisseur de schéma de Flink 1.19.x pour
ce format ne reconstruit pas correctement côté lecture. C'est un bug/une limite documentée de
l'écosystème Flink-Debezium-Avro à cette version, pas une erreur de configuration de notre part.

**Compromis assumé** : les topics `dprest-json.*` n'ont pas de contrat de schéma centralisé dans
Schema Registry — le schéma Debezium reste implicite, embarqué dans la structure de chaque message
JSON (pratique standard de Debezium en mode JSON). Les topics `dprest.*` (Avro, étape 2) restent la
référence pour l'architecture décrite dans `CLAUDE.md` (contrat de schéma via Schema Registry) et
pour tout consommateur externe à Flink ; ils continuent de tourner sans changement. Impact : deux
slots de réplication logique PostgreSQL actifs en parallèle sur `simulateur_V5` (charge CDC doublée),
acceptable vu la volumétrie du simulateur.

**Non retenu, écarté après tentative concrète** : persister sur Avro en cherchant d'autres versions de
connecteur — le bug était identique sur 3 configurations distinctes de schéma, signe d'un problème
structurel plutôt que d'un réglage à ajuster.

**Point ouvert pour le mémoire** : signaler cette limite technique (chapitre 6 ou 7) comme un
compromis d'outillage constaté en pratique, avec la piste de résolution possible (Flink 2.x complet +
connecteur 2.x, non testé ici faute de temps/ressources).

## 2026-09-10 — Piège de nommage : le format JSON de Flink mappe les colonnes par NOM

**Décision / règle à retenir** : dans un `CREATE TABLE` Flink lisant du JSON (ou de l'Avro), le nom de
chaque colonne **physique** doit correspondre exactement au nom du champ dans le message. Une colonne
calculée (`AS ...`) doit porter un nom *différent* de toute colonne physique.

**Pourquoi (erreur commise puis corrigée)** : j'avais nommé la colonne physique `DATE_CREATION_RAW`
pour libérer le nom `DATE_CREATION` au profit de la colonne calculée en TIMESTAMP. Or aucun champ
`DATE_CREATION_RAW` n'existe dans le message Debezium : Flink lisait donc `NULL` pour **100 % des
lignes**, sans aucune erreur visible. Conséquences en cascade, toutes trompeuses :
- toutes les lignes partaient dans le topic de contrôle qualité en `date_creation_manquante`
  (60 070 messages, soit exactement le volume du topic source — c'est ce chiffre identique qui a mis
  la puce à l'oreille) ;
- aucune ligne ne passait le filtre `IS NOT NULL`, donc le fenêtrage ne recevait rien, le watermark
  restait bloqué à `Long.MIN_VALUE` et la fenêtre hebdomadaire ne se fermait jamais → topic KPI vide.

**Fausse piste à corriger dans les notes précédentes** : j'en avais d'abord conclu (à tort) que
`simulateur_V5` produisait de vraies lignes avec `DATE_CREATION` nul — un « défaut de qualité des
données à la source ». C'était faux : mon propre script de diagnostic reproduisait la même erreur de
nommage, ce qui a confirmé une conclusion erronée. **Aucune anomalie de ce type n'est établie côté
source.** Correction retenue : colonne physique `DATE_CREATION` (STRING, nom exact du champ) +
colonne calculée `event_time` (TIMESTAMP) portant le watermark.

**Leçon méthodologique (utile au mémoire)** : un pipeline peut être « vert » de bout en bout
(connecteurs `RUNNING`, job Flink `RUNNING`, messages qui sortent) tout en produisant un résultat
entièrement faux. Ce sont les **contrôles de cohérence sur les volumes** (ici : nombre de rejets
qualité == nombre de lignes source) qui révèlent le problème, pas les statuts des composants — ce qui
justifie précisément les contrôles qualité et la supervision prévus dans l'architecture.

**Formule du premier agrégat marquée hypothèse provisoire** : fenêtrée sur `DATE_CREATION` (date
d'écriture en base) plutôt que `FACTURE_DATE_SOINS` (date de réalisation des soins, sur `TB_FACTURES`,
pas jointe ici) — but de l'étape 3 : prouver que le mécanisme Flink fonctionne de bout en bout, pas
livrer la définition finale du KPI. La formule définitive sera figée à l'étape 4 dans `docs/kpi.md`,
avec validation de Mathieu.

## 2026-09-11 — Dédoublonnage des événements CDC dans Flink (`cdc-events-duplicate`)

**Décision** : le job `kpi_prestations.sql` active
`SET 'table.exec.source.cdc-events-duplicate' = 'true'`. Flink insère alors, derrière chaque source, un
opérateur `ChangelogNormalize` qui dédoublonne les événements par la `PRIMARY KEY` déclarée (déjà
présente sur `prestations_src` et `factures_src`).

**Pourquoi (anomalie constatée)** : Debezium garantit une livraison **« au moins une fois »**
(*at-least-once*), pas « exactement une fois ». Le 2026-09-10 à 16:55:39, la tâche du connecteur JSON a
subi un incident de lecture (`failed to poll records`) ; en reprenant depuis le dernier offset validé,
elle a **réémis 48 événements déjà publiés** (même contenu, même LSN source, réémis 5 minutes plus
tard). Sans dédoublonnage, Flink les comptait deux fois : +47 prestations sur le 10/09.

**Comment ça a été détecté** : ni par un statut (tout était `RUNNING`), ni par un volume anormal, mais
par la comparaison jour par jour avec la source (`evaluation/controle_source.sql`) : le pipeline
comptait **plus** que la source. Une latence produit toujours l'inverse — un écart négatif signale
donc forcément une erreur. Le topic contenait 63 579 événements pour 63 531 clés distinctes, toutes
présentes en source : aucune suppression, uniquement des doublons.

**Coût** : l'opérateur garde en état la dernière version de chaque ligne source — de la mémoire en
plus sur le TaskManager. Mesuré : le job seul tient dans les 1536 Mo actuels.

**Leçon d'exploitation liée (incident du même jour)** : pendant la correction, deux instances du job
ont tourné en même temps sur les mêmes tables. Deux effets, tous deux instructifs :
1. **Écritures concurrentes** : en UPSERT, le dernier qui écrit gagne. Le second job, en pleine
   relecture du topic, a réécrit les jours passés avec des **compteurs partiels**, puis a été arrêté —
   laissant les 08, 09 et 10/09 faux (ex. 41 699 au lieu de 47 486). L'idempotence par UPSERT protège
   contre le rejeu d'**un** job, pas contre **deux** jobs simultanés.
2. **Saturation mémoire** : deux états de jointure sur un TaskManager dimensionné pour un seul →
   `OutOfMemoryError`, TaskManager désenregistré, jobs suivants refusés faute de slots.

Règle retenue : **un seul job par ensemble de tables cibles**. Le script `scripts/start-stack.ps1` ne
soumet le job que si aucun n'est déjà actif. En production, le mode haute disponibilité de Flink
(reprise du même job depuis un checkpoint) supprimerait ce risque de double soumission manuelle.

## 2026-09-11 — Un `TRUNCATE` côté source laisse des données fantômes dans le pipeline

**Constat** : après un reseed de `simulateur_V5` (nouveau volume de données, redémarrage à 14:55),
la base source ne comptait plus que 679 factures du 11/09, mais la base analytique affichait toujours
63 603 prestations sur 5 jours (07 au 11/09) — des données qui n'existaient plus côté source.

**Cause** : le nombre de messages Kafka (`TB_FACTURES` : 64 797, `TB_FACTURES_PRESTATIONS` : 63 721)
ne montrait aucune vague de suppressions, ce qui indique un `TRUNCATE` plutôt que des `DELETE` ligne
par ligne. Un `TRUNCATE` PostgreSQL n'est, par défaut, **pas traduit en événements** par Debezium
(contrairement à `DELETE`, capturé ligne par ligne). Kafka — et donc la base analytique — n'a jamais
été informé de la disparition des anciennes lignes : elles restent affichées indéfiniment.

**Limite du pipeline, à assumer explicitement au mémoire** : ce mécanisme ne détecte **aucune**
remise à zéro de la source par `TRUNCATE`. En production sur MIRKA, ce cas ne devrait jamais se
produire (une base métier n'est pas vidée), mais c'est une vraie limite du CDC log-based à mentionner.
Piste de durcissement non retenue ici : `skipped.operations` de Debezium peut être configuré pour
traiter le `TRUNCATE` autrement, ou une purge de la cible pourrait être déclenchée manuellement en
parallèle d'une purge connue de la source.

**Procédure de remise à zéro complète appliquée** (utile si ça se reproduit) :
1. Annuler le job Flink actif.
2. Supprimer les deux connecteurs Debezium (`DELETE /connectors/<nom>`).
3. Supprimer **uniquement nos deux** slots de réplication (`debezium_dprest`, `debezium_dprest_json`)
   — jamais les slots `cnam_*`, qui n'appartiennent pas à ce projet.
4. Supprimer les 4 topics Kafka concernés (Avro et JSON, `TB_FACTURES` et `TB_FACTURES_PRESTATIONS`).
5. Vider les 5 tables KPI de la base analytique (`TRUNCATE`).
6. Recréer les deux connecteurs à partir de leurs fichiers JSON — `snapshot.mode: initial` republie
   alors un instantané complet et à jour de la source.
7. **Piège rencontré** : supprimer un connecteur (étape 2) ne supprime pas la position de lecture que
   Kafka Connect a mémorisée pour lui (stockée dans le topic interne `_connect-offsets`, indépendamment
   de l'existence du connecteur). Recréer le connecteur avec un slot neuf mais une ancienne position
   fait échouer la tâche en boucle silencieuse (`status` reste `RUNNING` mais `tasks` reste vide).
   Corrigé avec l'API de gestion des offsets de Kafka Connect (disponible depuis la version 3.6, donc
   sur cette image) : `PUT /connectors/<nom>/stop`, `DELETE /connectors/<nom>/offsets`, puis
   `PUT /connectors/<nom>/resume`.
8. Resoumettre un seul job Flink.

**Validation** : source et pipeline identiques après coup (823 prestations, 827 factures, un seul
jour présent : 11/09).

## 2026-09-11 — Ententes préalables ajoutées au job existant ; KPI mensuel certifié en vue SQL, pas en fenêtre Flink

**Décision** : les KPI 16 à 21 (ententes préalables) sont ajoutés à `flink/sql/kpi_prestations.sql`
plutôt qu'à un job séparé — même principe que les familles précédentes : partager la lecture des
topics et l'état, ne pas dupliquer le dédoublonnage CDC. Le job passe de 5 à 7 sinks JDBC. Renommé
`kpi-continu` (au lieu de `kpi-prestations-continu`) pour refléter qu'il couvre les deux familles.

**Piège de nommage rencontré, corrigé avant intégration** : `ENTENTE_PREALABLE_DATE_DEBUT` est de
type `TIMESTAMP WITH TIME ZONE`, mais un ancien message de test lu par erreur (voir plus bas)
affichait `20703` — la même valeur que l'encodage entier des colonnes `DATE`. Vérification faite sur
un message frais : le champ arrive bien en chaîne ISO 8601 (`'2026-09-11T16:28:58.871991Z'`), pas en
entier. La cause de la confusion : le message lu venait de l'historique non nettoyé du topic (voir
point suivant), pas d'un bug de conversion.

**Même anomalie que pour les factures, sur les topics des ententes préalables** : au moment d'écrire
ce job, les 3 topics JSON des ententes préalables contenaient encore l'historique d'avant le reseed du
matin (13 000+ messages, contre 400 lignes en source) — je n'avais nettoyé que les topics
`TB_FACTURES*` lors du premier incident, pas ceux des ententes préalables. Corrigé avec la même
procédure : connecteurs arrêtés (`PUT .../stop`) **avant** suppression des topics (sinon le
connecteur, toujours actif, recrée le topic à la prochaine écriture — piège rencontré : une première
tentative de suppression, faite sans arrêter les connecteurs, a échoué silencieusement pour cette
raison), puis réinitialisation des offsets et reprise.

**KPI mensuel certifié (KPI 16-19, 21) : vue SQL plutôt que fenêtre Flink fermée.** La table
`kpi_ententes_prealables_mois` était prévue comme sortie d'une fenêtre Flink `TUMBLE` mensuelle avec
watermark. Décision : l'implémenter comme une **vue PostgreSQL** (`v_kpi_ententes_prealables_mois`,
`sql/analytics/003_kpi_ep_mois_vue.sql`) dérivée du grain journalier, filtrée aux mois entièrement
clos (`WHERE jour < DATE_TRUNC('month', CURRENT_DATE)`).

Pourquoi : une fenêtre Flink fermée sur un flux joint (EP × statut) réintroduirait exactement la
classe de bug rencontrée à l'étape 3 (watermark bloqué à `Long.MIN_VALUE`, fenêtre qui ne se ferme
jamais — voir plus haut, piège de nommage JSON) sur un TaskManager qui reste contraint en mémoire,
pour un gain qui ne se vérifiera de toute façon pas avant la fin du premier mois complet de données.
Le principe déjà posé dans `docs/kpi.md` (« toutes les périodes se dérivent du grain jour ») couvre
exactement ce besoin : une fois un jour écrit, il ne bouge plus (UPSERT sur la clé du jour passé,
jamais réécrit par un jour ultérieur) — donc une vue sur des mois clos est aussi définitive qu'une
fenêtre fermée, sans le risque opérationnel. La table physique `kpi_ententes_prealables_mois` (créée
dans `001_schema.sql`) reste en place, non alimentée, si une vraie fenêtre Flink devait être ajoutée
plus tard (ex. avec le mode haute disponibilité, à l'étape 7 ou au-delà).

**Validation** :
- Comparaison source/pipeline exacte sur les 3 statuts (357 acceptées, 72 refusées, 38 validées
  d'office), le montant engagé par statut (6 818 035 F / 0 F / 684 900 F) et l'activité par agent
  (20 agents, 429 lignes), avant et après le nettoyage.
- Idempotence testée explicitement sur les 4 tables continues (prestations, factures, EP, EP par
  agent) : job annulé puis resoumis, relecture complète des topics depuis `earliest-offset`, mêmes
  lignes et mêmes totaux avant/après (106/2334, 64/2338, 6/467, 40/429).
- Mémoire du TaskManager après ajout des 3 nouvelles sources et 2 nouveaux sinks : 437/744 Mo (59 %),
  en baisse par rapport à la mesure précédente (79 %) — la hausse observée avant le nettoyage des
  topics reflétait probablement le traitement de l'historique fantôme, pas un besoin réel plus élevé.

## 2026-09-11 — Étape 5 : Grafana s'appuie sur trois sources, pas uniquement Prometheus

**Décision** : plutôt que de faire transiter toutes les métriques par un exportateur Prometheus
générique, Grafana interroge directement la source la plus fiable pour chaque signal :
- **Prometheus** pour Flink (exportateur officiel `flink-metrics-prometheus`, natif, aucune
  dépendance supplémentaire) ;
- **SQL direct sur `postgres-analytics`** pour la fraîcheur des KPI (`EXTRACT(EPOCH FROM (now() -
  MAX(maj_le)))`) — la donnée existe déjà, inutile de la dupliquer dans un exportateur ;
- **API REST de Kafka Connect**, via le plugin communautaire Grafana `yesoreyeram-infinity-datasource`
  (installé au démarrage du conteneur via `GF_INSTALL_PLUGINS`), pour l'état des connecteurs
  Debezium — cette API donne déjà l'information exacte (`RUNNING`/`FAILED`) sans qu'il soit
  nécessaire d'ajouter un exportateur JMX à l'image `kafka-connect`.

**Pourquoi** : chaque exportateur supplémentaire est un processus JVM de plus, sur une machine qui
tourne déjà à la limite (~1,4 Go disponibles au moment du déploiement). Interroger directement les
API déjà existantes (REST Kafka Connect, SQL PostgreSQL) évite cette dépense sans perdre en fiabilité
— ce sont les sources de vérité elles-mêmes, pas une copie.

**Piège rencontré** : le panneau d'état des connecteurs restait vide malgré une requête Infinity
apparemment correcte. Cause : le paramètre `"parser": "backend"` était absent — sans lui, la réponse
JSON n'est pas convertie en colonnes de table. Vérifié requête par requête via l'API interne
`/api/ds/query` de Grafana avant d'écrire le JSON du tableau de bord, pour ne pas reproduire le genre
d'erreur silencieuse déjà rencontrée avec le nommage des colonnes Flink (étape 3).

**Alertes sur des données non numériques** : l'état d'un connecteur (`RUNNING`/`FAILED`) est un texte,
alors que le moteur d'alerte de Grafana évalue des seuils numériques. Résolu avec les
`computed_columns` d'Infinity, qui acceptent une expression conditionnelle
(`etat == "RUNNING" ? 1 : 0`), vérifiée manuellement avant intégration à la règle d'alerte.

**Décision assumée — pas d'alerte sur la RAM de la VM hôte** : la contrainte mémoire de cette machine
est réelle et documentée depuis l'étape 0, mais la couvrir avec un exportateur dédié (`node_exporter`
ou équivalent) reviendrait à consommer de la RAM pour surveiller un manque de RAM. Le contrôle reste
manuel (`podman machine ssh "free -m"`), déjà intégré aux guides d'exploitation.

**Validation** : alerte « Connecteur Debezium (JSON) en échec » déclenchée en mettant volontairement
le connecteur en pause (`PUT .../pause`), passée à `firing` après le délai `for: 1m`, puis revenue à
`inactive` après reprise (`PUT .../resume`). Cycle complet déclenchement/résolution confirmé — geste
réutilisable tel quel à l'étape 8 (mesure du temps de reprise après incident).

## 2026-09-11 — GF_SECURITY_ADMIN_PASSWORD ne s'applique qu'à la création de la base Grafana

**Constat** : connexion refusée sur Grafana avec `admin` / `dprest_dev_2026`, alors que ces valeurs
étaient bien celles définies dans `docker-compose.yml` et confirmées dans l'environnement du
conteneur (`printenv`).

**Cause** : `GF_SECURITY_ADMIN_USER`/`GF_SECURITY_ADMIN_PASSWORD` ne servent qu'à **créer** le compte
admin lors de la toute première initialisation de la base SQLite interne de Grafana. Une fois ce
compte créé, changer la variable d'environnement et redémarrer le conteneur ne met **pas** à jour le
mot de passe stocké — comportement normal de Grafana, pas un bug de notre configuration.

**Correction appliquée** :
```
podman exec pipeline_temps_reel-grafana-1 grafana-cli admin reset-admin-password dprest_dev_2026
```

**À savoir pour la suite** : si le mot de passe doit changer, cette commande est le seul moyen fiable
de le faire prendre effet sur un Grafana déjà initialisé — modifier `docker-compose.yml` seul ne suffit
pas.

## 2026-09-11 — Retrait du connecteur Debezium Avro (`dprest-postgres-source`)

**Décision** : suppression complète du second connecteur Debezium, celui en format Avro, resté en
place depuis l'étape 2 comme démonstration de l'architecture cible (Kafka + Schema Registry, contrat
de schéma). Supprimés avec lui : le connecteur (`DELETE /connectors/dprest-postgres-source`), son
slot de réplication PostgreSQL (`debezium_dprest`), ses 3 topics Avro (`dprest.public.*`), et son
fichier de configuration (`connectors/debezium-postgres.json`).

**Pourquoi maintenant** : Flink ne l'a jamais consommé (bug de désérialisation Avro documenté plus
haut, contourné par un second connecteur en JSON dès l'étape 3). Le connecteur Avro est retombé en
échec après le reseed du 11/09 (conflit de compatibilité de schéma sur `TB_ENTENTES_PREALABLES` dans
Schema Registry) — un incident de plus à corriger pour un composant qui ne sert déjà à rien dans le
pipeline. Décision : au lieu de le réparer une nouvelle fois, l'retirer.

**Ce que ça change pour le mémoire** : l'architecture ne montre plus, en direct, un flux Avro +
Schema Registry en fonctionnement. Ça reste documenté ici comme un choix exploré puis écarté, avec sa
justification technique complète (bug de compatibilité Flink/Avro) — argument tout aussi défendable en
soutenance qu'une démonstration active, et plus honnête qu'un connecteur qu'on relance sans qu'il
serve à rien.

**Fichiers mis à jour** : `scripts/start-stack.ps1` (un seul connecteur vérifié au démarrage),
`docs/guides/demarrage_arret.md`, `monitoring/grafana/dashboards/pipeline-overview.json` (panneau
« État des connecteurs » réduit à un seul connecteur).

## 2026-09-11 — REPLICA IDENTITY manquant sur les tables des ententes préalables

**Constat** : le job Flink est tombé en `FAILED` sur une erreur de désérialisation :
```
Caused by: java.lang.IllegalStateException: The "before" field of UPDATE message is null,
if you are using Debezium Postgres Connector, please check the Postgres table has been set
REPLICA IDENTITY to FULL level.
```
Détecté par l'alerte Grafana « Job Flink arrêté », déclenchée quelques minutes après l'incident.

**Cause** : `TB_ENTENTES_PREALABLES`, `TB_ENTENTES_PREALABLES_STATUTS` et
`TB_ENTENTES_PREALABLES_ACTES_MEDICAUX` étaient en `REPLICA IDENTITY DEFAULT` (clé primaire seule)
côté source, alors que `TB_FACTURES` et `TB_FACTURES_PRESTATIONS` étaient déjà en `FULL`. Sans
`FULL`, PostgreSQL n'inclut pas l'état complet de la ligne *avant* modification dans le journal de
réplication logique pour un `UPDATE` — Debezium ne peut alors publier qu'un `after`, avec `before`
à `null`. Le format `debezium-json` de Flink exige ce `before` pour maintenir correctement son état
de changelog (retrait de l'ancienne valeur avant application de la nouvelle).

**Correction** :
```sql
ALTER TABLE "TB_ENTENTES_PREALABLES" REPLICA IDENTITY FULL;
ALTER TABLE "TB_ENTENTES_PREALABLES_STATUTS" REPLICA IDENTITY FULL;
ALTER TABLE "TB_ENTENTES_PREALABLES_ACTES_MEDICAUX" REPLICA IDENTITY FULL;
```
Appliqué sur `simulateur_V5` — opération sans risque (aucune donnée modifiée, juste plus
d'information dans le journal de réplication à partir de maintenant), réversible
(`REPLICA IDENTITY DEFAULT`).

**Piège** : corriger la source ne suffit pas — le message corrompu restait dans l'historique du
topic Kafka, donc rejouer depuis `earliest-offset` aurait fait replanter le job identiquement.
Procédure appliquée : connecteur arrêté, les 3 topics JSON des ententes préalables supprimés,
offsets réinitialisés (nouvel instantané complet), connecteur repris, job resoumis. Même méthode que
l'incident du `TRUNCATE` plus haut.

**Validation** : job `RUNNING` sans exception ; source et pipeline identiques (556 ententes
préalables, 2776 prestations) ; les 6 alertes Grafana revenues à `inactive`.

**À retenir pour le dictionnaire de données** : toute nouvelle table ajoutée au périmètre CDC doit
être vérifiée pour `REPLICA IDENTITY FULL` **avant** l'intégration Flink, pas après le premier
`UPDATE` en production — ce n'est visible qu'à l'usage, jamais en lecture seule du schéma.

## 2026-09-11 — Plugin Grafana Infinity : version incompatible, puis syntaxe de fixation de version incorrecte

**Constat** : le panneau « État du connecteur Debezium » affichait une erreur au chargement :
`Kafka Connect (JSON API) plugin failed — Error: 404 Not Found, loading react/jsx-runtime from
.../yesoreyeram-infinity-datasource/module.js`. Les requêtes du panneau fonctionnaient pourtant
(vérifiées via l'API `/api/ds/query`) : le problème était uniquement le chargement du plugin côté
navigateur.

**Cause n°1** : `GF_INSTALL_PLUGINS: yesoreyeram-infinity-datasource` (sans version) installe la
dernière version disponible au moment du démarrage — la **4.0.0**, qui exige Grafana `>= 11.6.11`.
Notre image est `grafana-oss:11.1.4`. Le bundle JS de cette version réfère un module partagé
(`react/jsx-runtime`) exposé différemment par les Grafana récents, absent en 11.1.4 → 404 côté
navigateur uniquement (le backend du plugin, qui répond aux requêtes API, restait fonctionnel).

**Version compatible retenue** : `3.7.1`, la plus récente dont `grafanaDependency` (`>= 10.4.8`)
couvre notre version de Grafana. Vérifié via l'API `grafana.com/api/plugins/.../versions/<v>`, en
remontant les versions une à une (3.8.0 et au-delà exigent déjà `>= 11.6.0`).

**Cause n°2, plus sérieuse (boucle de redémarrage)** : fixer la version avec la syntaxe
`GF_INSTALL_PLUGINS: yesoreyeram-infinity-datasource@3.7.1` a fait tomber le conteneur en boucle de
redémarrage (`Error: 404: Plugin not found`, en continu). En lisant `/run.sh` dans l'image, le script
ne scinde `GF_INSTALL_PLUGINS` que sur la **virgule** (plusieurs plugins), puis passe la chaîne
entière à `grafana cli plugins install` — la syntaxe `id@version` n'est **pas** reconnue par ce
script (elle existe pour d'autres outils, pas celui-ci) : `grafana cli` cherche un plugin littéralement
nommé `yesoreyeram-infinity-datasource@3.7.1`, qui n'existe pas → 404 → le conteneur, en échec dès le
démarrage, redémarre indéfiniment (`restart: unless-stopped`).

**Syntaxe correcte** (seule prise en charge par ce script pour fixer une version) : une URL de
téléchargement directe, suivie d'un point-virgule et du nom de dossier d'installation :
```
GF_INSTALL_PLUGINS: "https://grafana.com/api/plugins/yesoreyeram-infinity-datasource/versions/3.7.1/download;yesoreyeram-infinity-datasource"
```
Vérifiée manuellement (`grafana cli --pluginUrl ... --pluginsDir ... plugins install ...`) avant
d'être intégrée à `docker-compose.yml`, pour ne pas répéter l'incident en boucle.

**Validation** : conteneur recréé, `healthy` sans redémarrage ; `module.js` servi contient bien
`plugin: yesoreyeram-infinity-datasource@3.7.1` dans son en-tête ; requête du panneau toujours
fonctionnelle après recréation.

**Point à surveiller pour le mémoire** : cette image Grafana ne conserve aucun volume persistant
(base SQLite interne dans la couche du conteneur) — toute recréation réinitialise le mot de passe
admin (`grafana-cli admin reset-admin-password`, voir plus haut) et réinstalle les plugins depuis
zéro. Comportement voulu ici (tout reste reproductible par fichiers), mais à noter comme différence
avec une exploitation réelle, où un volume dédié serait indispensable.

## 2026-09-11 — Étape 5, ajouts : retard des slots de réplication + volume du jour

**Décision** : deux ajouts à la supervision, suite au diagnostic du panneau Debezium.

1. **Nouvelle source de données Grafana « PostgreSQL source (simulateur_V5) »**, connexion directe
   en lecture à `postgres` sur `simulateur_v5_net` (réseau ajouté à Grafana). Ce n'est pas une entorse
   au principe « la source n'est jamais interrogée par les outils de restitution » : la seule requête
   utilisée porte sur `pg_replication_slots`, une vue système de supervision, jamais sur les données
   métier.
2. **Panneau + alerte sur le retard des slots de réplication logique.** Découverte lors du diagnostic
   du panneau Debezium (session précédente) : deux slots `cnam_*`, non utilisés par ce pipeline,
   retiennent indéfiniment les journaux de transactions PostgreSQL — 3,7 Go puis 6 Go / 3 Go constatés
   en quelques heures le même jour. Un slot qui n'avance plus est un risque réel de saturation du
   disque de la VM, invisible dans tout ce qu'on surveillait jusque-là (RAM, job Flink, connecteurs).
   Seuil retenu : 2 Go, `for: 5m` (plus long que les autres alertes, pour ne pas réagir à un pic
   ponctuel de trafic — un retard de réplication est normalement transitoire).
3. **Deux compteurs d'activité du jour** (prestations, ententes préalables) sur la base analytique —
   complémentaires à la fraîcheur : une fraîcheur dégradée conjuguée à un volume qui continue de
   monter n'a pas le même diagnostic qu'un simulateur simplement à l'arrêt.

**Panneau « État du connecteur Debezium » vide (« No data »), diagnostiqué séparément** : le backend
répondait correctement (vérifié via `/api/ds/query`, identique à la requête stockée dans le panneau) ;
la cause la plus probable était un cache navigateur de l'ancienne version du plugin (4.0.0, cassée),
non renouvelé par un rechargement forcé après la mise à jour vers 3.7.1.

**Validation** : datasource testée (`Database Connection OK`), requête du panneau de retard des slots
confirmée avec les chiffres réels (`cnam_debezium_slot` 6010 Mo, `cnam_slot` 3076 Mo,
`debezium_dprest_json` 0,3 Mo), 7 règles d'alerte chargées sans erreur, nouvelle alerte en `pending`.

## 2026-09-11/12 — Étape 6 : Superset, architecture proche production, secrets en `.env`

**Décision — RAM de la VM portée à 7 Go.** Avant de commencer Superset, la RAM disponible sur la VM
Podman était insuffisante (mesurée à 121-188 Mo libres avec le reste de la stack). Deux options
présentées à Mathieu : SQLite/sans cache pour Superset (léger, mais reproduit modestement
l'architecture cible), ou base PostgreSQL dédiée + Redis (proche production) en augmentant la RAM.
Choix retenu : **augmenter la RAM à 7 Go** (et non 8 comme d'abord envisagé — seulement 1,8 Go libre
côté Windows au moment de la demande, à 15,6 Go de RAM totale sur la machine) puis architecture
proche production. Procédure : `.wslconfig` modifié (6 → 7 Go), `wsl --shutdown`, redémarrage complet
de la VM — confirmé par `podman machine ssh "free -m"` (6934 Mo total, ~2 Go de marge réelle après
démarrage de la stack).

**Décision — secrets Superset dans `.env`, pas dans `docker-compose.yml`.** Contrairement à tous les
autres services de ce dépôt, où un mot de passe de développement apparaît en clair (documenté et
justifié à chaque fois : services internes, sans exposition au-delà de cette machine), Superset gère
de **vrais comptes utilisateurs**. Règle stricte de `CLAUDE.md` appliquée ici : `SUPERSET_SECRET_KEY`,
`SUPERSET_DB_PASSWORD`, `SUPERSET_ADMIN_PASSWORD` générés aléatoirement (32/16/12 octets, CSPRNG) et
stockés uniquement dans `.env`, déjà exclu du dépôt par `.gitignore`.

**Piège rencontré — `apache/superset` sans pilote PostgreSQL.** L'image officielle ne contient que le
nécessaire pour SQLite. `superset-init` échouait dès la première commande (`superset db upgrade`) avec
`ModuleNotFoundError: No module named 'psycopg2'` — sur la connexion à sa **propre** base de
métadonnées, avant même d'atteindre `postgres-analytics`. Corrigé par une image dérivée minimale
(`superset/Containerfile`, `pip install psycopg2-binary`).

**Choix d'architecture Superset — pas de worker Celery.** Redis est présent (cache des requêtes, état
des filtres), mais aucun worker asynchrone n'est déployé : les requêtes restent synchrones. Justifié
par le faible volume du simulateur et l'absence de besoin d'export planifié à ce stade — Celery
ajouterait un service de plus sur une machine qui reste contrainte, pour un gain non nécessaire ici.

**Automatisation via API plutôt qu'interface.** Connexion à la base, 10 jeux de données, 14
graphiques et 2 tableaux de bord (avec mise en page, `position_json`) créés par scripts contre l'API
REST de Superset, cohérent avec le principe déjà appliqué à Grafana : tout reproductible par fichiers
ou commandes, rien qui ne dépende d'un clic non documenté.

**Piège rencontré — jeton CSRF « manquant » alors qu'il était présent.** Le script Python
d'automatisation (construction des tableaux de bord) échouait avec *"The CSRF session token is
missing"* malgré un jeton CSRF valide transmis. Cause : le fichier de cookies généré par `curl -c`
préfixe les cookies marqués `HttpOnly` par `#HttpOnly_` (format Netscape standard) — un filtre naïf
qui ignore les lignes commençant par `#` (pour sauter les commentaires du fichier) éliminait donc
aussi le cookie de session, pourtant indispensable à la vérification CSRF côté serveur. Corrigé en
reconnaissant explicitement ce préfixe avant de l'ignorer.

**Vérification, avant construction des tableaux de bord** : chaque type de graphique testé isolément
via `/api/v1/chart/data` et comparé au SQL de contrôle avant d'être multiplié — même discipline que
pour Grafana (ne jamais généraliser un format non vérifié). Résultat : tous les chiffres identiques
(4894 prestations, taux de couverture 82,1 %, ventilation EP par statut avec un cas réel
`sans_reponse` qui valide l'hypothèse H4 en conditions réelles, montant engagé 17 169 685 F).

**Limite assumée, à mentionner en soutenance** : les tableaux de bord ont une mise en page fonctionnelle
mais générée par script (grille simple, pas de mise en forme visuelle fine) ; aucun contrôle d'accès
par rôle pour distinguer les comptes DPREST (prévu à l'étape 7) ; le sélecteur de période natif de
Superset (filtres temporels sur `jour`) n'a pas encore été configuré par défaut sur les tableaux de
bord — à faire au prochain passage, ou par l'utilisateur directement dans l'interface.

## 2026-09-12 — Étape 7 : sécurité, chiffrement, rôles, traçabilité

Périmètre validé avec Mathieu avant de commencer : secrets hors du code (1), comptes par service à
moindre privilège (2), rôle DPREST en lecture seule dans Superset (3), traçabilité (4), et HTTPS
**uniquement sur les interfaces web** (Grafana, Superset) plutôt que sur tout le pipeline (5) — le
reste communique en clair sur le réseau Podman interne, qui ne sort jamais de cette machine. Décision
explicite : une vraie mise en production distribuée sur plusieurs machines exigerait du TLS
inter-services (voire du mTLS), non implémenté ici et documenté comme limite.

### 1. Secrets hors du code versionné

- `docker-compose.yml` : tous les mots de passe par défaut en clair (`${VAR:-dprest_dev_2026}`)
  remplacés par `${VAR:?message}` — le service refuse de démarrer si `.env` est absent, plutôt que de
  retomber silencieusement sur une valeur publique. `.env` porte désormais aussi
  `ANALYTICS_PASSWORD`, `ANALYTICS_RO_PASSWORD`, `GRAFANA_ADMIN_PASSWORD`, `SOURCE_RO_PASSWORD`
  (secrets aléatoires générés par CSPRNG).
- **Connecteur Debezium** : le mot de passe de la base source ne figure plus dans
  `connectors/*.json` (versionné). Mécanisme FileConfigProvider, natif à Kafka Connect — pas un
  outil ajouté : le champ référence `/run/secrets/db.properties`, résolu au démarrage depuis
  `connectors/secrets/db.properties` (monté en volume, non versionné, voir
  `connectors/secrets/README.md`). Vérifié : la valeur littérale reste dans la réponse de l'API
  (`GET .../config`) — le secret n'est jamais exposé, même à un administrateur Kafka Connect
  consultant la config du connecteur.
- **Job Flink** : le mot de passe JDBC (7 occurrences dans `flink/sql/kpi_prestations.sql`) remplacé
  par un jeton, substitué par `scripts/start-stack.ps1` dans la copie du fichier à l'intérieur du
  conteneur JobManager, juste avant soumission — jamais écrit en clair sur disque côté hôte ni dans
  Git.
- **Grafana (`datasources.yml`)** : passage à la syntaxe de substitution de variable d'environnement
  officiellement documentée par Grafana depuis la 8.x. Une tentative précédente (étape 5) avait
  conclu à tort que cette substitution était peu fiable et était restée en clair — revérifié ici et
  confirmé fonctionnel (testé via l'API santé de la source de données puis une requête réelle).
  Correction de cette hypothèse précédente.

### 2. Comptes par service, moindre privilège

| Compte | Rôle | Portée | Testé |
|---|---|---|---|
| `dprest` (existant) | Propriétaire, écriture | `postgres-analytics`, utilisé par Flink | — |
| `dprest_lecture` (nouveau) | Lecture seule | `postgres-analytics`, utilisé par Grafana et Superset | `SELECT` OK, `DELETE` refusé (permission denied) |
| `monitoring_ro` (nouveau) | Rôle intégré `pg_monitor` | Base source (`simulateur_V5`), lecture des vues système uniquement | Lit les slots de réplication, lecture d'une table métier refusée |
| `echo` (existant, inchangé) | Compte applicatif de `simulateur_V5` | Utilisé par Debezium pour le CDC | — |

Limite assumée, documentée plutôt que risquée : pas de compte Debezium totalement séparé du compte
applicatif `echo`. Le connecteur, déjà en fonctionnement, est propriétaire de la publication de
réplication logique ; transférer cette propriété à un rôle CDC dédié est une opération plus risquée
sur un pipeline en production sur cette machine, non testée de façon isolée dans le temps imparti.
Piste retenue pour une vraie mise en production : créer le rôle CDC dès la création de la
publication, pas après coup.

### 3. Rôle DPREST en lecture seule (Superset)

Compte `dprest` créé avec le rôle intégré Gamma (le plus restrictif de Superset, hors « Public »).
Accès explicitement accordé (via le shell Superset, mécanisme officiellement documenté — pas de
création de rôle exposée par l'API REST versionnée) : accès à la base analytique et aux 10 jeux de
données KPI.

Piège rencontré : les deux tableaux de bord étaient invisibles pour ce compte malgré les permissions
accordées. Cause : ils étaient en brouillon (non publiés) — Superset masque les dashboards en
brouillon aux utilisateurs autres que leurs propriétaires, indépendamment des permissions de données.
Corrigé en les publiant.

Validation : le compte DPREST voit exactement les 2 tableaux de bord, ni plus ni moins ; une
tentative d'écriture est refusée par le rôle Gamma.

### 4. Traçabilité

- Superset : journal d'accès déjà actif nativement, sans configuration supplémentaire — chaque appel
  d'API est journalisé avec l'utilisateur et l'horodatage. Vérifié : les actions du compte DPREST
  apparaissent distinctement de celles de l'admin.
- PostgreSQL analytique : connexions et déconnexions désormais journalisées, avec un préfixe de log
  incluant l'utilisateur et la base. Vérifié : une connexion avec le compte en lecture seule apparaît
  nommément dans les journaux du conteneur.
- Non fait : pas d'agrégation centralisée des journaux (ELK, Loki) — chaque service garde les siens.
  Justifié pour ce simulateur ; à prévoir pour une vraie exploitation.

### 5. HTTPS sur les interfaces web (Grafana, Superset)

Un unique conteneur reverse proxy (nginx, léger) fait office de terminaison TLS pour les deux
applications, plutôt que de configurer TLS séparément dans chacune. Certificat auto-signé
(non versionné, régénérable — voir `reverse-proxy/README.md`). Grafana en HTTPS sur le port 3443,
Superset sur 8443 ; les anciens ports en clair (3000, 8088) ne sont plus publiés vers l'hôte —
uniquement le proxy y accède, en interne.

Deux pièges rencontrés, corrigés avant validation finale :

1. Image nginx figée visée au départ, jamais récupérée : un pull s'est bloqué côté client
   Podman/Windows sans jamais atteindre la VM, malgré une connectivité réseau par ailleurs
   fonctionnelle — probablement un incident ponctuel du jour. Contournement : image générique déjà
   en cache local. Non retesté si l'incident se reproduit avec une version figée.
2. « Bad Gateway » après recréation de Grafana ou Superset. nginx résout les noms de service une
   seule fois, au démarrage ; recréer l'un de ces conteneurs (nouvelle adresse interne) laissait le
   proxy bloqué jusqu'à son propre redémarrage manuel. Corrigé avec un résolveur DNS dynamique
   (force une réévaluation à chaque requête) — mais l'adresse conventionnelle de Docker pour son DNS
   interne ne fonctionne pas sous Podman : le DNS interne (aardvark-dns) répond sur une adresse
   propre à chaque réseau, lisible dans la configuration réseau du conteneur. Résolu en injectant
   cette adresse au démarrage du service. Validé en conditions réelles : Grafana recréé, proxy
   jamais touché, reconnexion automatique dès que Grafana redevient disponible.

Validation finale de toute l'étape 7 : les 6 points ci-dessus testés individuellement, puis
vérification de bout en bout du pipeline après toutes les rotations de mots de passe et recréations
de conteneurs — source et cible toujours identiques, job Flink RUNNING sans exception.

### Découverte annexe, corrigée au passage

`docs/` figurait dans `.gitignore` depuis le tout début du projet : toute la documentation
(`PLAN.md`, ce fichier, `kpi.md`, les guides) n'avait jamais été commitée. Retiré de `.gitignore` dès
que constaté — sans rapport avec la sécurité, mais trop important pour attendre.

## 2026-09-12/13 — Supervision étendue : métriques Kafka, taille des bases, connexions, audit Superset

Une fois les 7 étapes en place, extension de Grafana avec ce qui manquait encore, en distinguant ce
qui ne coûte rien (aucun service de plus) de ce qui a un vrai coût.

**Trois ajouts sans coût, en réutilisant les sources de données déjà existantes** : taille des deux
bases PostgreSQL (`pg_database_size`), connexions actives par compte sur la base analytique
(`pg_stat_activity` — rend visible en un coup d'œil les comptes créés à l'étape 7), et activité
Superset (nouvelle source de données Grafana vers `superset-db`, avec un rôle `superset_ro` dédié en
lecture seule, cohérent avec le principe de moindre privilège déjà appliqué ailleurs — le journal
d'audit de Superset était déjà vérifié en ligne de commande, il est maintenant visible sans y penser).

**Métriques Kafka, via l'agent JMX intégré au broker — pas un service séparé.** L'agent tourne dans le
même processus JVM que Kafka : aucun conteneur de plus, coût mémoire marginal. Débit du broker
(entrant/sortant), taille par topic, nombre de partitions.

**Découverte majeure en préparant cet ajout : Kafka n'avait jamais eu de stockage persistant.**
`log.dirs` valait `/tmp/kraft-combined-logs` par défaut — un chemin qui ne survit à aucune recréation
de conteneur. Passé inaperçu jusqu'ici car Kafka n'avait jamais eu besoin d'être reconstruit (image
officielle utilisée telle quelle depuis l'étape 2). Corrigé avec un volume dédié (`kafka_data`) et
`KAFKA_LOG_DIRS` explicite — plus jamais de perte de données à une future recréation.

**Remise à zéro complète et assumée**, seule option raisonnable pour appliquer ce changement sur un
Kafka déjà en fonctionnement (pas de migration en direct de l'état interne KRaft tentée, jugée trop
risquée sans fenêtre de test) : job Flink annulé, Kafka Connect et Schema Registry arrêtés, Kafka
recréé avec son nouveau volume, les deux redémarrés (leurs topics internes se recréent tout seuls),
connecteur Debezium réenregistré depuis `connectors/debezium-postgres-json.json`, job Flink resoumis.
Impact réel : uniquement les données de démonstration dans Kafka ; la base analytique a gardé tout
son historique.

**Deux pièges rencontrés en cours de route** :
1. `curl` absent de l'image de base (Alpine) — `wget`, déjà présent, utilisé à la place.
2. **Le plus sérieux** : `KAFKA_OPTS` (utilisé pour charger l'agent JMX) s'applique, par conception de
   l'image, à **toute** commande `kafka-*.sh` du conteneur — y compris le propre test de santé du
   conteneur et n'importe quel `podman exec` ultérieur. Chacune de ces commandes tentait de rouvrir le
   port 7071 déjà occupé par l'agent du broker principal, provoquant l'échec du healthcheck : le
   conteneur restait bloqué en `starting` indéfiniment, sans jamais passer `healthy`, sans message
   d'erreur explicite au premier coup d'œil. Corrigé en injectant l'option **seulement sur la commande
   de démarrage du broker** (`command:` du service, pas `environment:`) plutôt que globalement —
   `podman exec` n'hérite alors plus de cette variable, seul le process broker la porte.

**Validation** : les 4 nouveaux panneaux vérifiés un par un via l'API de requête de Grafana avant
d'être considérés acquis (même discipline que pour les étapes précédentes) ; pipeline entier revérifié
de bout en bout après la reconstruction de Kafka — source et cible identiques, job Flink `RUNNING`
sans exception, les 6 alertes de fonctionnement normal revenues à `inactive` (la 7ᵉ, le retard des
slots orphelins, reste `pending` — anomalie réelle et déjà connue, pas une régression).

## 2026-09-14 — Superset reconstruit à neuf ; piège `reverse-proxy` : `$host` sans port casse le CSRF

**Contexte.** Les dashboards Superset (créés le 2026-09-11 par script API REST, jamais versionné —
voir plus haut) n'étaient plus reproductibles. Décision : conteneurs `superset`, `superset-init`,
`superset-db`, `superset-redis` supprimés, volume `superset_db_data` détruit, tout reconstruit à zéro,
et remplacement du script perdu par deux guides pas-à-pas manuels versionnés :
`docs/guides/etape6a_superset_prestations.md` et `etape6b_superset_ententes.md` — 24 KPI couverts,
16 + 8 graphiques, un type de visualisation choisi par nature de donnée (Big Number / Gauge / Table /
Bar / Line) plutôt que reconduit à l'identique, une palette de couleurs unique partagée entre les deux
dashboards. `postgres-analytics` n'a pas été touché.

**Piège rencontré — la reconstruction efface aussi la connexion PostgreSQL de Superset**, pas
seulement les dashboards : la connexion à `dprest_analytics` est elle-même une métadonnée stockée
dans `superset-db`. À refaire manuellement (compte `dprest_lecture`, lecture seule — même choix que
pour Grafana, voir plus haut § comptes par service — jamais le compte `dprest`, qui a les droits
d'écriture de Flink : une connexion de restitution ne doit techniquement pas pouvoir modifier la base
analytique, quel que soit le compte Superset connecté derrière).

**Piège le plus sérieux — `400 Bad Request: The referrer does not match the host`, sur toute création
dans Superset (connexion, jeu de données...).** Cause : `reverse-proxy/nginx.conf` transmettait
`proxy_set_header Host $host;` — la variable nginx `$host` ne porte jamais le port. Le navigateur
accède à Superset via `https://<IP>:8443`, donc son `Referer` contient `:8443` ; mais Superset, recevant
un en-tête `Host` sans port, reconstruit sa propre URL avec le port réel sur lequel il écoute en
interne (`:8088`) — les deux ne correspondent plus, et la protection CSRF de Flask-WTF rejette la
requête. Diagnostiqué en reproduisant côté serveur exactement l'appel du formulaire (même commande
interne que le bouton « Test Connection », `superset.commands.database.test_connection`) : succès à
chaque fois en direct, donc le problème n'était ni le mot de passe, ni le réseau, ni la base — signal
qu'il fallait chercher entre le navigateur et Superset. Corrigé en remplaçant `$host` par `$http_host`
(qui conserve le port du client) dans les deux blocs (`Superset`, et `Grafana` par symétrie/prévention)
de `reverse-proxy/nginx.conf`. Point d'attention pour la suite : ce fichier n'est qu'un **gabarit**
(`nginx.conf.template`, substitué par `sed` au démarrage du conteneur, voir `docker-compose.yml`) — un
`nginx -s reload` sur le conteneur en cours ne suffit pas après une modification, il faut redémarrer le
conteneur (`podman compose restart reverse-proxy`) pour que la substitution se refasse depuis le
fichier corrigé.

**Validation** : connexion recréée et testée en relisant l'enregistrement Superset puis en ouvrant une
vraie connexion PostgreSQL dessus (`SELECT current_user` → `dprest_lecture`) ; correctif nginx vérifié
en relisant la configuration générée à l'intérieur du conteneur après redémarrage (les deux occurrences
de `Host $http_host` bien présentes).

## 2026-09-15 — Noms des centres/praticiens et coordonnées géographiques dans Superset

**Besoin.** Les dashboards n'affichaient que des codes (`CENTRE_SANTE_CODE`, `PROFESSIONNEL_SANTE_CODE`) —
peu lisibles pour la DPREST. Vérifié au préalable : ce n'est pas interdit par la loi n°2013-450 (qui
protège l'identité des *patients*, `TB_REF_ASSURES` — jamais touchée) ; les centres et praticiens sont
des entités professionnelles/institutionnelles, dont le nom est déjà public.

**Mise en œuvre**, même mécanisme CDC que le reste du pipeline (cohérent avec l'architecture Kappa) :
- `connectors/debezium-postgres-json.json` : `TB_REF_CENTRES_SANTE`, `TB_REF_PROFESSIONNELS_SANTE`,
  `TB_REF_COLLECTIVITES` ajoutées à `table.include.list`. Cette dernière porte les coordonnées
  (`COLLECTIVITE_LATITUDE`/`LONGITUDE`, nullables — géocodage incomplet côté simulateur, migration
  `simulateur_V5/alembic/versions/20260914_0027_coordonnees_collectivites.py`), pas les centres
  eux-mêmes : un centre est rattaché à une localité (`COLLECTIVITE_CODE`, sans contrainte FK jusqu'ici).
- `sql/analytics/004_dim_referentiels.sql` : trois tables de dimension (`dim_centres_sante`,
  `dim_professionnels_sante` — `nom`/`prenoms` séparés comme en source —, `dim_collectivites`), plus
  deux vues (`v_centres_sante_geo`, `v_professionnels_sante` avec `nom_complet` pré-assemblé).
  Droits `SELECT` accordés à `dprest_lecture`.
- `flink/sql/kpi_prestations.sql` : trois nouveaux flux d'upsert (dix au total désormais), simples
  1:1 sans jointure — la jointure fait-table/dimension se fait à la lecture (vue SQL ou dataset
  Superset), pas figée dans Flink, pour rester valable si le géocodage progresse plus tard.

**Piège rencontré — ajouter des tables à `table.include.list` ne suffit pas.** Le connecteur avait déjà
fait son instantané initial ; `snapshot.mode: initial` ne rejoue pas automatiquement un instantané pour
des tables ajoutées après coup. Première tentative (connecteur supprimé, slot `debezium_dprest_json` et
publication `dbz_publication_json` supprimés côté source, connecteur recréé) : Debezium a quand même
**sauté l'instantané** (`SnapshotResult [status=SKIPPED]`) — Kafka Connect avait conservé l'offset du
connecteur **sous son nom**, indépendamment du slot PostgreSQL, et a repris le streaming au lieu de
repartir de zéro. Corrigé via l'API de réinitialisation des offsets (KIP-875, disponible sur cette
version) : `PUT .../stop` → `DELETE .../offsets` → `PUT .../resume`. Après ça, instantané complet
effectué, 3 nouveaux topics Kafka créés, connecteur `RUNNING` stable.

**Job Flink** : le job précédent était déjà `CANCELED` proprement (aucune tâche en échec — arrêté
pendant une pause de travail). Nouveau job resoumis avec le fichier mis à jour ; comme le job ne
redémarre pas depuis un checkpoint/savepoint, chaque source Kafka relit depuis `earliest-offset` — donc
un rejeu complet de l'historique, sans double comptage grâce à l'UPSERT idempotent sur la clé
(jour + dimensions).

**Fausse alerte, à retenir pour la suite** : après resoumission, le total agrégé de prestations
(29 363) semblait très supérieur au nombre de lignes dans la source (6 383 à ce moment). Cause réelle,
pas un bug : le simulateur avait été **remis à zéro et reseedé pour une nouvelle simulation** entre
temps (source ne portant plus que le 2026-09-15), alors que l'analytique conserve l'historique complet
depuis le 2026-09-11 — comportement voulu d'un entrepôt analytique, à ne pas confondre avec une
incohérence. Vérification correcte : comparer jour par jour, pas les totaux cumulés (`SELECT jour,
SUM(nombre_prestations) FROM kpi_prestations_jour GROUP BY jour ORDER BY jour`) — le jour courant
concorde à 5 lignes près avec la source (retard de fraîcheur normal). Argument valorisable en
soutenance (chapitre 7) : le pipeline a survécu à une réinitialisation complète de la source sans
perdre l'historique déjà calculé, contrairement au circuit manuel actuel.

**Superset** : deux jeux de données Custom SQL créés directement en base (même méthode que la
connexion recréée plus haut — script Python contre l'ORM Superset, pas de script API REST non
versionné cette fois) — `v_top_centres_sante_nomme` et `v_top_praticiens_nomme`, joignant chaque table
de faits à sa dimension. Guide `docs/guides/etape6a_superset_prestations.md` (sections 3.9, 3.10)
corrigé pour les utiliser dès la construction initiale des graphiques Top centres/praticiens, plutôt
que de repasser dessus après coup.

**Vigilance restante, non traitée ici** : `cnam_slot` et `cnam_debezium_slot` (slots de réplication
orphelins signalés dans le panneau Grafana dédié) n'ont pas été supprimés — `cnam_slot` appartient au
projet voisin `pipeline-cnam` (`debezium-connector.json`), à ne pas couper sans confirmation que ce
projet est abandonné ; `cnam_debezium_slot` semble réellement orphelin (aucun propriétaire trouvé) mais
reste en attente de décision.

## 2026-09-17 — Deux corrections sur le dashboard « Ententes préalables », et une table de qualité

Revue à froid des KPI de la famille B, déclenchée par un doute utilisateur (« les calculs sur les
ententes préalables sont faussés »). Deux erreurs distinctes trouvées, aucune dans le job Flink
lui-même pour la première.

**1. KPI 19 (délai moyen de traitement), formule Superset erronée.** Le Big Number utilisait
`SUM(delai_moyen_jours * nombre_ententes) / SUM(nombre_ententes)`. Le numérateur exclut de fait les
lignes `sans_reponse` (`delai_moyen_jours` y est `NULL`, `NULL * x` sauté par `SUM`), mais le
dénominateur les comptait quand même — délai moyen systématiquement sous-estimé. La vue
`v_kpi_ententes_prealables_mois` (`sql/analytics/003_kpi_ep_mois_vue.sql`) avait déjà la bonne
formule ; recopiée dans le graphique Superset. Voir `docs/guides/etape6b_superset_ententes.md`
section 2.4 et `evaluation/controle_analytics.sql` section 14b.

**2. KPI 20 (activité par praticien-conseil), périmètre faux depuis le début.** `docs/kpi.md` exige de
restreindre aux agents `AGENT_TYPE_CODE = 'medecin_conseil'`. `TB_REF_AGENTS` — seule table portant ce
type — n'était pas dans `table.include.list` du connecteur : le filtre était donc structurellement
impossible, pas juste absent du graphique. Le dashboard comptait les ~20 agents de tous types
(accueil, medecin_conseil, autre) sous un titre qui n'aurait dû en montrer que ~10.

Correction, même mécanisme que l'ajout des dimensions du 2026-09-15 : `TB_REF_AGENTS` ajoutée à
`table.include.list` ; nouvelle source/cible Flink `dim_agents` (upsert direct, sans jointure, comme
les autres dimensions) ; vue `v_kpi_ep_agent_medecin_conseil`
(`sql/analytics/006_dim_agents.sql`) qui filtre à la lecture. Guide et graphique Superset à refaire
pointer vers cette vue plutôt que `kpi_ententes_prealables_agent_jour` directement (voir
`docs/guides/etape6b_superset_ententes.md`, section 2.5).

**Déploiement, pas encore exécuté au moment d'écrire ceci** : comme au 2026-09-15, ajouter une table à
`table.include.list` sur un connecteur déjà en `RUNNING` ne déclenche pas d'instantané automatique
pour cette table. Reproduire la même procédure : `PUT .../stop` → `DELETE .../offsets` → `PUT
.../resume` (API KIP-875) pour forcer un instantané complet de `TB_REF_AGENTS`, puis appliquer
`sql/analytics/006_dim_agents.sql` et `GRANT SELECT` à `dprest_lecture`, puis annuler et resoumettre le
job Flink avec le fichier mis à jour.

**3. Manque plus général, corrigé en même temps** : le job Flink actuel n'isolait plus aucune anomalie
de qualité (COALESCE silencieux vers 0/'(inconnu)' partout) — le mécanisme de quarantaine du tout
premier prototype (`flink/sql/pipeline_kpi_hebdo.sql`) n'avait pas été repris dans le job final,
contrairement à la règle de `CLAUDE.md`. Nouvelle table `qualite_anomalies`
(`sql/analytics/005_qualite_anomalies.sql`) et trois détections ajoutées (prestation, facture, entente
préalable) — voir `docs/guides/etape6c_superset_qualite.md` pour le dashboard associé. Décision de
gouvernance : ce dashboard ne va **pas** dans les permissions du rôle Gamma (compte `dprest`) — lignes
brutes à but de diagnostic technique, pas un KPI DPREST.

**4. `dim_agents` recréée deux fois le même jour** — première version oubliait `AGENT_NOM`/
`AGENT_PRENOMS`, pourtant présents sur `TB_REF_AGENTS` (visibles dans le log d'instantané Debezium dès
la première snapshot). Le graphique Superset n'affichait alors que des codes agents, pas des noms —
signalé par l'utilisateur en repérant l'absence de la colonne dans le panneau de configuration du
chart. Corrigé par `ALTER TABLE` (colonnes ajoutées, pas de perte des 20 lignes déjà présentes) et vue
`v_kpi_ep_agent_medecin_conseil` refaite pour exposer `agent_nom_complet` (même principe que
`v_professionnels_sante`). Job Flink annulé proprement puis resoumis avec `dim_agents_src` étendue.

**5. KPI 19, deuxième correction le même jour : unité jour → heure.** Après la correction de formule
(point 1 ci-dessus), le délai moyen restait obstinément à 0,00 — pas un nouveau bug, une vérification
en base (`SELECT delai_moyen_jours, statut_code, SUM(nombre_ententes) ... GROUP BY 1,2`) a montré que
13 339 EP sur 13 341 ont un délai de traitement de très exactement 0 jour plein : le simulateur répond
en quelques minutes à quelques heures, sans jamais franchir minuit, donc `TIMESTAMPDIFF(DAY, ...)` vaut
structurellement 0 presque partout. H3 de `docs/kpi.md` (« délai en jours pleins ») était donc trop
grossière dès le départ pour un pipeline temps réel, indépendamment de tout bug de calcul.

Corrigé en heures (`TIMESTAMPDIFF(HOUR, ...)`), pas en minutes/secondes : suffisant pour distinguer un
traitement instantané d'un traitement qui traîne, sans viser une précision que rien dans le besoin
métier ne justifie. Renommage de colonne `delai_moyen_jours` → `delai_moyen_heures` plutôt que garder
l'ancien nom avec une nouvelle unité (aurait été trompeur pour quiconque relit le schéma plus tard) :
`sql/analytics/007_kpi_ep_delai_heures.sql` (`ALTER TABLE RENAME COLUMN` + vue mensuelle refaite),
`flink/sql/kpi_prestations.sql` (source, agrégat), `docs/kpi.md` (H3 réécrite : les colonnes source
portent en réalité un horodatage complet, pas une `DATE` sans heure comme l'affirmait H3 à tort — la
mesure en jours n'a jamais été une contrainte du schéma, seulement un choix initial trop prudent).

**6. `qualite_anomalies` étendue : colonne `famille`, et détection sur l'identité des assurés.**
L'utilisateur a demandé à pouvoir dire « telle anomalie est sur les dates, sur les noms » — `domaine`
seul ne le permettait pas (un même domaine, ex. `prestation`, mélange des anomalies de nature
différente). Ajout de la colonne `famille` (`MONTANTS`, `DATES`, `IDENTITE`, `QUANTITES`, `FORMAT`,
`REFERENTIEL`), reprise telle quelle du dict `FAMILLES` de
`simulateur_V5/anomalies/catalogue.py` — migration `sql/analytics/008_qualite_anomalies_famille.sql`.

Répondre à « sur les noms » exigeait aussi de couvrir les anomalies d'identité du catalogue
(`NUMERO_SECU_INVALIDE`, `DATE_NAISSANCE_ABERRANTE`, `CHAMP_OBLIGATOIRE_VIDE`, `ENCODAGE_CASSE`,
`TENTATIVE_INJECTION`), jusqu'ici hors de portée car elles ciblent `TB_REF_ASSURES`, absente du
connecteur Debezium (décision consciemment reportée le 2026-09-17 plus haut, § anomalies). Décision
prise ce même jour de l'ajouter : `TB_REF_ASSURES` intégrée à `table.include.list`
(`connectors/debezium-postgres-json.json`) et à la publication logique PostgreSQL
(`ALTER PUBLICATION dbz_publication_json ADD TABLE`), nouvelle source `dim_assures_src` dans
`flink/sql/kpi_prestations.sql`. Seules les colonnes utiles à la détection transitent par le pipeline
(numéro de sécu, nom, date de naissance) — toutes synthétiques (conforme à `CLAUDE.md`), et cette
table de quarantaine reste réservée aux comptes techniques (jamais exposée au rôle Gamma). `DOUBLON_EXACT`
/ `DOUBLON_APPROCHANT` restent hors de portée : leur détection exige de comparer une fiche aux autres
déjà connues (auto-jointure), pas une simple `CASE WHEN` ligne à ligne.

Deux pièges rencontrés en déployant : `LENGTH()` n'existe pas en Flink SQL (`CHAR_LENGTH()` seulement),
et `INTERVAL '100' YEAR` dépasse la précision par défaut de Calcite (`YEAR(2)`, deux chiffres) — corrigé
en `INTERVAL '100' YEAR(3)`. Un bug de données a aussi été introduit puis corrigé le jour même : le
préfixe attendu du numéro de sécu a d'abord été codé `384` d'après une documentation du simulateur
(`docs/guides/alignement_donnees_libre_qualite.md`) qui contenait une coquille — vérification en base
(`seed/identifiants.py`, puis un échantillon de `TB_REF_ASSURES`) a montré que le vrai préfixe est
`394`. Conséquence concrète : la première version du job a classé les 100 000 assurés comme
`NUMERO_SECU_INVALIDE` (faux positifs à 100 %), détecté immédiatement au contrôle de volume
(`GROUP BY domaine, motif_anomalie` — un taux d'anomalie de 100 % sur toute une table est le signe
qu'on a un bug de détection, pas 100 000 vraies anomalies). Corrigé, lignes fautives supprimées
(`DELETE ... WHERE domaine = 'assure' AND motif_anomalie = 'NUMERO_SECU_INVALIDE'`), job resoumis :
`qualite_anomalies` est repassée à 0 ligne, cohérent avec le module d'injection du simulateur toujours
désactivé (`TB_CONFIG_ANOMALIES.ENABLED = false`).

Incident sans rapport rencontré pendant ce déploiement : la stack a dû être relancée en cours de route
(`podman machine stop`/`start`, encore un cas de veille moderne Windows cassant le réseau Windows↔VM —
voir l'incident similaire plus haut), avec au passage un échec transitoire `crun: ... OCI runtime error`
sur 3 conteneurs au redémarrage (symptôme connu de scope systemd résiduel après une coupure brutale) —
résolu par une simple répétition de `podman compose up -d`, sans intervention plus profonde nécessaire.

## 2026-09-21 — Renforcement de la sécurité (étapes 7b à 7e), hors TLS

**Décision** : ajout de quatre dispositifs, un guide chacun (`docs/guides/etape7b_` à `etape7e_`) :
sauvegarde chiffrée avec test de restauration (7b), journal d'audit archivé avec chaîne de hachage (7c),
contrôle d'accès par rôle sur la base analytique (7d), authentification Kafka SASL/SCRAM et ACL par topic (7e).
Le TLS interne est explicitement exclu du périmètre (écart assumé, à citer au chapitre 6).

**Pseudonymisation écartée** : la DPREST a besoin des identités réelles ; la protection repose donc sur la
séparation des rôles (7d) et non sur le hachage. Aujourd'hui les tops d'assurés n'exposent qu'un UUID.

**Pièges rencontrés** :
- PowerShell 5.1 traite tout message stderr d'un exécutable natif (NOTICE psql, info gpg) comme une erreur
  fatale avec `$ErrorActionPreference = "Stop"` : les scripts utilisent `Continue` et testent `$LASTEXITCODE`.
- `flink-sql-connector-kafka` embarque un `kafka-clients` relocalisé : le module JAAS doit être
  `org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule`.
- Git-Bash convertit les chemins `/opt/...` passés à `podman exec` : `MSYS_NO_PATHCONV=1`.
- Le compte Flink était le superutilisateur `dprest` : remplacé par `flink_writer` (12 tables, droits DML).

**Vérifié** : 8 tests (`tests/test_securite.py`), test de restauration conforme (13 tables, 0 % d'écart),
concordance exacte source/analytique après rejeu complet du job (2 118 prestations, 2 119 factures).

**Retour arrière Kafka** : fichiers d'origine dans `backups/pre-sasl/` (voir guide 7e).

## 2026-09-21 — Famille C : prescriptions et pathologies (KPI 25, 27, 30 à 33)

**Besoin** : le SGD veut compter les médicaments prescrits, et les médicaments/pathologies rattachés aux
ententes préalables. `TB_FACTURES_PRESCRIPTIONS` et `TB_FACTURES_PATHOLOGIES` étaient jusque-là hors périmètre
CDC (dictionnaire de données). KPI retenus après exploration de la source : 25, 27, 30, 31, 32, 33 (les KPI 26,
28 et 29 — taux de prescription, prescriptions par DCI, coût théorique — ont été écartés par le SGD).

**Exploration avant conception** (source au 2026-09-21, 183 725 factures) : une facture porte au plus une
prescription ; `PRESCRIPTION_QUANTITE` vaut toujours 1 ; les 918 médicaments prescrits sont tous au
référentiel ; les 106 392 ententes portent un `FACTURE_NUMERO` existant, sans facture partagée entre deux
ententes ; aucune entente n'a plusieurs statuts. **Point structurant** : les ententes n'ont **pas** de
médicaments propres (`TB_ENTENTES_PREALABLES_PRESTATIONS` vide, `..._ACTES_MEDICAUX` contient des actes) ; le
rattachement passe par la facture (hypothèse H7, à confirmer avec le métier).

**Décision — pas de jointure d'état dans Flink** : Flink agrège (KPI 25, 27, 30) ou recopie 1:1 (KPI 31 à 33) ;
les jointures entente -> facture -> prescription se font à la lecture, dans des vues PostgreSQL
(`sql/analytics/010_kpi_clinique.sql`). Raison : le TaskManager a déjà dépassé sa mémoire à cause de jointures
d'état (2026-09-11 et 2026-09-17). Alternative écartée : joindre `ep_enrichies` aux prescriptions dans Flink
(un opérateur d'état de plus sur ~100 000 ententes).

**Décision — séparation des accès (H10)** : trois niveaux. Dimensions lisibles par la DPREST ; agrégats et
faits (numéro de facture, jamais l'assuré) réservés au SGD (`role_qualite_nominatif`) ; vues masquées
(regroupements < 5 supprimés) seules exposées à la DPREST (`role_kpi_lecture`). Données de santé sensibles
(loi n°2013-450). Limite assumée : les vues masquées portent sur toute la période ; une période libre n'est
disponible que côté SGD.

**Correction découverte pendant la vérification des droits** : un droit par défaut posé plus tôt le même jour
(`ALTER DEFAULT PRIVILEGES FOR ROLE dprest ... GRANT SELECT ON TABLES TO dprest_lecture`) donnait à la DPREST
un `SELECT` sur toute nouvelle table, `fait_prescriptions` comprise, ce qui contournait le masquage et
contredisait l'étape 7d. Retiré et révoqué explicitement dans `010` ; vérifié avec une vraie connexion
(`permission denied` sur les tables sensibles, vues masquées lisibles, anciens KPI intacts).

**Autre écart comblé** : `REPLICA IDENTITY FULL` n'était dans aucune migration (cause de la panne Flink du
2026-09-21 après recréation de la base). Désormais versionné : `sql/source/001_replica_identity_cdc.sql`
(18 tables), avec un test qui échoue si une table suivie par CDC n'y figure pas.

**Vérifié** : logique des vues sur un jeu de test contrôlé (66,7 %, 5 médicaments, 9 et 6 pathologies,
regroupements < 5 masqués, « sans réponse » géré), transaction annulée ; `EXPLAIN STATEMENT SET` du job Flink
complet (9 nouveaux sinks planifiés, rien soumis) ; 12 tests de cohérence (`tests/test_famille_c.py`), dont un
vérifié par mutation ; requêtes de contrôle source/analytique ajoutées (`evaluation/`, sections 20 à 25).

**Non fait à ce stade** : déploiement (reset du connecteur, resoumission du job) — voir
`docs/guides/famille_c_deploiement.md` ; dashboard Superset « Clinique » ; lint `ruff` (non installé sur le poste).

**Risque à surveiller** : le snapshot ajoute ~490 000 lignes cliniques (125 325 prescriptions + 367 003
pathologies) aux ~100 000 assurés et ~180 000 factures déjà en état Flink. Si le TaskManager (2 304 Mo)
retombe en OutOfMemoryError : retirer d'abord `fait_pathologies` (la plus volumineuse), puis envisager
RocksDB ou plus de mémoire.

## 2026-09-21 (soir) — Flink : état sur RocksDB au lieu du tas JVM

**Constat** : le job `kpi-continu` tournait depuis 13 h 37 quand le SGD a lancé un remplissage antidaté de la
source. Il s'est arrêté à 17 h 21 sur `Heartbeat of TaskManager ... timed out` (conteneur non tué par
manque de mémoire, `OOMKilled=false` : le JVM ne répondait plus, typique d'un tas saturé). À ce moment la
source comptait ~345 000 factures ; elle a fini à 786 229 factures, 535 119 prescriptions,
**1 570 852 pathologies**, 456 250 ententes, 786 226 prestations. Les KPI de la base analytique sont restés
figés à 17 h 21 pendant environ une heure.

**Cause** : l'état de Flink (dédoublonnage CDC de chaque source, deux jointures, agrégats) est gardé en tas
JVM (HashMapStateBackend). Il croît avec le nombre de lignes source ; à 786 000 factures il dépasse les
~685 Mo de tas disponibles. Les ajouts de la famille C (~2,1 millions de lignes cliniques, surtout
pathologies) l'auraient aggravé (voir l'entrée précédente, « Risque à surveiller »).

**Décision** : `SET 'state.backend.type' = 'rocksdb'` dans `flink/sql/kpi_prestations.sql`, et
`taskmanager.memory.managed.fraction` de 0.1 à 0.4 dans `docker-compose.yml` (RocksDB utilise la mémoire
managée). Constaté après recréation du TaskManager : tas 685 Mo, mémoire managée 762 Mo, réseau 191 Mo.
RocksDB est inclus dans `flink-dist-1.19.1.jar` (vérifié), rien à installer. C'était l'alternative déjà
annoncée dans le commentaire du compose (« la mémoire managée sert surtout à RocksDB »).

**Alternatives écartées** : augmenter `taskmanager.memory.process.size` (la VM dispose de 7 Go, déjà
occupée par Kafka Connect ~1,8 Go, Kafka, Superset…) ; retirer `fait_pathologies` (règle le volume clinique,
pas celui de la famille A, déjà en cause avant les ajouts).

**Compromis** : RocksDB est plus lent qu'un accès mémoire et écrit dans `/tmp` du TaskManager. Pas de
checkpoint (inchangé) : l'état disparaît avec le TaskManager, le job repart de `earliest-offset`, sans effet
de bord (écriture en UPSERT). La reprise après incident est donc plus longue qu'avec peu de données, ce qui
est un point à mesurer au chapitre 7 (critère « reprise après incident »).

**Retour arrière** : remettre `0.1` dans le compose (`podman compose up -d --no-deps flink-taskmanager`)
et retirer le `SET` du fichier SQL.

**Aussi observé ce jour** : Kafka a été marqué `unhealthy` (129 échecs, code 125) et la connexion SSH à la VM
Podman s'est coupée à plusieurs reprises pendant la charge ; les deux se sont rétablis d'eux-mêmes
(Kafka `healthy` à 18 h 12). Cause non identifiée : à surveiller si cela se reproduit.

## 2026-09-22 (nuit) — Réduction du volume de la source, retour à l'état en tas, limites de capacité mesurées

**Contexte** : le SGD a rempli la source avec des données antidatées (786 229 factures sur 16 semaines de
49 000, 535 119 prescriptions, 1 570 852 pathologies, 456 250 ententes). Le pipeline, dimensionné pour ~60 000
à 300 000 factures, n'a pas pu les absorber. Chronologie et mesures ci-dessous.

**Mesures** (VM Podman de 7 Go, 4 processeurs, disque virtuel WSL2) :
- État en tas JVM à ~345 000 factures : arrêt du job à 17 h 21 (« Heartbeat of TaskManager timed out »).
- État RocksDB (passage du 2026-09-21 soir) : le job tient mais avance à **30-40 lignes/s** par flux, puis
  ~5 lignes/s après 70 min ; `kpi_prestations_jour` restait à 0 (les 4 jointures n'avaient rien émis). Cause
  mesurée : écriture synchrone de **6 ms** sur le disque de la VM (contre 6 µs en mémoire), attente disque de
  19 %, charge de 6 à 10 sur 4 processeurs. Durée extrapolée pour 786 000 factures : > 10 h.
- Dans les deux cas, la VM manquait de mémoire : swap de 2 Go épuisé, 1,2 Go disponible (relevé de 00 h 00).
  La machine hôte (15,6 Go) n'avait que 0,8 Go libre : agrandir la VM n'était pas possible.

**Décision 1 — réduire la source** (choix du SGD, option recommandée) : suppression des factures dont la date de
soins est antérieure au **2026-09-07**, avec leurs prestations, statuts, prescriptions, pathologies et les
ententes rattachées. Résultat : 100 229 factures, 57 928 ententes (686 000 et 398 322 supprimées). Sauvegarde
préalable complète : `backups/echo_db_avant_purge_20260921_190243.dump`. Garde-fous : transaction unique,
effectifs contrôlés avant et après, annulation intégrale sinon.
**Contrepartie** : aucun mois calendaire clos dans les données (07-21 septembre) : la vue certifiée
`v_kpi_ententes_prealables_mois` reste vide.

**Piège découvert : deux clés étrangères sans index.** `TB_ENTENTES_PREALABLES.FACTURE_NUMERO` et
`TB_FACTURES.ENTENTE_PREALABLE_ID` (références croisées, sans suppression en cascade) n'ont pas d'index : chaque
ligne supprimée forçait une lecture complète de la table liée. Première tentative annulée après 11 min ; la
seconde, avec deux index **temporaires** créés et supprimés dans la même transaction, a pris 15 min 32 s pour
la dernière instruction (~1 million de contrôles de clés). Le schéma de la source est resté inchangé. À
ajouter aux migrations du simulateur si une purge devait être répétée.

**Décision 2 — retour à l'état en tas JVM** (`SET 'state.backend.type' = 'hashmap'`,
`taskmanager.memory.managed.fraction` 0.1) avec un TaskManager de **3 840 Mo** (tas 2 416 Mo). À 100 000
factures, l'état tient : rattrapage complet en **environ 10 minutes** (contre > 10 h sous RocksDB). Un premier
essai à 3 072 Mo (tas 1 836 Mo) est tombé à 91 % des pathologies sur un nouveau « Heartbeat timed out » : la VM
était en pénurie de mémoire (swap épuisé).

**Décision 3 — libérer la mémoire pendant le rattrapage** : arrêt temporaire d'AKHQ, Grafana, Prometheus,
blackbox et Kafka Connect (~1,2 Go ; aucune donnée ne change pendant ce temps). Mémoire disponible de 1,2 Go à
3,9 Go. Kafka Connect, Prometheus, blackbox et Grafana ont été redémarrés ensuite ; **AKHQ est resté arrêté**
(288 Mo, inutile au flux) : `podman start pipeline_temps_reel-akhq-1` pour le remettre.

**Délais RPC allongés** (`pekko.ask.timeout: 60 s`, `heartbeat.timeout: 180000`) : conservés dans le compose.
Le premier essai RocksDB avait échoué au déploiement (« Cannot deploy task ... Ask timed out after 10000 ms »).
Attention : les commentaires placés dans le bloc `FLINK_PROPERTIES` sont interprétés par l'entrée du conteneur
comme des paramètres et créent des clés parasites dans `config.yaml` (sans effet, mais à sortir du bloc).

**Nettoyage des tables dérivées** : les tables analytiques (14 tables KPI, `fait_*`, `qualite_anomalies`)
contenaient des lignes périmées des essais précédents (jours antérieurs au 2026-09-07, factures supprimées) que
Flink, qui écrit en UPSERT, ne supprime jamais. Elles ont été vidées job arrêté (sauvegarde préalable de la base
analytique) puis reconstruites en totalité depuis Kafka. Les dimensions n'ont pas été touchées.

**Incidents d'exploitation** : le script de démarrage a été lancé deux fois d'affilée (deux jobs identiques
`kpi-continu`, doublon annulé) ; la connexion à la VM se coupe de façon répétée sous charge (messages
« ssh handshake failed »), d'où des nouvelles tentatives automatiques dans mes commandes ; un suivi
automatique a pris à tort l'ancien job `FAILED`, encore listé, pour un échec du job en cours.

**Vérifié** (2026-09-22, ~00 h 10) : job `RUNNING` ; toutes les tables analytiques aux valeurs attendues
(prestations 100 226, factures 100 229, prescriptions 68 271, pathologies 200 430, ententes 57 928, `fait_*`
identiques) ; comparaison source / analytique des sections 2, 3, 4, 11 à 17 et 20 à 24 de `evaluation/` :
**identiques** (les écarts apparents des sections 15 et 16 sont des différences de présentation, montants et
effectifs égaux) ; section 25 : 0 doublon ; `qualite_anomalies` : 0 ligne (injection désactivée dans le simulateur).

**Limite de capacité (résultat pour le chapitre 7)** : sur ce poste (VM 7 Go, disque WSL2 à écriture synchrone de
6 ms), le pipeline traite environ **100 000 factures** en tas JVM (rattrapage en ~10 min) ; à 345 000 le tas ne
suffit plus, et RocksDB, seule alternative testée, est inutilisable (10 h+). Un passage à l'échelle demanderait
plus de mémoire et un disque à faible latence, non une modification du code.

**Retour arrière** : restaurer `backups/echo_db_avant_purge_20260921_190243.dump` (`pg_restore`) puis refaire
l'instantané (`docs/guides/famille_c_deploiement.md`) ; repasser à RocksDB : `state.backend.type` = `rocksdb` et
`managed.fraction` 0.4.
