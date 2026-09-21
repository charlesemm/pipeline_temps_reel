# Étape 7e — Kafka : authentification par composant et ACL

## Architecture

| Écouteur | Adresse | Protocole | Usage |
|---|---|---|---|
| `SASL` | `kafka:9092` | SASL_PLAINTEXT, SCRAM-SHA-512 | tous les conteneurs |
| `HOST` | `localhost:29092` (publié) | SASL_PLAINTEXT, SCRAM-SHA-512 | outils depuis Windows (utilisateur `admin`) |
| `LOCAL` | `localhost:9094` | PLAINTEXT | administration et trafic inter-broker, **joignable uniquement depuis le conteneur `kafka`** |
| `CONTROLLER` | `kafka:9093` | PLAINTEXT | quorum KRaft interne, non publié |

`allow.everyone.if.no.acl.found=false` : tout accès non autorisé par une ACL est refusé. Le principal anonyme
n'existe que sur `LOCAL` et `CONTROLLER`, qui ne sont pas atteignables depuis les autres conteneurs.

## Utilisateurs et ACL (`scripts/kafka-secure-setup.ps1`, idempotent)

| Utilisateur | Droits |
|---|---|
| `connect` | écrit `dprest-json.*` et `__debezium-heartbeat.*` ; gère `_connect-*` ; groupe `pipeline-connect` |
| `flink` | **lecture seule** de `dprest-json.*`, groupes `flink-kpi-*` |
| `akhq` | lecture seule de tous les topics (outil d'inspection SGD) |
| `schema-registry` | topic `_schemas` et son groupe |
| `admin` | super-utilisateur |

Mots de passe : `KAFKA_<NOM>_PASSWORD` dans `.env`. Injection : `docker-compose.yml` (Connect, Schema Registry,
AKHQ) et `start-stack.ps1` (Flink, placeholder `__FLINK_KAFKA_PASSWORD__`).

## Procédure de mise en service (ordre)

1. Arrêter le job Flink. 2. `podman compose up -d --no-deps kafka`. 3. `.\scripts\kafka-secure-setup.ps1`.
4. `podman compose up -d --no-deps schema-registry kafka-connect akhq`. 5. `.\scripts\start-stack.ps1`
(resoumet le job).

## Piège rencontré

Flink échouait avec `No LoginModule found for org.apache.kafka...ScramLoginModule` : `flink-sql-connector-kafka`
embarque un `kafka-clients` **relocalisé**. Le module JAAS des sources Flink doit s'écrire
`org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule`.

## Vérifié le 2026-09-21

Client anonyme sur `kafka:9092` : refusé. `flink` : lit `dprest-json.*`, ne peut ni écrire ni lire `_connect-*`.
Debezium `RUNNING`, Schema Registry sain, job Flink `RUNNING` et stable, lag du groupe `flink-kpi-prestations` à 0,
concordance source/analytique exacte. Tests : `pytest tests/test_securite.py`.

## Limites assumées

- **Pas de TLS** (exclu du périmètre) : SCRAM protège les mots de passe (échange par défi), mais le contenu des
  messages, qui inclut des données de santé synthétiques, circule en clair sur le réseau Podman.
- **AKHQ lit tous les topics**, donc les événements bruts, dont les référentiels nominatifs. À réserver au SGD.
- **Secrets en variables d'environnement** des conteneurs (visibles par `podman inspect`) : en production,
  utiliser un gestionnaire de secrets.
- **Retour arrière** : restaurer `backups/pre-sasl/` (`docker-compose.yml`, `kpi_prestations.sql`,
  `akhq/application.yml`, `start-stack.ps1`), puis `podman compose up -d --no-deps kafka schema-registry
  kafka-connect akhq` et resoumettre le job. Les données Kafka (volume `kafka_data`) ne sont pas modifiées.
