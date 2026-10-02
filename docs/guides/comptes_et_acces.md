# Comptes et accès — qui se connecte où

État vérifié le 2026-10-02, après le nettoyage des comptes, sur la stack en marche (catalogues `pg_roles`, utilisateurs Superset,
`docker-compose.yml`, `scripts/kafka-secure-setup.ps1`).

> **Aucun mot de passe dans ce document.** Chaque mot de passe est dans `.env` (non versionné),
> sous le nom de variable indiqué dans la colonne « Mot de passe ». Pour lire une valeur :
>
> ```powershell
> Select-String -Path .env -Pattern '^NOM_DE_LA_VARIABLE='
> ```

---

## 1. Lire d'abord : deux sortes de comptes

| Sorte | Qui le tape ? | Exemple |
|---|---|---|
| **Compte technique** (base de données, Kafka) | Un **programme**, automatiquement. Un humain ne le tape jamais, sauf l'administrateur. | Flink écrit dans la base avec `flink_writer` |
| **Compte web** | Un **humain**, dans son navigateur | Le SGD se connecte à Grafana avec `sgd_admin` |

Superset et Grafana apparaissent deux fois, et c'est normal : ce sont des **sites web** où un humain se
connecte (compte web), **et** des **programmes** qui vont lire la base analytique (compte technique).

```
 Humain ──(compte web)──> Superset ──(compte technique dprest_lecture)──> Base analytique
```

---

## 2. Vue d'ensemble

```
 ┌──────────────────────┐
 │ BASE SOURCE  echo_db │  <── monitoring_ro (Grafana)      <── sgd_admin (SGD)
 └──────────┬───────────┘
            │ echo
            ▼
 ┌──────────────────────┐
 │ Debezium (Connect)   │
 └──────────┬───────────┘
            │ connect
            ▼
 ┌──────────────────────┐
 │ KAFKA                │  <── schema-registry   <── akhq (AKHQ)   <── admin
 └──────────┬───────────┘
            │ flink
            ▼
 ┌──────────────────────┐
 │ FLINK                │
 └──────────┬───────────┘
            │ flink_writer
            ▼
 ┌──────────────────────────────┐
 │ BASE ANALYTIQUE              │  <── dprest_lecture (Superset, Grafana)
 │ dprest_analytics             │  <── sgd_admin (SGD, humain)
 └──────────────────────────────┘  <── dprest (administration, scripts)
```

---

## 3. Base analytique `dprest_analytics`

- Depuis Windows : `localhost:15433` (ou IP de la VM) — depuis un conteneur : `postgres-analytics:5432`

| Compte | Qui l'utilise | Ce qu'il peut faire | Mot de passe (`.env`) |
|---|---|---|---|
| `dprest` | Création de la base, scripts d'admin (`apply-roles-analytics.ps1`, sauvegardes) | **Tout** (superutilisateur) | `ANALYTICS_PASSWORD` |
| `sgd_admin` | **Humain SGD** : DBeaver, scripts `evaluation/` | **Tout** (membre de `dprest`) | `SGD_ADMIN_DB_PASSWORD` |
| `flink_writer` | Programme : job Flink `kpi-continu` | Lire/écrire les 12 tables KPI, dimensions, quarantaine | `FLINK_WRITER_PASSWORD` |
| `dprest_lecture` | Programmes : Superset (connexion « PostgreSQL analytique (dprest_analytics) ») et Grafana | Lire les KPI, **sans** `donnee_brute` | `ANALYTICS_RO_PASSWORD` |

**Réponse courte à « qui accède à la base analytique ? »**
- Humains : **`sgd_admin` uniquement** (`dprest` en secours).
- Programmes : `flink_writer` (écrit), `dprest_lecture` (lit pour Superset et Grafana).
- Données nominatives (`donnee_brute`, vues `v_anomalies_*`) : `sgd_admin` uniquement, dans DBeaver. Le groupe
  `role_qualite_nominatif` est conservé, sans compte de connexion.
- DPREST : **jamais en direct**. Les agents passent par le site Superset (section 7).

---

## 4. Base source `echo_db` (simulateur_V5)

- Depuis Windows : `localhost:5433` — depuis un conteneur : `postgres:5432`

| Compte | Qui l'utilise | Ce qu'il peut faire | Mot de passe |
|---|---|---|---|
| `echo` | Programmes : application simulateur_V5, Debezium (lecture du journal CDC) | **Tout** (superutilisateur + réplication) | `connectors/secrets/db.properties` (clé `source.db.password`) |
| `monitoring_ro` | Programme : Grafana (retard des slots de réplication) | Vues de supervision seulement, aucune table métier | `SOURCE_RO_PASSWORD` |
| `sgd_admin` | **Humain SGD** : scripts `evaluation/` | Lire toutes les tables + supervision | `SGD_ADMIN_DB_PASSWORD` |

---

## 5. Kafka

- Depuis un conteneur : `kafka:9092` — depuis Windows : `localhost:29092` — authentification SASL/SCRAM-SHA-512

| Compte | Qui l'utilise | Ce qu'il peut faire | Mot de passe (`.env`) |
|---|---|---|---|
| `admin` | Administration Kafka | **Tout** (super-utilisateur Kafka) | `KAFKA_ADMIN_PASSWORD` |
| `connect` | Programme : Kafka Connect / Debezium | Écrire les topics `dprest-json.*` | `KAFKA_CONNECT_PASSWORD` |
| `flink` | Programme : job Flink | Lire les topics `dprest-json.*` | `KAFKA_FLINK_PASSWORD` |
| `akhq` | Programme : interface AKHQ | Lire tous les topics (consultation) | `KAFKA_AKHQ_PASSWORD` |
| `schema-registry` | Programme : Schema Registry | Topic `_schemas` | `KAFKA_SCHEMA_REGISTRY_PASSWORD` |

`ANONYMOUS` (sans mot de passe) est super-utilisateur mais n'existe que sur l'écouteur interne
`localhost:9094`, joignable uniquement depuis l'intérieur du conteneur `kafka`.

---

## 6. Base interne de Superset `superset` (non publiée hors Docker)

| Compte | Qui l'utilise | Ce qu'il peut faire | Mot de passe (`.env`) |
|---|---|---|---|
| `superset` | Programme : Superset (ses tableaux, ses utilisateurs, son journal) | **Tout** (superutilisateur) | `SUPERSET_DB_PASSWORD` |
| `superset_ro` | Programme : Grafana (journal d'activité Superset) | Lecture | `SUPERSET_RO_PASSWORD` |

---

## 7. Comptes web (humains)

| Outil | Adresse | Compte | Pour qui | Mot de passe |
|---|---|---|---|---|
| Superset | `https://<IP>:8443` | `sgd_admin` (rôle Admin) | SGD | `SUPERSET_ADMIN_PASSWORD` |
| Superset | `https://<IP>:8443` | `dprest_lecteur` (rôles `DPREST_Consultation` + `DPREST_Lecture`) | DPREST | Hors `.env` : `secrets/identifiants_comptes.md` |
| Grafana | `https://<IP>:3443` | `sgd_admin` (seul compte, Admin ; défini dans `docker-compose.yml`) | SGD | `GRAFANA_ADMIN_PASSWORD` |
| AKHQ (Kafka) | `http://<IP>:8085` | `AKHQ_ADMIN_USER` (`sgd_admin`) | SGD | En clair hors projet ; `.env` ne contient que l'empreinte `AKHQ_ADMIN_PASSWORD_SHA256` |
| Flink (interface) | `http://<IP>:8082` | `FLINK_ADMIN_USER` (`sgd_admin`) | SGD | En clair hors projet ; `.env` ne contient que l'empreinte `FLINK_ADMIN_PASSWORD_APR1` |

`<IP>` : adresse affichée par `scripts/start-stack.ps1` en fin de démarrage.

Un compte Superset DPREST doit avoir **deux rôles** : `DPREST_Consultation` (voir les tableaux de bord et les
graphiques) **et** `DPREST_Lecture` (droit de lire les jeux de données). Avec un seul des deux, l'utilisateur
ne voit rien.

`dprest_lecteur` n'existe que **dans Superset** (le rôle PostgreSQL homonyme, orphelin, a été supprimé le
2026-10-02).

---

## 8. Règles qui maintiennent ce modèle

- **Superset** : le compte Admin créé au démarrage est `SUPERSET_ADMIN_USER=sgd_admin` (`.env`). Remettre `admin`
  recréerait un second administrateur à chaque `superset-init`.
- **Grafana** n'a pas de volume sur `/var/lib/grafana` : un compte créé dans l'interface disparaît à la recréation
  du conteneur. Le seul compte est défini dans `docker-compose.yml` (`GF_SECURITY_ADMIN_USER: sgd_admin`).
- **Un compte DPREST n'est jamais propriétaire** d'un objet Superset : un propriétaire peut modifier l'objet et voit
  le jeu de données hors de ses rôles. Contrôle et correction : `superset/retirer_proprietes_dprest.py` (idempotent).
- **Jamais de compte humain dans une connexion Superset ou Grafana.** Si un tableau SGD nominatif est créé un jour,
  créer un compte technique dédié en lecture seule, membre de `role_qualite_nominatif` — pas `sgd_admin`.
- Tests : `pytest tests/test_securite.py` (vérifie entre autres l'absence de `sgd_qualite` et `dprest_lecteur` dans
  PostgreSQL et qu'aucun objet Superset n'appartient à `dprest_lecteur`).

## 9. Écarts restants (chantier suivant)

| # | Constat | Risque | Action proposée |
|---|---|---|---|
| 1 | Ports publiés sans authentification : Kafka Connect 8083, Schema Registry 8081, Prometheus 9091, Mailpit 8025 | Arrêt ou modification de la capture depuis le réseau | Publier sur `127.0.0.1` seulement |
| 2 | AKHQ 8085 et Flink 8082 en HTTP ; Kafka 29092 `SASL_PLAINTEXT` ; PostgreSQL 15433 sans TLS | Mots de passe et données lisibles sur le réseau | Reverse-proxy HTTPS ou `127.0.0.1` |
| 3 | Debezium lit la source avec `echo`, superutilisateur | Un connecteur compromis = base source compromise | Compte dédié `REPLICATION` + `SELECT` sur les tables suivies |
| 4 | Port 15433 ouvert à tous les comptes, y compris techniques | Un humain qui obtient `ANALYTICS_RO_PASSWORD` contourne Superset | Restreindre `pg_hba.conf` (comptes techniques depuis le réseau Docker seulement) |
| 5 | `pg_hba.conf` en `trust` pour les connexions locales au conteneur | Pas de mot de passe depuis l'intérieur du conteneur | Écart accepté pour le simulateur, à citer au chapitre 7 |
