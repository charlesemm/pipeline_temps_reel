-- ═══════════════════════════════════════════════════════════════
-- Étape 4 — KPI « prestations » écrits dans la base analytique
--
-- Définitions : docs/kpi.md (famille A, KPI 1-15, 23-24).
-- Schéma cible : sql/analytics/001_schema.sql, 002_kpi_passages_assures.sql,
-- et 004_dim_referentiels.sql (dimensions centres/praticiens/localités,
-- ajoutées le 2026-09-15 pour afficher des noms plutôt que des codes
-- dans Superset).
--
-- Choix de conception : AGRÉGAT CONTINU (GROUP BY sans fenêtre).
-- Flink maintient les compteurs en état et réécrit la ligne du jour
-- concerné à chaque nouvel événement — le tableau de bord est donc
-- temps réel. Semaine, mois et plages libres se dérivent ensuite par
-- simple regroupement SQL sur la table journalière (voir docs/kpi.md).
--
-- Pas de WATERMARK ici : un agrégat continu n'a pas besoin de savoir
-- quand une période est « terminée », puisqu'il ne clôture rien. Ça
-- supprime toute la complexité du temps événementiel, réservée aux
-- KPI mensuels certifiés (fichier kpi_ententes_prealables_mois.sql).
-- ═══════════════════════════════════════════════════════════════

SET 'table.local-time-zone' = 'UTC';
-- Nom lisible dans l'UI Flink, utile pour la supervision (étape 5).
-- Couvre désormais les deux familles de KPI (prestations + ententes
-- préalables) : un seul job, un seul état partagé (voir plus bas).
SET 'pipeline.name' = 'kpi-continu';

-- État sur disque (RocksDB) plutôt qu'en tas JVM — décision du 2026-09-21.
-- La source est passée à ~790 000 factures, ~535 000 prescriptions et
-- ~1,57 million de pathologies (remplissage antidaté du SGD) : l'état en
-- mémoire (HashMap, choix d'origine) ne tenait plus et le TaskManager a fini
-- en « Heartbeat timed out » (arrêt du job à 17 h 21, docs/decisions.md).
-- Compagnon : taskmanager.memory.managed.fraction passé de 0.1 à 0.4 dans
-- docker-compose.yml, car c'est cette mémoire « managée » que RocksDB utilise.
-- Pas de checkpoint (comme avant) : l'état disparaît avec le TaskManager, le
-- job repart de earliest-offset, ce qui reste sans effet de bord (UPSERT).
-- RETOUR à l'état en tas JVM (nuit du 2026-09-21) : mesuré sur ce poste, RocksDB
-- tombait à ~5 lignes/s après 70 min (disque de la VM : écriture synchrone de
-- 6 ms) et les jointures n'avaient rien émis. La source a été ramenée à ~100 000
-- factures (elle ne tenait pas en tas à 345 000) ; voir docs/decisions.md.
SET 'state.backend.type' = 'hashmap';

-- Étape 7e : les sources Kafka s'authentifient en SASL/SCRAM (utilisateur
-- « flink », lecture seule de dprest-json.*). Le module JAAS porte le nom
-- RELOCALISÉ du kafka-clients embarqué dans flink-sql-connector-kafka
-- (org.apache.flink.kafka.shaded...) : avec le nom standard, Flink échoue
-- avec « No LoginModule found for ...ScramLoginModule ».

-- Debezium garantit une livraison « au moins une fois » : après un
-- incident, sa tâche repart du dernier offset validé et RÉÉMET des
-- événements déjà publiés. Constaté le 2026-09-10 (48 prestations
-- comptées deux fois). Cette option fait dédoublonner chaque source
-- par sa PRIMARY KEY avant tout calcul (opérateur ChangelogNormalize).
-- Voir docs/decisions.md.
SET 'table.exec.source.cdc-events-duplicate' = 'true';

-- ── Source 1 : les lignes de prestation ─────────────────────────
-- Rappel (voir docs/decisions.md) : les colonnes physiques doivent
-- porter EXACTEMENT le nom du champ JSON — Flink fait la
-- correspondance par nom, et une colonne mal nommée vaut NULL
-- silencieusement sur 100 % des lignes.
-- Colonnes ajoutées le 2026-09-17 (QUANTITE_PRESCRITE/SERVIE,
-- TAUX_REMBOURSEMENT) : nécessaires pour détecter les anomalies
-- QUANTITE_EXCESSIVE, QUANTITE_NULLE et MONTANT_HORS_BAREME du
-- catalogue simulateur_V5/anomalies/catalogue.py — voir plus bas.
CREATE TABLE prestations_src (
    `FACTURE_NUMERO`                STRING NOT NULL,
    `PRESTATION_CODE`               STRING NOT NULL,
    `PROFESSIONNEL_SANTE_CODE`      STRING,
    `PRESTATION_MONTANT_DEPENSE`    DOUBLE,
    `PRESTATION_MONTANT_RQ`         DOUBLE,
    `PRESTATION_MONTANT_ASSURE`     DOUBLE,
    `PRESTATION_QUANTITE_PRESCRITE` DOUBLE,
    `PRESTATION_QUANTITE_SERVIE`    DOUBLE,
    `PRESTATION_TAUX_REMBOURSEMENT` DOUBLE,
    PRIMARY KEY (`FACTURE_NUMERO`, `PRESTATION_CODE`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dprest-json.public.TB_FACTURES_PRESTATIONS',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.mechanism' = 'SCRAM-SHA-512',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="flink" password="__FLINK_KAFKA_PASSWORD__";',
    'properties.group.id' = 'flink-kpi-prestations',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- ── Source 2 : l'en-tête de facture (dimensions + date de soins) ──
-- FACTURE_DATE_SOINS arrive en entier : nombre de jours depuis
-- 1970-01-01 (type Debezium io.debezium.time.Date). Converti en DATE
-- par la colonne calculée `jour_soins` — formule vérifiée en amont
-- (20703 -> 2026-09-07).
-- Colonnes ajoutées le 2026-09-17 : `CENTRE_SANTE_TYPE_CODE` (détection
-- TYPE_CENTRE_INCONNU) et `DATE_CREATION` (détection DATE_ANTIDATEE /
-- DATE_SOINS_FUTURE — la date de soins se compare à la date de saisie,
-- pas à `now()`, sinon un simple rejeu Kafka depuis `earliest-offset`
-- ferait apparaître comme « future » une facture ancienne). Même
-- format ISO 8601 que `ENTENTE_PREALABLE_DATE_DEBUT` (voir ep_src) :
-- `DATE_CREATION` est aussi un TIMESTAMPTZ côté source.
CREATE TABLE factures_src (
    `FACTURE_NUMERO`                STRING NOT NULL,
    `TYPE_FACTURE_CODE`             STRING,
    `REGIME_CODE`                   STRING,
    `CENTRE_SANTE_CODE`             STRING,
    `CENTRE_SANTE_TYPE_CODE`        STRING,
    `CENTRE_SANTE_TYPE_LIBELLE`     STRING,
    `PERSONNE_UUID`                 STRING,
    `FACTURE_DATE_SOINS`            INT,
    `DATE_CREATION`                 STRING,
    `jour_soins` AS CAST(
        TO_TIMESTAMP_LTZ(CAST(`FACTURE_DATE_SOINS` AS BIGINT) * 86400000, 3) AS DATE
    ),
    `date_creation_facture` AS TO_TIMESTAMP(
        REPLACE(SUBSTRING(`DATE_CREATION`, 1, 23), 'T', ' '), 'yyyy-MM-dd HH:mm:ss.SSS'
    ),
    PRIMARY KEY (`FACTURE_NUMERO`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dprest-json.public.TB_FACTURES',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.mechanism' = 'SCRAM-SHA-512',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="flink" password="__FLINK_KAFKA_PASSWORD__";',
    'properties.group.id' = 'flink-kpi-factures',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- ── Source 3 : ententes préalables (en-tête de la demande) ───────
-- ENTENTE_PREALABLE_DATE_DEBUT arrive en chaîne ISO 8601 avec 6
-- décimales ('2026-09-11T16:28:58.871991Z') — contrairement aux DATE
-- pures (entier, voir factures_src). Conversion en TIMESTAMP par
-- troncature à la milliseconde, formule identique à l'étape 3
-- (voir flink/sql/pipeline_kpi_hebdo.sql).
CREATE TABLE ep_src (
    `ENTENTE_PREALABLE_ID`          INT NOT NULL,
    -- Ajoutée le 2026-09-21 (famille C, KPI 31 à 33) : clé de rattachement
    -- entente -> facture -> prescriptions/pathologies (H7 de docs/kpi.md).
    -- Aucune jointure d'état ici : elle sert seulement à alimenter fait_entente_facture.
    `FACTURE_NUMERO`                STRING,
    `TYPE_DEMANDE_CODE`             STRING,
    `ENTENTE_PREALABLE_DATE_DEBUT`  STRING,
    `event_time` AS TO_TIMESTAMP(
        REPLACE(SUBSTRING(`ENTENTE_PREALABLE_DATE_DEBUT`, 1, 23), 'T', ' '), 'yyyy-MM-dd HH:mm:ss.SSS'
    ),
    PRIMARY KEY (`ENTENTE_PREALABLE_ID`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dprest-json.public.TB_ENTENTES_PREALABLES',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.mechanism' = 'SCRAM-SHA-512',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="flink" password="__FLINK_KAFKA_PASSWORD__";',
    'properties.group.id' = 'flink-kpi-ep',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- ── Source 4 : statut de la demande ───────────────────────────────
-- Clé réelle côté source : (ENTENTE_PREALABLE_ID, STATUT_CODE,
-- STATUT_DATE_DEBUT) — une EP peut en théorie changer de statut
-- plusieurs fois. Ici la clé Flink est réduite à ENTENTE_PREALABLE_ID
-- seul, sous l'hypothèse H5 de docs/kpi.md (« statuts terminaux, un
-- seul par EP ») : confirmé sur les données actuelles (400 EP, 400
-- statuts, jamais plus d'un). Si cette hypothèse cessait d'être vraie,
-- cette table garderait silencieusement le dernier statut écrit pour
-- l'EP concernée — à surveiller si le générateur évolue.
CREATE TABLE ep_statuts_src (
    `ENTENTE_PREALABLE_ID`  INT NOT NULL,
    `STATUT_CODE`           STRING,
    `STATUT_DATE_DEBUT`     STRING,
    `AGENT_CODE`            STRING,
    `event_time` AS TO_TIMESTAMP(
        REPLACE(SUBSTRING(`STATUT_DATE_DEBUT`, 1, 23), 'T', ' '), 'yyyy-MM-dd HH:mm:ss.SSS'
    ),
    PRIMARY KEY (`ENTENTE_PREALABLE_ID`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dprest-json.public.TB_ENTENTES_PREALABLES_STATUTS',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.mechanism' = 'SCRAM-SHA-512',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="flink" password="__FLINK_KAFKA_PASSWORD__";',
    'properties.group.id' = 'flink-kpi-ep-statuts',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- ── Source 5 : actes médicaux liés à l'EP (montant engagé, KPI 22) ─
-- Clé réelle et composite : plusieurs actes possibles par EP.
CREATE TABLE ep_actes_src (
    `ENTENTE_PREALABLE_ID`           INT NOT NULL,
    `ACTE_MEDICAL_CODE`              STRING NOT NULL,
    `ACTE_MEDICAL_MONTANT_CMU`       DOUBLE,
    PRIMARY KEY (`ENTENTE_PREALABLE_ID`, `ACTE_MEDICAL_CODE`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dprest-json.public.TB_ENTENTES_PREALABLES_ACTES_MEDICAUX',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.mechanism' = 'SCRAM-SHA-512',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="flink" password="__FLINK_KAFKA_PASSWORD__";',
    'properties.group.id' = 'flink-kpi-ep-actes',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- ── Source 6 : référentiel centres de santé (dénomination) ───────
-- Ajouté le 2026-09-15 : noms de centres/praticiens/localités sur les
-- dashboards Superset. Pas de date/event_time ici : un référentiel n'a
-- pas de fenêtre temporelle, juste un état courant tenu à jour par UPSERT.
CREATE TABLE dim_centres_sante_src (
    `CENTRE_SANTE_CODE`                 STRING NOT NULL,
    `COLLECTIVITE_CODE`                 STRING,
    `TYPE_ETABLISSEMENT_SANITAIRE_CODE` STRING,
    `CENTRE_SANTE_DENOMINATION`         STRING NOT NULL,
    PRIMARY KEY (`CENTRE_SANTE_CODE`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dprest-json.public.TB_REF_CENTRES_SANTE',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.mechanism' = 'SCRAM-SHA-512',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="flink" password="__FLINK_KAFKA_PASSWORD__";',
    'properties.group.id' = 'flink-kpi-dim-centres',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- ── Source 7 : référentiel praticiens (nom, prénoms) ─────────────
CREATE TABLE dim_professionnels_src (
    `PROFESSIONNEL_SANTE_CODE`  STRING NOT NULL,
    `NOM`                       STRING NOT NULL,
    `PRENOMS`                   STRING NOT NULL,
    `TYPE_CODE`                 STRING,
    `STATUT`                    STRING,
    PRIMARY KEY (`PROFESSIONNEL_SANTE_CODE`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dprest-json.public.TB_REF_PROFESSIONNELS_SANTE',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.mechanism' = 'SCRAM-SHA-512',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="flink" password="__FLINK_KAFKA_PASSWORD__";',
    'properties.group.id' = 'flink-kpi-dim-professionnels',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- ── Source 8 : référentiel localités (nom, coordonnées) ──────────
-- LATITUDE/LONGITUDE arrivent en DOUBLE : le connecteur Debezium est
-- configuré en 'decimal.handling.mode': 'double' (voir
-- connectors/debezium-postgres-json.json), donc un NUMERIC PostgreSQL
-- traverse Kafka comme un nombre flottant, pas comme une chaîne à
-- décimales fixes — même traitement que les montants (montant_depense
-- et consorts, voir prestations_enrichies plus bas).
CREATE TABLE dim_collectivites_src (
    `COLLECTIVITE_CODE`          STRING NOT NULL,
    `COLLECTIVITE_DENOMINATION`  STRING NOT NULL,
    `COLLECTIVITE_LATITUDE`      DOUBLE,
    `COLLECTIVITE_LONGITUDE`     DOUBLE,
    PRIMARY KEY (`COLLECTIVITE_CODE`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dprest-json.public.TB_REF_COLLECTIVITES',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.mechanism' = 'SCRAM-SHA-512',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="flink" password="__FLINK_KAFKA_PASSWORD__";',
    'properties.group.id' = 'flink-kpi-dim-collectivites',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- ── Source 9 : référentiel agents (type, pour restreindre KPI 20) ─
-- Ajoutée le 2026-09-17 : sans elle, impossible de distinguer un agent
-- `medecin_conseil` d'un agent `accueil` ou `autre` — voir
-- sql/analytics/006_dim_agents.sql. Ajoutée à `table.include.list` du
-- connecteur en même temps (connectors/debezium-postgres-json.json).
-- `AGENT_EMAIL` ajouté le 2026-09-17 : uniquement pour la détection
-- EMAIL_INVALIDE (catalogue anomalies) — pas utilisé par le KPI 20,
-- donc pas propagé dans la table analytique `dim_agents`.
CREATE TABLE dim_agents_src (
    `AGENT_CODE`       STRING NOT NULL,
    `AGENT_NOM`        STRING NOT NULL,
    `AGENT_PRENOMS`    STRING NOT NULL,
    `AGENT_EMAIL`      STRING NOT NULL,
    `AGENT_TYPE_CODE`  STRING NOT NULL,
    PRIMARY KEY (`AGENT_CODE`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dprest-json.public.TB_REF_AGENTS',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.mechanism' = 'SCRAM-SHA-512',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="flink" password="__FLINK_KAFKA_PASSWORD__";',
    'properties.group.id' = 'flink-kpi-dim-agents',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- ── Source 10 : référentiel assurés (identité, pour la famille IDENTITE) ─
-- Ajoutée le 2026-09-17 : sans elle, les 6 codes du catalogue portant sur
-- une fiche assuré (NUMERO_SECU_INVALIDE, DATE_NAISSANCE_ABERRANTE,
-- CHAMP_OBLIGATOIRE_VIDE, ENCODAGE_CASSE, TENTATIVE_INJECTION visent
-- TB_REF_ASSURES ; DOUBLON_EXACT/APPROCHANT aussi, mais leur détection
-- exige de comparer une fiche aux autres déjà connues — hors de portée
-- d'une simple CASE WHEN, restent non détectés ci-dessous) étaient
-- invisibles au pipeline. Ajoutée à `table.include.list` du connecteur en
-- même temps (connectors/debezium-postgres-json.json). Seules les colonnes
-- utiles à la détection sont reprises ici, PAS de dimension `dim_assures`
-- en sortie : ce référentiel ne sert à aucun KPI DPREST, seulement au
-- contrôle qualité (voir sql/analytics/005_qualite_anomalies.sql).
CREATE TABLE dim_assures_src (
    `PERSONNE_UUID`             STRING NOT NULL,
    `ASSURE_NUMERO_IDENTIFIANT` STRING,
    `NUMERO_SECU`               STRING NOT NULL,
    `ASSURE_NOM`                STRING NOT NULL,
    `ASSURE_PRENOMS`            STRING,
    -- DATE pure côté source (comme FACTURE_DATE_SOINS) : Debezium
    -- l'encode en entier (jours depuis epoch), pas en chaîne ISO.
    `ASSURE_DATE_NAISSANCE`     INT,
    `date_naissance` AS CAST(
        TO_TIMESTAMP_LTZ(CAST(`ASSURE_DATE_NAISSANCE` AS BIGINT) * 86400000, 3) AS DATE
    ),
    PRIMARY KEY (`PERSONNE_UUID`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dprest-json.public.TB_REF_ASSURES',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.mechanism' = 'SCRAM-SHA-512',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="flink" password="__FLINK_KAFKA_PASSWORD__";',
    'properties.group.id' = 'flink-kpi-dim-assures',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- ── Cible 1 : activité par jour et par dimension ─────────────────
-- 'sink.parallelism' = 1 : une seule connexion JDBC, suffisant au
-- volume du simulateur et plus économe sur une machine contrainte.
CREATE TABLE kpi_prestations_jour (
    jour                        DATE NOT NULL,
    prestation_code             STRING NOT NULL,
    type_facture_code           STRING NOT NULL,
    regime_code                 STRING NOT NULL,
    centre_sante_type_libelle   STRING NOT NULL,
    nombre_prestations          BIGINT,
    montant_depense             DECIMAL(18, 2),
    montant_pris_en_charge      DECIMAL(18, 2),
    montant_reste_a_charge      DECIMAL(18, 2),
    PRIMARY KEY (jour, prestation_code, type_facture_code, regime_code, centre_sante_type_libelle) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'kpi_prestations_jour',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

-- ── Cible 2 : classement par centre de santé (KPI 10) ────────────
CREATE TABLE kpi_prestations_centre_jour (
    jour                    DATE NOT NULL,
    centre_sante_code       STRING NOT NULL,
    nombre_prestations      BIGINT,
    montant_depense         DECIMAL(18, 2),
    PRIMARY KEY (jour, centre_sante_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'kpi_prestations_centre_jour',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

-- ── Cible 3 : classement par praticien (KPI 11) ──────────────────
CREATE TABLE kpi_prestations_praticien_jour (
    jour                        DATE NOT NULL,
    professionnel_sante_code    STRING NOT NULL,
    nombre_prestations          BIGINT,
    montant_depense             DECIMAL(18, 2),
    PRIMARY KEY (jour, professionnel_sante_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'kpi_prestations_praticien_jour',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

-- ── Cible 4 : passages (factures) par jour (KPI 2/4) ─────────────
CREATE TABLE kpi_factures_jour (
    jour                        DATE NOT NULL,
    type_facture_code           STRING NOT NULL,
    regime_code                 STRING NOT NULL,
    centre_sante_type_libelle   STRING NOT NULL,
    nombre_factures             BIGINT,
    PRIMARY KEY (jour, type_facture_code, regime_code, centre_sante_type_libelle) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'kpi_factures_jour',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

-- ── Cible 5 : prestations par assuré et par centre (KPI 23/24) ───
-- Le classement (top 10) n'est pas calculé ici : il se fait à la
-- lecture (vues v_top10_*), pour rester valable sur toute période.
CREATE TABLE kpi_prestations_assure_jour (
    jour                    DATE NOT NULL,
    personne_uuid           STRING NOT NULL,
    centre_sante_code       STRING NOT NULL,
    nombre_prestations      BIGINT,
    montant_depense         DECIMAL(18, 2),
    PRIMARY KEY (jour, personne_uuid, centre_sante_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'kpi_prestations_assure_jour',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

-- ── Cible 6 : ententes préalables par jour, statut, type de demande ─
-- KPI 16 à 19, 21, 22.
CREATE TABLE kpi_ententes_prealables_jour (
    jour                DATE NOT NULL,
    statut_code         STRING NOT NULL,
    type_demande_code   STRING NOT NULL,
    nombre_ententes     BIGINT,
    delai_moyen_heures  DECIMAL(10, 2),
    montant_engage_cmu  DECIMAL(18, 2),
    PRIMARY KEY (jour, statut_code, type_demande_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'kpi_ententes_prealables_jour',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

-- ── Cible 7 : activité des praticiens-conseils (KPI 20) ───────────
CREATE TABLE kpi_ententes_prealables_agent_jour (
    jour             DATE NOT NULL,
    agent_code       STRING NOT NULL,
    statut_code      STRING NOT NULL,
    nombre_ententes  BIGINT,
    PRIMARY KEY (jour, agent_code, statut_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'kpi_ententes_prealables_agent_jour',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

-- ── Cible 8 : dimension centres de santé (nom) ───────────────────
CREATE TABLE dim_centres_sante (
    centre_sante_code                  STRING NOT NULL,
    centre_sante_denomination          STRING NOT NULL,
    collectivite_code                  STRING,
    type_etablissement_sanitaire_code  STRING,
    PRIMARY KEY (centre_sante_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'dim_centres_sante',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

-- ── Cible 9 : dimension praticiens (nom, prénoms) ────────────────
CREATE TABLE dim_professionnels_sante (
    professionnel_sante_code   STRING NOT NULL,
    nom                        STRING NOT NULL,
    prenoms                    STRING NOT NULL,
    type_code                  STRING,
    statut                     STRING,
    PRIMARY KEY (professionnel_sante_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'dim_professionnels_sante',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

-- ── Cible 10 : dimension localités (nom, coordonnées) ────────────
CREATE TABLE dim_collectivites (
    collectivite_code           STRING NOT NULL,
    collectivite_denomination   STRING NOT NULL,
    collectivite_latitude       DECIMAL(9, 6),
    collectivite_longitude      DECIMAL(9, 6),
    PRIMARY KEY (collectivite_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'dim_collectivites',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

-- ── Cible 12 : dimension agents (type, pour KPI 20) ──────────────
CREATE TABLE dim_agents (
    agent_code       STRING NOT NULL,
    agent_nom        STRING NOT NULL,
    agent_prenoms    STRING NOT NULL,
    agent_type_code  STRING NOT NULL,
    PRIMARY KEY (agent_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'dim_agents',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

-- ── Cible 11 : quarantaine des anomalies de qualité ──────────────
-- Ajoutée le 2026-09-17 : sql/analytics/005_qualite_anomalies.sql.
-- Une ligne source qui échoue à une des vérifications ci-dessous n'est
-- plus seulement ramenée à 0/'(inconnu)' par COALESCE dans les KPI —
-- elle est EN PLUS isolée ici, avec la ligne source complète en JSON
-- (`donnee_brute`), pour respecter la règle CLAUDE.md « isole les
-- rejets, ne les ignore pas silencieusement ».
-- `famille` ajoutée le 2026-09-17 : reprend les six familles de la console
-- d'injection du simulateur (simulateur_V5/anomalies/catalogue.py,
-- dict FAMILLES) — MONTANTS, DATES, IDENTITE, QUANTITES, FORMAT,
-- REFERENTIEL. `domaine` dit sur QUELLE TABLE porte l'anomalie
-- (prestation/facture/agent/assure/entente_prealable) ; `famille` dit QUEL
-- ASPECT de la donnée est en cause (une date, un montant, un nom...) —
-- c'est cet axe que l'utilisateur du dashboard veut pouvoir filtrer
-- (« telle anomalie sur les dates, sur les noms »). Un motif ad hoc
-- (pas un code du catalogue, ex. date_soins_manquante) reprend quand même
-- la famille la plus proche, pour rester filtrable au même endroit.
CREATE TABLE qualite_anomalies (
    domaine         STRING NOT NULL,
    motif_anomalie  STRING NOT NULL,
    famille         STRING NOT NULL,
    cle_metier      STRING NOT NULL,
    donnee_brute    STRING NOT NULL,
    PRIMARY KEY (domaine, cle_metier, motif_anomalie) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'qualite_anomalies',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

-- ═══════════════════════════════════════════════════════════════
-- Famille C — prescriptions et pathologies (ajoutée le 2026-09-21)
-- KPI 25, 27, 30, 31, 32, 33 de docs/kpi.md ; hypothèses H7 à H10.
--
-- Volontairement SANS jointure d'état : chaque source est agrégée ou
-- recopiée seule, les jointures (entente -> facture -> prescription,
-- dénominations) se font à la lecture dans PostgreSQL
-- (sql/analytics/010_kpi_clinique.sql). Le TaskManager a déjà dépassé sa
-- mémoire avec des jointures d'état (docs/decisions.md, 2026-09-17).
-- Les groupes de consommation gardent le préfixe `flink-kpi-` autorisé par
-- les ACL Kafka (scripts/kafka-secure-setup.ps1).
-- ═══════════════════════════════════════════════════════════════

-- Les DATE pures arrivent en entier (jours depuis 1970-01-01, type Debezium
-- io.debezium.time.Date), comme FACTURE_DATE_SOINS de factures_src.
CREATE TABLE prescriptions_src (
    `FACTURE_NUMERO`     STRING NOT NULL,
    `PRESCRIPTION_CODE`  STRING NOT NULL,
    `DATE_DEBUT`         INT NOT NULL,
    `jour_prescription` AS CAST(
        TO_TIMESTAMP_LTZ(CAST(`DATE_DEBUT` AS BIGINT) * 86400000, 3) AS DATE
    ),
    PRIMARY KEY (`FACTURE_NUMERO`, `PRESCRIPTION_CODE`, `DATE_DEBUT`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dprest-json.public.TB_FACTURES_PRESCRIPTIONS',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.mechanism' = 'SCRAM-SHA-512',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="flink" password="__FLINK_KAFKA_PASSWORD__";',
    'properties.group.id' = 'flink-kpi-prescriptions',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

CREATE TABLE pathologies_src (
    `FACTURE_NUMERO`         STRING NOT NULL,
    `PATHOLOGIE_CODE`        STRING NOT NULL,
    `PATHOLOGIE_DATE_DEBUT`  INT NOT NULL,
    `jour_pathologie` AS CAST(
        TO_TIMESTAMP_LTZ(CAST(`PATHOLOGIE_DATE_DEBUT` AS BIGINT) * 86400000, 3) AS DATE
    ),
    PRIMARY KEY (`FACTURE_NUMERO`, `PATHOLOGIE_CODE`, `PATHOLOGIE_DATE_DEBUT`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dprest-json.public.TB_FACTURES_PATHOLOGIES',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.mechanism' = 'SCRAM-SHA-512',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="flink" password="__FLINK_KAFKA_PASSWORD__";',
    'properties.group.id' = 'flink-kpi-pathologies',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- Référentiels : clé Flink = code seul (la source versionne par code + date
-- de début, mais aucun code n'a plusieurs versions au 2026-09-21).
CREATE TABLE dim_medicaments_src (
    `MEDICAMENT_CODE`          STRING NOT NULL,
    `MEDICAMENT_DENOMINATION`  STRING NOT NULL,
    `DCI_CODE`                 STRING,
    PRIMARY KEY (`MEDICAMENT_CODE`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dprest-json.public.TB_REF_MEDICAMENTS',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.mechanism' = 'SCRAM-SHA-512',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="flink" password="__FLINK_KAFKA_PASSWORD__";',
    'properties.group.id' = 'flink-kpi-dim-medicaments',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

CREATE TABLE dim_dci_src (
    `DCI_CODE`          STRING NOT NULL,
    `DCI_DENOMINATION`  STRING NOT NULL,
    PRIMARY KEY (`DCI_CODE`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dprest-json.public.TB_REF_DCI',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.mechanism' = 'SCRAM-SHA-512',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="flink" password="__FLINK_KAFKA_PASSWORD__";',
    'properties.group.id' = 'flink-kpi-dim-dci',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

CREATE TABLE dim_pathologies_src (
    `PATHOLOGIE_CODE`          STRING NOT NULL,
    `PATHOLOGIE_DENOMINATION`  STRING NOT NULL,
    PRIMARY KEY (`PATHOLOGIE_CODE`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dprest-json.public.TB_REF_PATHOLOGIES',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.security.protocol' = 'SASL_PLAINTEXT',
    'properties.sasl.mechanism' = 'SCRAM-SHA-512',
    'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.scram.ScramLoginModule required username="flink" password="__FLINK_KAFKA_PASSWORD__";',
    'properties.group.id' = 'flink-kpi-dim-pathologies',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- ── Cibles de la famille C ────────────────────────────────────────
CREATE TABLE kpi_prescriptions_medicament_jour (
    jour                  DATE NOT NULL,
    medicament_code       STRING NOT NULL,
    nombre_prescriptions  BIGINT,
    PRIMARY KEY (jour, medicament_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'kpi_prescriptions_medicament_jour',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

CREATE TABLE kpi_pathologies_jour (
    jour                DATE NOT NULL,
    pathologie_code     STRING NOT NULL,
    nombre_pathologies  BIGINT,
    PRIMARY KEY (jour, pathologie_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'kpi_pathologies_jour',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

CREATE TABLE fait_prescriptions (
    facture_numero     STRING NOT NULL,
    prescription_code  STRING NOT NULL,
    date_debut         DATE NOT NULL,
    PRIMARY KEY (facture_numero, prescription_code, date_debut) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'fait_prescriptions',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

CREATE TABLE fait_pathologies (
    facture_numero   STRING NOT NULL,
    pathologie_code  STRING NOT NULL,
    date_debut       DATE NOT NULL,
    PRIMARY KEY (facture_numero, pathologie_code, date_debut) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'fait_pathologies',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

CREATE TABLE fait_entente_facture (
    entente_prealable_id  INT NOT NULL,
    facture_numero        STRING,
    type_demande_code     STRING,
    date_debut            DATE,
    PRIMARY KEY (entente_prealable_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'fait_entente_facture',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

CREATE TABLE fait_entente_statut (
    entente_prealable_id  INT NOT NULL,
    statut_code           STRING,
    PRIMARY KEY (entente_prealable_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'fait_entente_statut',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

CREATE TABLE dim_medicaments (
    medicament_code          STRING NOT NULL,
    medicament_denomination  STRING NOT NULL,
    dci_code                 STRING,
    PRIMARY KEY (medicament_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'dim_medicaments',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

CREATE TABLE dim_dci (
    dci_code          STRING NOT NULL,
    dci_denomination  STRING NOT NULL,
    PRIMARY KEY (dci_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'dim_dci',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

CREATE TABLE dim_pathologies (
    pathologie_code          STRING NOT NULL,
    pathologie_denomination  STRING NOT NULL,
    PRIMARY KEY (pathologie_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'dim_pathologies',
    'username' = 'flink_writer',
    'password' = '__FLINK_WRITER_PASSWORD__',
    'sink.parallelism' = '1'
);

-- ── Prestations, jointes SANS coalesce ni filtre ──────────────────
-- Contrairement à `prestations_enrichies` (plus bas), on garde ici les
-- valeurs NULL et les lignes sans date de soins : c'est justement ce
-- qu'on veut détecter. Vue séparée plutôt que de modifier
-- `prestations_enrichies` : les KPI et la détection d'anomalies ont des
-- besoins opposés sur les mêmes colonnes (l'un veut du '(inconnu)'/0
-- pour agréger proprement, l'autre veut le NULL brut pour le repérer).
CREATE VIEW prestations_brutes AS
SELECT
    f.`jour_soins`                    AS jour,
    p.`FACTURE_NUMERO`                AS facture_numero,
    p.`PRESTATION_CODE`               AS prestation_code,
    p.`PROFESSIONNEL_SANTE_CODE`      AS professionnel_sante_code,
    p.`PRESTATION_MONTANT_DEPENSE`    AS montant_depense,
    p.`PRESTATION_MONTANT_RQ`         AS montant_rq,
    p.`PRESTATION_MONTANT_ASSURE`     AS montant_assure,
    p.`PRESTATION_QUANTITE_PRESCRITE` AS quantite_prescrite,
    p.`PRESTATION_QUANTITE_SERVIE`    AS quantite_servie,
    p.`PRESTATION_TAUX_REMBOURSEMENT` AS taux_remboursement,
    f.`TYPE_FACTURE_CODE`             AS type_facture_code,
    f.`REGIME_CODE`                   AS regime_code,
    f.`CENTRE_SANTE_CODE`             AS centre_sante_code,
    f.`CENTRE_SANTE_TYPE_CODE`        AS centre_sante_type_code,
    f.`PERSONNE_UUID`                 AS personne_uuid,
    f.`date_creation_facture`         AS date_creation_facture
FROM prestations_src AS p
JOIN factures_src AS f ON p.`FACTURE_NUMERO` = f.`FACTURE_NUMERO`;

-- ── Montant engagé, pré-agrégé par EP avant jointure ──────────────
-- Réduit le volume joint : une ligne par EP plutôt qu'une ligne par acte.
CREATE VIEW ep_montant AS
SELECT
    `ENTENTE_PREALABLE_ID`,
    SUM(`ACTE_MEDICAL_MONTANT_CMU`) AS montant_engage_cmu
FROM ep_actes_src
GROUP BY `ENTENTE_PREALABLE_ID`;

-- ── Ententes enrichies : EP + statut (LEFT JOIN) + montant ────────
-- LEFT JOIN sur le statut : une EP sans ligne de statut est une EP
-- « sans réponse » (hypothèse H4 de docs/kpi.md), pas une ligne perdue.
CREATE VIEW ep_enrichies AS
SELECT
    CAST(e.`event_time` AS DATE)                        AS jour,
    COALESCE(s.`STATUT_CODE`, 'sans_reponse')            AS statut_code,
    COALESCE(e.`TYPE_DEMANDE_CODE`, '(inconnu)')         AS type_demande_code,
    s.`AGENT_CODE`                                       AS agent_code,
    -- Correction du 2026-09-17 : H3 (docs/kpi.md) mesurait en jours
    -- pleins, ce qui donnait 0 pour la quasi-totalité des EP (traitement
    -- en quelques minutes/heures côté simulateur, franchit rarement
    -- minuit) — pas un bug de calcul, juste une unité trop grossière
    -- pour un pipeline temps réel. Passé en heures.
    CASE WHEN s.`event_time` IS NOT NULL
         THEN TIMESTAMPDIFF(HOUR, e.`event_time`, s.`event_time`)
    END                                                   AS delai_heures,
    COALESCE(m.montant_engage_cmu, 0)                    AS montant_engage_cmu
FROM ep_src AS e
LEFT JOIN ep_statuts_src AS s ON e.`ENTENTE_PREALABLE_ID` = s.`ENTENTE_PREALABLE_ID`
LEFT JOIN ep_montant AS m ON e.`ENTENTE_PREALABLE_ID` = m.`ENTENTE_PREALABLE_ID`
WHERE e.`event_time` IS NOT NULL;

-- ── Passages : lus directement sur les factures, sans jointure ────
-- Une facture = un passage (hypothèse H6). Pas besoin des prestations :
-- inutile de payer l'état de la jointure pour ce décompte.
CREATE VIEW factures_valides AS
SELECT
    `jour_soins`                                        AS jour,
    COALESCE(`TYPE_FACTURE_CODE`, '(inconnu)')          AS type_facture_code,
    COALESCE(`REGIME_CODE`, '(inconnu)')                AS regime_code,
    COALESCE(`CENTRE_SANTE_TYPE_LIBELLE`, '(inconnu)')  AS centre_sante_type_libelle
FROM factures_src
WHERE `jour_soins` IS NOT NULL;

-- ── Jointure des deux flux CDC ───────────────────────────────────
-- Jointure classique entre deux changelogs : Flink garde les deux
-- côtés en état et réémet le résultat quand l'un ou l'autre change.
-- COALESCE sur les dimensions : une clé primaire n'accepte pas NULL
-- côté PostgreSQL (convention '(inconnu)', voir sql/analytics/).
CREATE VIEW prestations_enrichies AS
SELECT
    f.`jour_soins`                                      AS jour,
    p.`PRESTATION_CODE`                                 AS prestation_code,
    COALESCE(f.`TYPE_FACTURE_CODE`, '(inconnu)')        AS type_facture_code,
    COALESCE(f.`REGIME_CODE`, '(inconnu)')              AS regime_code,
    COALESCE(f.`CENTRE_SANTE_TYPE_LIBELLE`, '(inconnu)') AS centre_sante_type_libelle,
    COALESCE(f.`CENTRE_SANTE_CODE`, '(inconnu)')        AS centre_sante_code,
    COALESCE(p.`PROFESSIONNEL_SANTE_CODE`, '(inconnu)') AS professionnel_sante_code,
    COALESCE(f.`PERSONNE_UUID`, '(inconnu)')            AS personne_uuid,
    COALESCE(p.`PRESTATION_MONTANT_DEPENSE`, 0)         AS montant_depense,
    COALESCE(p.`PRESTATION_MONTANT_RQ`, 0)              AS montant_rq,
    COALESCE(p.`PRESTATION_MONTANT_ASSURE`, 0)          AS montant_assure
FROM prestations_src AS p
JOIN factures_src AS f ON p.`FACTURE_NUMERO` = f.`FACTURE_NUMERO`
WHERE f.`jour_soins` IS NOT NULL;

-- Toutes les écritures ci-dessous (KPI, dimensions, détection
-- d'anomalies) partagent la même lecture des topics et le même état :
-- un seul job. Un job séparé dupliquerait lecture, dédoublonnage et
-- état de jointure — c'est ce qui a saturé la mémoire le 2026-09-11
-- (voir docs/decisions.md), et une des causes du nouvel
-- OutOfMemoryError du 2026-09-17 (voir docker-compose.yml et
-- docs/decisions.md, même date).
EXECUTE STATEMENT SET
BEGIN

INSERT INTO kpi_prestations_jour
SELECT
    jour,
    prestation_code,
    type_facture_code,
    regime_code,
    centre_sante_type_libelle,
    COUNT(*)                                AS nombre_prestations,
    CAST(SUM(montant_depense) AS DECIMAL(18, 2)),
    CAST(SUM(montant_rq)      AS DECIMAL(18, 2)),
    CAST(SUM(montant_assure)  AS DECIMAL(18, 2))
FROM prestations_enrichies
GROUP BY jour, prestation_code, type_facture_code, regime_code, centre_sante_type_libelle;

INSERT INTO kpi_prestations_centre_jour
SELECT
    jour,
    centre_sante_code,
    COUNT(*)                                AS nombre_prestations,
    CAST(SUM(montant_depense) AS DECIMAL(18, 2))
FROM prestations_enrichies
GROUP BY jour, centre_sante_code;

INSERT INTO kpi_prestations_praticien_jour
SELECT
    jour,
    professionnel_sante_code,
    COUNT(*)                                AS nombre_prestations,
    CAST(SUM(montant_depense) AS DECIMAL(18, 2))
FROM prestations_enrichies
GROUP BY jour, professionnel_sante_code;

INSERT INTO kpi_factures_jour
SELECT
    jour,
    type_facture_code,
    regime_code,
    centre_sante_type_libelle,
    COUNT(*)                                AS nombre_factures
FROM factures_valides
GROUP BY jour, type_facture_code, regime_code, centre_sante_type_libelle;

INSERT INTO kpi_prestations_assure_jour
SELECT
    jour,
    personne_uuid,
    centre_sante_code,
    COUNT(*)                                AS nombre_prestations,
    CAST(SUM(montant_depense) AS DECIMAL(18, 2))
FROM prestations_enrichies
GROUP BY jour, personne_uuid, centre_sante_code;

INSERT INTO kpi_ententes_prealables_jour
SELECT
    jour,
    statut_code,
    type_demande_code,
    COUNT(*)                                  AS nombre_ententes,
    CAST(AVG(delai_heures) AS DECIMAL(10, 2)) AS delai_moyen_heures,
    CAST(SUM(montant_engage_cmu) AS DECIMAL(18, 2))
FROM ep_enrichies
GROUP BY jour, statut_code, type_demande_code;

INSERT INTO kpi_ententes_prealables_agent_jour
SELECT
    jour,
    agent_code,
    statut_code,
    COUNT(*) AS nombre_ententes
FROM ep_enrichies
WHERE agent_code IS NOT NULL
GROUP BY jour, agent_code, statut_code;

-- ── Dimensions : upsert direct, pas d'agrégation ──────────────────
-- Un référentiel n'a rien à compter : chaque changement CDC réécrit
-- simplement la ligne correspondante (clé = code), comme le ferait un
-- accès à la vue matérielle courante côté source.
INSERT INTO dim_centres_sante
SELECT
    `CENTRE_SANTE_CODE`,
    `CENTRE_SANTE_DENOMINATION`,
    `COLLECTIVITE_CODE`,
    `TYPE_ETABLISSEMENT_SANITAIRE_CODE`
FROM dim_centres_sante_src;

INSERT INTO dim_professionnels_sante
SELECT
    `PROFESSIONNEL_SANTE_CODE`,
    `NOM`,
    `PRENOMS`,
    `TYPE_CODE`,
    `STATUT`
FROM dim_professionnels_src;

INSERT INTO dim_collectivites
SELECT
    `COLLECTIVITE_CODE`,
    `COLLECTIVITE_DENOMINATION`,
    CAST(`COLLECTIVITE_LATITUDE`  AS DECIMAL(9, 6)),
    CAST(`COLLECTIVITE_LONGITUDE` AS DECIMAL(9, 6))
FROM dim_collectivites_src;

INSERT INTO dim_agents
SELECT
    `AGENT_CODE`,
    `AGENT_NOM`,
    `AGENT_PRENOMS`,
    `AGENT_TYPE_CODE`
FROM dim_agents_src;

-- ═══════════════════════════════════════════════════════════════
-- Détection alignée sur le catalogue du simulateur
-- (simulateur_V5/anomalies/catalogue.py, table TB_REF_ANOMALIES) :
-- `motif_anomalie` reprend le CODE exact du catalogue quand le type y
-- figure (MONTANT_ABERRANT, QUANTITE_NULLE, ...), pour pouvoir
-- comparer un jour ce qui est détecté ici à TB_ANOMALIES_INJECTIONS
-- (le « journal vérité terrain » du simulateur — non répliqué dans ce
-- pipeline, volontairement : le comparer directement aurait été
-- tricher, un moteur de qualité réel n'a pas accès à cette vérité).
-- Les motifs qui ne sont PAS des codes du catalogue (ex.
-- date_soins_manquante, montant_depense_manquant) restent des
-- contrôles de complétude généraux, distincts des anomalies injectées
-- volontairement.
--
-- Sur les 19 codes du catalogue, 14 sont détectables avec les tables
-- répliquées par CDC (TB_FACTURES, TB_FACTURES_PRESTATIONS, TB_REF_AGENTS,
-- et depuis le 2026-09-17 TB_REF_ASSURES) : traités ci-dessous. Les 4
-- restants restent hors de portée d'une CASE WHEN ligne à ligne :
-- DOUBLON_EXACT / DOUBLON_APPROCHANT (comparent une fiche aux autres déjà
-- connues — auto-jointure, pas encore écrite), DATE_HORS_DROITS (cible
-- TB_ASSURES_DROITS, non répliquée), et FORMAT_DATE_INCOHERENT (hors de
-- portée du moteur temps réel d'après le simulateur lui-même, portée
-- "campagne" uniquement).
--
-- PRESTATION_ORPHELINE et TYPE_CENTRE_INCONNU comparent à une liste de
-- valeurs valides figée en dur ci-dessous plutôt qu'à un référentiel
-- répliqué (TB_REF_ACTES_MEDICAUX n'est pas non plus dans
-- table.include.list) — liste constatée sur les données actuelles
-- (2026-09-17), à revérifier si le simulateur ajoute de nouveaux codes.
-- ═══════════════════════════════════════════════════════════════

-- ── Anomalies 1/5 : prestations ────────────────────────────────────
-- Un COALESCE en amont (dans prestations_enrichies) aurait rendu les
-- deux premiers cas invisibles : montant NULL et montant 0 se
-- seraient confondus. Ici, on lit `prestations_brutes`, avant tout
-- COALESCE. CASE = un seul motif par ligne, dans cet ordre de
-- priorité (une ligne du simulateur ne porte normalement qu'une seule
-- anomalie injectée à la fois).
INSERT INTO qualite_anomalies
SELECT
    'prestation' AS domaine,
    CASE
        WHEN jour IS NULL THEN 'date_soins_manquante'
        WHEN montant_depense IS NULL THEN 'montant_depense_manquant'
        -- MONTANT_ABERRANT : négatif, ou démesuré. Toutes les
        -- prestations valent 10 000 F sur ce simulateur (limite n°4 de
        -- docs/kpi.md) : au-delà de 500 000 F (50x le tarif normal),
        -- ce n'est plus un montant plausible.
        WHEN montant_depense < 0 OR montant_depense > 500000 THEN 'MONTANT_ABERRANT'
        WHEN quantite_servie = 0 AND quantite_prescrite > 0 THEN 'QUANTITE_NULLE'
        WHEN quantite_prescrite IS NOT NULL AND quantite_servie > quantite_prescrite THEN 'QUANTITE_EXCESSIVE'
        -- Un taux de remboursement hors de [0, 100] est techniquement
        -- impossible, quel que soit le régime de l'assuré.
        WHEN taux_remboursement IS NOT NULL AND (taux_remboursement < 0 OR taux_remboursement > 100) THEN 'MONTANT_HORS_BAREME'
        -- Part CMU + part assuré doivent recomposer le montant dépensé
        -- (tolérance 1 F pour les arrondis).
        WHEN montant_depense IS NOT NULL
             AND ABS(COALESCE(montant_rq, 0) + COALESCE(montant_assure, 0) - montant_depense) > 1
             THEN 'REPARTITION_FAUSSEE'
        -- Liste des 30 codes de prestation valides, constatée sur
        -- TB_FACTURES_PRESTATIONS le 2026-09-17.
        WHEN prestation_code NOT IN (
            'BIO-CRE','BIO-CRP','BIO-GEU','BIO-GLY','BIO-GRO','BIO-HBA','BIO-HBS','BIO-LIP',
            'BIO-NFS','BIO-SEL','BIO-TRA','BIO-URE','BIO-URI','CONS-GEN','CONS-SPE',
            'DENT-CAR','DENT-DET','DENT-EXT','HOS-CHI','HOS-MAT','HOS-MED','HOS-PED','HOS-REA',
            'IMG-ECH','IMG-IRM','IMG-MAM','IMG-RAD','IMG-SCA','SOI-PAN','URG-ACC'
        ) THEN 'PRESTATION_ORPHELINE'
    END AS motif_anomalie,
    CASE
        WHEN jour IS NULL THEN 'DATES'
        WHEN montant_depense IS NULL THEN 'MONTANTS'
        WHEN montant_depense < 0 OR montant_depense > 500000 THEN 'MONTANTS'
        WHEN quantite_servie = 0 AND quantite_prescrite > 0 THEN 'QUANTITES'
        WHEN quantite_prescrite IS NOT NULL AND quantite_servie > quantite_prescrite THEN 'QUANTITES'
        WHEN taux_remboursement IS NOT NULL AND (taux_remboursement < 0 OR taux_remboursement > 100) THEN 'MONTANTS'
        WHEN montant_depense IS NOT NULL
             AND ABS(COALESCE(montant_rq, 0) + COALESCE(montant_assure, 0) - montant_depense) > 1
             THEN 'MONTANTS'
        WHEN prestation_code NOT IN (
            'BIO-CRE','BIO-CRP','BIO-GEU','BIO-GLY','BIO-GRO','BIO-HBA','BIO-HBS','BIO-LIP',
            'BIO-NFS','BIO-SEL','BIO-TRA','BIO-URE','BIO-URI','CONS-GEN','CONS-SPE',
            'DENT-CAR','DENT-DET','DENT-EXT','HOS-CHI','HOS-MAT','HOS-MED','HOS-PED','HOS-REA',
            'IMG-ECH','IMG-IRM','IMG-MAM','IMG-RAD','IMG-SCA','SOI-PAN','URG-ACC'
        ) THEN 'REFERENTIEL'
    END AS famille,
    facture_numero || '/' || prestation_code AS cle_metier,
    JSON_OBJECT(
        KEY 'facture_numero'           VALUE facture_numero,
        KEY 'prestation_code'          VALUE prestation_code,
        KEY 'professionnel_sante_code' VALUE professionnel_sante_code,
        KEY 'montant_depense'          VALUE montant_depense,
        KEY 'montant_rq'               VALUE montant_rq,
        KEY 'montant_assure'           VALUE montant_assure,
        KEY 'quantite_prescrite'       VALUE quantite_prescrite,
        KEY 'quantite_servie'          VALUE quantite_servie,
        KEY 'taux_remboursement'       VALUE taux_remboursement,
        KEY 'type_facture_code'        VALUE type_facture_code,
        KEY 'regime_code'              VALUE regime_code,
        KEY 'centre_sante_code'        VALUE centre_sante_code,
        KEY 'personne_uuid'            VALUE personne_uuid,
        KEY 'jour_soins'               VALUE CAST(jour AS STRING)
    ) AS donnee_brute
FROM prestations_brutes
WHERE jour IS NULL
   OR montant_depense IS NULL
   OR montant_depense < 0 OR montant_depense > 500000
   OR (quantite_servie = 0 AND quantite_prescrite > 0)
   OR (quantite_prescrite IS NOT NULL AND quantite_servie > quantite_prescrite)
   OR (taux_remboursement IS NOT NULL AND (taux_remboursement < 0 OR taux_remboursement > 100))
   OR (montant_depense IS NOT NULL AND ABS(COALESCE(montant_rq, 0) + COALESCE(montant_assure, 0) - montant_depense) > 1)
   OR prestation_code NOT IN (
        'BIO-CRE','BIO-CRP','BIO-GEU','BIO-GLY','BIO-GRO','BIO-HBA','BIO-HBS','BIO-LIP',
        'BIO-NFS','BIO-SEL','BIO-TRA','BIO-URE','BIO-URI','CONS-GEN','CONS-SPE',
        'DENT-CAR','DENT-DET','DENT-EXT','HOS-CHI','HOS-MAT','HOS-MED','HOS-PED','HOS-REA',
        'IMG-ECH','IMG-IRM','IMG-MAM','IMG-RAD','IMG-SCA','SOI-PAN','URG-ACC'
   );

-- ── Anomalies 2/5 : factures (dates, référentiel centre) ──────────
-- Bloc A : `date_soins_manquante` — ces lignes étaient jusqu'ici
-- filtrées par `factures_valides` (WHERE jour_soins IS NOT NULL) et
-- disparaissaient purement et simplement des KPI 2/4, sans laisser de
-- trace. Restent filtrées des KPI (pas de date à rattacher à une
-- fenêtre), mais comptées ici plutôt que perdues.
INSERT INTO qualite_anomalies
SELECT
    'facture' AS domaine,
    'date_soins_manquante' AS motif_anomalie,
    'DATES' AS famille,
    `FACTURE_NUMERO` AS cle_metier,
    JSON_OBJECT(
        KEY 'facture_numero'            VALUE `FACTURE_NUMERO`,
        KEY 'type_facture_code'         VALUE `TYPE_FACTURE_CODE`,
        KEY 'regime_code'               VALUE `REGIME_CODE`,
        KEY 'centre_sante_code'         VALUE `CENTRE_SANTE_CODE`,
        KEY 'centre_sante_type_libelle' VALUE `CENTRE_SANTE_TYPE_LIBELLE`,
        KEY 'personne_uuid'             VALUE `PERSONNE_UUID`,
        KEY 'facture_date_soins_brute'  VALUE CAST(`FACTURE_DATE_SOINS` AS STRING)
    ) AS donnee_brute
FROM factures_src
WHERE `jour_soins` IS NULL;

-- Bloc B : les 3 codes du catalogue portés par TB_FACTURES.
-- DATE_ANTIDATEE / DATE_SOINS_FUTURE comparent la date de SOINS à la
-- date de CRÉATION du dossier (pas à `now()` : un rejeu Kafka depuis
-- `earliest-offset` ferait sinon apparaître comme "future" une
-- facture ancienne rejouée après coup). Seuil de 60 jours pour
-- DATE_ANTIDATEE : en-deçà, un soin déclaré un peu après coup reste
-- plausible ; le catalogue du simulateur antidate jusqu'à un an, donc
-- 60 jours reste largement dans la zone anormale sans faux positif sur
-- un délai de saisie normal.
INSERT INTO qualite_anomalies
SELECT
    'facture' AS domaine,
    CASE
        WHEN jour_soins < CAST(date_creation_facture AS DATE) - INTERVAL '60' DAY THEN 'DATE_ANTIDATEE'
        WHEN jour_soins > CAST(date_creation_facture AS DATE) THEN 'DATE_SOINS_FUTURE'
        WHEN `CENTRE_SANTE_TYPE_CODE` NOT IN (
            'AUT','CAT','CHR','CHU','CLN','CMS','CS','CSR','CSU','DISP','FSU','HG','HOP','INST','MAT','MIL','PMI','SSSU'
        ) THEN 'TYPE_CENTRE_INCONNU'
    END AS motif_anomalie,
    CASE
        WHEN jour_soins < CAST(date_creation_facture AS DATE) - INTERVAL '60' DAY THEN 'DATES'
        WHEN jour_soins > CAST(date_creation_facture AS DATE) THEN 'DATES'
        WHEN `CENTRE_SANTE_TYPE_CODE` NOT IN (
            'AUT','CAT','CHR','CHU','CLN','CMS','CS','CSR','CSU','DISP','FSU','HG','HOP','INST','MAT','MIL','PMI','SSSU'
        ) THEN 'REFERENTIEL'
    END AS famille,
    `FACTURE_NUMERO` AS cle_metier,
    JSON_OBJECT(
        KEY 'facture_numero'          VALUE `FACTURE_NUMERO`,
        KEY 'centre_sante_code'       VALUE `CENTRE_SANTE_CODE`,
        KEY 'centre_sante_type_code'  VALUE `CENTRE_SANTE_TYPE_CODE`,
        KEY 'jour_soins'              VALUE CAST(`jour_soins` AS STRING),
        KEY 'date_creation_facture'   VALUE CAST(`date_creation_facture` AS STRING)
    ) AS donnee_brute
FROM factures_src
WHERE `jour_soins` IS NOT NULL
  AND `date_creation_facture` IS NOT NULL
  AND (
        `jour_soins` < CAST(`date_creation_facture` AS DATE) - INTERVAL '60' DAY
        OR `jour_soins` > CAST(`date_creation_facture` AS DATE)
        OR `CENTRE_SANTE_TYPE_CODE` NOT IN (
            'AUT','CAT','CHR','CHU','CLN','CMS','CS','CSR','CSU','DISP','FSU','HG','HOP','INST','MAT','MIL','PMI','SSSU'
        )
      );

-- ── Anomalies 3/5 : agents (EMAIL_INVALIDE) ───────────────────────
-- Contrôle de forme simple (présence d'un '@' suivi d'un '.') plutôt
-- qu'une regex complète : suffisant pour repérer une adresse cassée
-- par l'injecteur, pas un vérificateur RFC 5322 complet.
INSERT INTO qualite_anomalies
SELECT
    'agent' AS domaine,
    'EMAIL_INVALIDE' AS motif_anomalie,
    'FORMAT' AS famille,
    `AGENT_CODE` AS cle_metier,
    JSON_OBJECT(
        KEY 'agent_code'       VALUE `AGENT_CODE`,
        KEY 'agent_nom'        VALUE `AGENT_NOM`,
        KEY 'agent_prenoms'    VALUE `AGENT_PRENOMS`,
        KEY 'agent_email'      VALUE `AGENT_EMAIL`,
        KEY 'agent_type_code'  VALUE `AGENT_TYPE_CODE`
    ) AS donnee_brute
FROM dim_agents_src
WHERE `AGENT_EMAIL` NOT LIKE '%@%.%';

-- ── Anomalies 4/5 : ententes préalables incohérentes ──────────────
-- Type de demande manquant (viole l'hypothèse implicite du KPI 21) ou
-- montant d'acte engagé négatif (un montant CMU ne peut pas l'être).
INSERT INTO qualite_anomalies
SELECT
    'entente_prealable' AS domaine,
    CASE
        WHEN e.`TYPE_DEMANDE_CODE` IS NULL THEN 'type_demande_manquant'
        WHEN m.montant_engage_cmu < 0 THEN 'montant_engage_negatif'
    END AS motif_anomalie,
    CASE
        WHEN e.`TYPE_DEMANDE_CODE` IS NULL THEN 'FORMAT'
        WHEN m.montant_engage_cmu < 0 THEN 'MONTANTS'
    END AS famille,
    CAST(e.`ENTENTE_PREALABLE_ID` AS STRING) AS cle_metier,
    JSON_OBJECT(
        KEY 'entente_prealable_id' VALUE CAST(e.`ENTENTE_PREALABLE_ID` AS STRING),
        KEY 'type_demande_code'    VALUE e.`TYPE_DEMANDE_CODE`,
        KEY 'date_debut'           VALUE e.`ENTENTE_PREALABLE_DATE_DEBUT`,
        KEY 'statut_code'          VALUE s.`STATUT_CODE`,
        KEY 'agent_code'           VALUE s.`AGENT_CODE`,
        KEY 'montant_engage_cmu'   VALUE m.montant_engage_cmu
    ) AS donnee_brute
FROM ep_src AS e
LEFT JOIN ep_statuts_src AS s ON e.`ENTENTE_PREALABLE_ID` = s.`ENTENTE_PREALABLE_ID`
LEFT JOIN ep_montant AS m ON e.`ENTENTE_PREALABLE_ID` = m.`ENTENTE_PREALABLE_ID`
WHERE e.`TYPE_DEMANDE_CODE` IS NULL OR m.montant_engage_cmu < 0;

-- ── Anomalies 5/5 : identité de l'assuré (dates, noms) ────────────
-- Ajoutée le 2026-09-17 avec `dim_assures_src` / TB_REF_ASSURES. Couvre
-- 5 des 6 codes IDENTITE du catalogue détectables ligne à ligne :
--   - NUMERO_SECU_INVALIDE : longueur ou préfixe invalide. Format valide
--     constaté dans seed/identifiants.py — treize caractères, préfixe
--     "394" (vérifié en base le 2026-09-17 : une doc du simulateur
--     mentionnait "384" par erreur). Le moteur temps réel injecte la
--     sentinelle "00000000000000"
--     (quatorze zéros, volontairement hors format) ou une troncature.
--   - DATE_NAISSANCE_ABERRANTE : future, ou plus que centenaire — la
--     règle exacte de anomalies/config.py:tirer_date_naissance.
--   - CHAMP_OBLIGATOIRE_VIDE : `ASSURE_NOM` vide (chaîne '', pas NULL :
--     la colonne est NOT NULL côté source).
--   - ENCODAGE_CASSE : mojibake ou substitution par '?' — même geste que
--     simulation/inscription.py:_casser_encodage (utf-8 relu en latin-1,
--     ou premier caractère remplacé par '?').
--   - TENTATIVE_INJECTION : une des 6 charges fixes de
--     simulation/inscription.py:CHARGES_INJECTION (payloads SQL/XSS/LDAP/
--     template/traversal classiques) glissée dans le nom.
-- DOUBLON_EXACT et DOUBLON_APPROCHANT restent hors de portée : leur
-- détection compare une fiche aux autres déjà connues (auto-jointure sur
-- fenêtre glissante), pas une simple CASE WHEN ligne à ligne — à traiter
-- séparément si besoin (voir docs/guides/etape6c_superset_qualite.md).
INSERT INTO qualite_anomalies
SELECT
    'assure' AS domaine,
    CASE
        WHEN `ASSURE_NOM` = '' THEN 'CHAMP_OBLIGATOIRE_VIDE'
        WHEN `ASSURE_NOM` LIKE '%?%'
          OR `ASSURE_NOM` LIKE '%Ã%' OR `ASSURE_NOM` LIKE '%Â%'
             THEN 'ENCODAGE_CASSE'
        WHEN `ASSURE_NOM` IN (
            '''; DROP TABLE TB_FACTURES; --', ''' OR ''1''=''1',
            '<script>alert(1)</script>', '../../../etc/passwd',
            '${jndi:ldap://x}', '{{7*7}}'
        ) THEN 'TENTATIVE_INJECTION'
        WHEN CHAR_LENGTH(`NUMERO_SECU`) <> 13 OR `NUMERO_SECU` NOT LIKE '394%' THEN 'NUMERO_SECU_INVALIDE'
        WHEN `date_naissance` IS NOT NULL AND (
                `date_naissance` > CURRENT_DATE
                OR `date_naissance` < CURRENT_DATE - INTERVAL '100' YEAR(3)
             ) THEN 'DATE_NAISSANCE_ABERRANTE'
    END AS motif_anomalie,
    CASE
        WHEN `ASSURE_NOM` = '' THEN 'IDENTITE'
        WHEN `ASSURE_NOM` LIKE '%?%'
          OR `ASSURE_NOM` LIKE '%Ã%' OR `ASSURE_NOM` LIKE '%Â%'
             THEN 'FORMAT'
        WHEN `ASSURE_NOM` IN (
            '''; DROP TABLE TB_FACTURES; --', ''' OR ''1''=''1',
            '<script>alert(1)</script>', '../../../etc/passwd',
            '${jndi:ldap://x}', '{{7*7}}'
        ) THEN 'FORMAT'
        WHEN CHAR_LENGTH(`NUMERO_SECU`) <> 13 OR `NUMERO_SECU` NOT LIKE '394%' THEN 'IDENTITE'
        WHEN `date_naissance` IS NOT NULL AND (
                `date_naissance` > CURRENT_DATE
                OR `date_naissance` < CURRENT_DATE - INTERVAL '100' YEAR(3)
             ) THEN 'IDENTITE'
    END AS famille,
    `PERSONNE_UUID` AS cle_metier,
    JSON_OBJECT(
        KEY 'personne_uuid'             VALUE `PERSONNE_UUID`,
        KEY 'assure_numero_identifiant' VALUE `ASSURE_NUMERO_IDENTIFIANT`,
        KEY 'numero_secu'               VALUE `NUMERO_SECU`,
        KEY 'assure_nom'                VALUE `ASSURE_NOM`,
        KEY 'assure_prenoms'            VALUE `ASSURE_PRENOMS`,
        KEY 'assure_date_naissance'     VALUE CAST(`date_naissance` AS STRING)
    ) AS donnee_brute
FROM dim_assures_src
WHERE `ASSURE_NOM` = ''
   OR `ASSURE_NOM` LIKE '%?%'
   OR `ASSURE_NOM` LIKE '%Ã%' OR `ASSURE_NOM` LIKE '%Â%'
   OR `ASSURE_NOM` IN (
        '''; DROP TABLE TB_FACTURES; --', ''' OR ''1''=''1',
        '<script>alert(1)</script>', '../../../etc/passwd',
        '${jndi:ldap://x}', '{{7*7}}'
   )
   OR CHAR_LENGTH(`NUMERO_SECU`) <> 13 OR `NUMERO_SECU` NOT LIKE '394%'
   OR (`date_naissance` IS NOT NULL AND (
        `date_naissance` > CURRENT_DATE
        OR `date_naissance` < CURRENT_DATE - INTERVAL '100' YEAR(3)
   ));

-- ═══════════════════════════════════════════════════════════════
-- Famille C — prescriptions et pathologies (2026-09-21)
-- Agrégats (KPI 25, 27, 30), copies 1:1 (KPI 31 à 33) et référentiels.
-- Aucune jointure : voir le bloc de définition des sources plus haut.
-- ═══════════════════════════════════════════════════════════════

-- KPI 25 et 27 : lignes de prescription par jour (H9 : DATE_DEBUT) et médicament.
INSERT INTO kpi_prescriptions_medicament_jour
SELECT
    jour_prescription,
    `PRESCRIPTION_CODE`,
    COUNT(*) AS nombre_prescriptions
FROM prescriptions_src
GROUP BY jour_prescription, `PRESCRIPTION_CODE`;

-- KPI 30 : pathologies déclarées par jour et par pathologie.
INSERT INTO kpi_pathologies_jour
SELECT
    jour_pathologie,
    `PATHOLOGIE_CODE`,
    COUNT(*) AS nombre_pathologies
FROM pathologies_src
GROUP BY jour_pathologie, `PATHOLOGIE_CODE`;

-- KPI 31 à 33 : copies 1:1, jointes à la lecture (sql/analytics/010_kpi_clinique.sql).
INSERT INTO fait_prescriptions
SELECT
    `FACTURE_NUMERO`,
    `PRESCRIPTION_CODE`,
    jour_prescription
FROM prescriptions_src;

INSERT INTO fait_pathologies
SELECT
    `FACTURE_NUMERO`,
    `PATHOLOGIE_CODE`,
    jour_pathologie
FROM pathologies_src;

INSERT INTO fait_entente_facture
SELECT
    `ENTENTE_PREALABLE_ID`,
    `FACTURE_NUMERO`,
    `TYPE_DEMANDE_CODE`,
    CAST(`event_time` AS DATE)
FROM ep_src;

INSERT INTO fait_entente_statut
SELECT
    `ENTENTE_PREALABLE_ID`,
    `STATUT_CODE`
FROM ep_statuts_src;

INSERT INTO dim_medicaments
SELECT
    `MEDICAMENT_CODE`,
    `MEDICAMENT_DENOMINATION`,
    `DCI_CODE`
FROM dim_medicaments_src;

INSERT INTO dim_dci
SELECT
    `DCI_CODE`,
    `DCI_DENOMINATION`
FROM dim_dci_src;

INSERT INTO dim_pathologies
SELECT
    `PATHOLOGIE_CODE`,
    `PATHOLOGIE_DENOMINATION`
FROM dim_pathologies_src;

END;
