-- ═══════════════════════════════════════════════════════════════
-- Étape 4 — KPI « prestations » écrits dans la base analytique
--
-- Définitions : docs/kpi.md (famille A, KPI 1-15, 23-24).
-- Schéma cible : sql/analytics/001_schema.sql et 002_kpi_passages_assures.sql.
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
CREATE TABLE prestations_src (
    `FACTURE_NUMERO`                STRING NOT NULL,
    `PRESTATION_CODE`               STRING NOT NULL,
    `PROFESSIONNEL_SANTE_CODE`      STRING,
    `PRESTATION_MONTANT_DEPENSE`    DOUBLE,
    `PRESTATION_MONTANT_RQ`         DOUBLE,
    `PRESTATION_MONTANT_ASSURE`     DOUBLE,
    PRIMARY KEY (`FACTURE_NUMERO`, `PRESTATION_CODE`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dprest-json.public.TB_FACTURES_PRESTATIONS',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-kpi-prestations',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- ── Source 2 : l'en-tête de facture (dimensions + date de soins) ──
-- FACTURE_DATE_SOINS arrive en entier : nombre de jours depuis
-- 1970-01-01 (type Debezium io.debezium.time.Date). Converti en DATE
-- par la colonne calculée `jour_soins` — formule vérifiée en amont
-- (20703 -> 2026-09-07).
CREATE TABLE factures_src (
    `FACTURE_NUMERO`                STRING NOT NULL,
    `TYPE_FACTURE_CODE`             STRING,
    `REGIME_CODE`                   STRING,
    `CENTRE_SANTE_CODE`             STRING,
    `CENTRE_SANTE_TYPE_LIBELLE`     STRING,
    `PERSONNE_UUID`                 STRING,
    `FACTURE_DATE_SOINS`            INT,
    `jour_soins` AS CAST(
        TO_TIMESTAMP_LTZ(CAST(`FACTURE_DATE_SOINS` AS BIGINT) * 86400000, 3) AS DATE
    ),
    PRIMARY KEY (`FACTURE_NUMERO`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dprest-json.public.TB_FACTURES',
    'properties.bootstrap.servers' = 'kafka:9092',
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
    'properties.group.id' = 'flink-kpi-ep-actes',
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
    'username' = 'dprest',
    'password' = 'dprest_dev_2026',
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
    'username' = 'dprest',
    'password' = 'dprest_dev_2026',
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
    'username' = 'dprest',
    'password' = 'dprest_dev_2026',
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
    'username' = 'dprest',
    'password' = 'dprest_dev_2026',
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
    'username' = 'dprest',
    'password' = 'dprest_dev_2026',
    'sink.parallelism' = '1'
);

-- ── Cible 6 : ententes préalables par jour, statut, type de demande ─
-- KPI 16 à 19, 21, 22.
CREATE TABLE kpi_ententes_prealables_jour (
    jour                DATE NOT NULL,
    statut_code         STRING NOT NULL,
    type_demande_code   STRING NOT NULL,
    nombre_ententes     BIGINT,
    delai_moyen_jours   DECIMAL(10, 2),
    montant_engage_cmu  DECIMAL(18, 2),
    PRIMARY KEY (jour, statut_code, type_demande_code) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres-analytics:5432/dprest_analytics',
    'table-name' = 'kpi_ententes_prealables_jour',
    'username' = 'dprest',
    'password' = 'dprest_dev_2026',
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
    'username' = 'dprest',
    'password' = 'dprest_dev_2026',
    'sink.parallelism' = '1'
);

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
    CASE WHEN s.`event_time` IS NOT NULL
         THEN TIMESTAMPDIFF(DAY, e.`event_time`, s.`event_time`)
    END                                                   AS delai_jours,
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

-- Les sept écritures partagent la même lecture des topics et le même
-- état : un seul job. Un job séparé dupliquerait lecture, dédoublonnage
-- et état de jointure — c'est ce qui a saturé la mémoire le 2026-09-11
-- (voir docs/decisions.md).
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
    COUNT(*)                                AS nombre_ententes,
    CAST(AVG(delai_jours) AS DECIMAL(10, 2)) AS delai_moyen_jours,
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

END;
