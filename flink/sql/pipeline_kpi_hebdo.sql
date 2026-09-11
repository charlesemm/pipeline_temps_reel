-- ═══════════════════════════════════════════════════════════════
-- Étape 3 — premier job Flink : preuve du mécanisme de bout en bout
-- (lecture CDC → fenêtrage → écriture), PAS la définition finale des
-- KPI. La formule ci-dessous est une HYPOTHÈSE PROVISOIRE :
--   - fenêtrée sur DATE_CREATION (date d'écriture en base), alors que
--     docs/dictionnaire_donnees.md signale que FACTURE_DATE_SOINS
--     (sur TB_FACTURES, pas TB_FACTURES_PRESTATIONS) est probablement
--     la bonne date de référence métier ;
--   - pas de jointure avec TB_FACTURES pour le type de prestation.
-- À reprendre à l'étape 4 une fois les définitions validées avec
-- Mathieu dans docs/kpi.md. Voir aussi le "taux de rejet facture vs
-- ligne" signalé dans le dictionnaire de données.
-- ═══════════════════════════════════════════════════════════════

-- ── Source : le flux CDC des prestations, tel que Debezium l'écrit ──
-- Lu depuis le topic JSON (connecteur dprest-postgres-source-json),
-- pas le topic Avro de l'étape 2 : le connecteur Flink
-- 'debezium-avro-confluent' a un bug de résolution de schéma sur
-- l'enveloppe Debezium (voir docs/decisions.md, 2026-09-10 —
-- "Second connecteur Debezium en JSON") — contournement documenté,
-- pas une préférence de conception. 'format' = 'debezium-json' fait
-- le même travail de déballage de l'enveloppe CDC que la variante
-- Avro, mais sans ce bug.
CREATE TABLE facture_prestations_src (
    `FACTURE_NUMERO`               STRING NOT NULL,
    `PRESTATION_CODE`              STRING NOT NULL,
    `STATUT_REMBOURSEMENT`         STRING,
    `MOTIF_REJET_CODE`             STRING,
    `PRESTATION_MONTANT_DEPENSE`   DOUBLE,
    -- ATTENTION au nommage : le format JSON de Flink fait la
    -- correspondance colonne <-> champ JSON **par nom**. La colonne
    -- physique doit donc s'appeler exactement `DATE_CREATION`, comme le
    -- champ produit par Debezium. Une colonne nommée autrement (ex.
    -- `DATE_CREATION_RAW`) ne correspond à aucun champ du message et
    -- vaut NULL pour 100 % des lignes — erreur commise puis corrigée
    -- ici : elle envoyait toutes les lignes en anomalie qualité et
    -- empêchait le watermark d'avancer, donc la fenêtre de se fermer.
    --
    -- DATE_CREATION arrive en chaîne ISO-8601 (type Debezium
    -- ZonedTimestamp, pas un format temporel natif JSON) — convertie en
    -- TIMESTAMP par la colonne calculée `event_time`, qui porte un nom
    -- distinct pour ne pas entrer en conflit avec la colonne physique.
    `DATE_CREATION`                STRING,
    `event_time` AS TO_TIMESTAMP(
        REPLACE(SUBSTRING(`DATE_CREATION`, 1, 23), 'T', ' '), 'yyyy-MM-dd HH:mm:ss.SSS'
    ),
    WATERMARK FOR `event_time` AS `event_time` - INTERVAL '30' SECOND,
    PRIMARY KEY (`FACTURE_NUMERO`, `PRESTATION_CODE`) NOT ENFORCED
) WITH (
    'connector' = 'kafka',
    'topic' = 'dprest-json.public.TB_FACTURES_PRESTATIONS',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-kpi-prestations-hebdo',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'debezium-json'
);

-- ── Sortie 1 : agrégat hebdomadaire (nombre + montant, par statut) ──
-- upsert-kafka : la clé (semaine + statut) est mise à jour à chaque
-- recalcul de fenêtre plutôt que d'empiler des lignes en double —
-- comportement idempotent, cohérent avec les règles d'ingénierie de
-- CLAUDE.md.
CREATE TABLE kpi_prestations_hebdo_sink (
    semaine_debut           TIMESTAMP(3),
    statut_remboursement    STRING,
    nombre_prestations      BIGINT,
    montant_total           DOUBLE,
    PRIMARY KEY (semaine_debut, statut_remboursement) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'kpi.prestations_hebdo_brut',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- ── Sortie 2 : contrôle qualité — lignes isolées, pas ignorées ──
-- Montant manquant ou négatif : anomalie de fait, indépendante de
-- toute définition de KPI encore à valider.
-- upsert-kafka (pas un simple topic 'kafka') : la table source vient
-- d'un flux CDC avec clé primaire, donc c'est un changelog (INSERT/
-- UPDATE/DELETE), pas un flux append-only — un sink 'kafka' classique
-- refuse ces évènements de mise à jour/suppression. Bénéfice pratique :
-- une ligne corrigée par la suite (montant réparé) disparaît d'elle-
-- même du topic qualité, au lieu d'y rester indéfiniment.
-- date_creation_brute en STRING (pas TIMESTAMP) : la colonne calculée
-- `DATE_CREATION` porte les métadonnées de "rowtime" issues du
-- WATERMARK de la table source, propagées par Flink même après un
-- filtrage/projection — un sink en aval qui la reçoit NULL (cas
-- "date_creation_manquante", justement le cas qu'on veut isoler) fait
-- planter l'écriture (NullPointerException dans
-- StreamRecordTimestampInserter, testé en pratique). Utiliser la
-- valeur brute (DATE_CREATION_RAW) évite complètement ce mécanisme.
CREATE TABLE qualite_prestations_rejetees_sink (
    facture_numero          STRING,
    prestation_code         STRING,
    montant_depense         DOUBLE,
    motif_rejet_qualite     STRING,
    date_creation_brute     STRING,
    PRIMARY KEY (facture_numero, prestation_code) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'qualite.prestations_rejetees',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- Filtre `event_time IS NOT NULL` avant le TUMBLE, dans une vue à
-- part : une ligne sans date exploitable ne peut pas être rattachée à
-- une fenêtre temporelle (Flink refuse une "RowTime" nulle en entrée
-- d'un fenêtrage — testé en pratique). Ces lignes ne sont pas perdues :
-- isolées dans le contrôle qualité ci-dessous. TUMBLE(TABLE ...) exige
-- une table/vue nommée, pas une sous-requête en ligne.
CREATE VIEW facture_prestations_valide AS
SELECT * FROM facture_prestations_src WHERE `event_time` IS NOT NULL;

-- Les deux INSERT partagent la même lecture du topic source (un seul
-- job Flink, un seul groupe de consommateurs) grâce à STATEMENT SET.
EXECUTE STATEMENT SET
BEGIN

INSERT INTO kpi_prestations_hebdo_sink
SELECT
    window_start                                   AS semaine_debut,
    COALESCE(`STATUT_REMBOURSEMENT`, 'inconnu')     AS statut_remboursement,
    COUNT(*)                                        AS nombre_prestations,
    SUM(COALESCE(`PRESTATION_MONTANT_DEPENSE`, 0))  AS montant_total
FROM TABLE(
    TUMBLE(TABLE facture_prestations_valide, DESCRIPTOR(`event_time`), INTERVAL '7' DAY)
)
GROUP BY window_start, window_end, `STATUT_REMBOURSEMENT`;

INSERT INTO qualite_prestations_rejetees_sink
SELECT
    `FACTURE_NUMERO`,
    `PRESTATION_CODE`,
    `PRESTATION_MONTANT_DEPENSE`,
    CASE
        WHEN `DATE_CREATION` IS NULL THEN 'date_creation_manquante'
        WHEN `PRESTATION_MONTANT_DEPENSE` IS NULL THEN 'montant_manquant'
        WHEN `PRESTATION_MONTANT_DEPENSE` < 0 THEN 'montant_negatif'
    END AS motif_rejet_qualite,
    `DATE_CREATION`
FROM facture_prestations_src
WHERE `DATE_CREATION` IS NULL
   OR `PRESTATION_MONTANT_DEPENSE` IS NULL
   OR `PRESTATION_MONTANT_DEPENSE` < 0;

END;
