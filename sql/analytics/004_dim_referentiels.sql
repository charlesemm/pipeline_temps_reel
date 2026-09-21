-- ═══════════════════════════════════════════════════════════════
-- Schéma analytique — ajout du 2026-09-15
--
-- Tables de dimension : dénominations des centres de santé et des
-- praticiens (KPI 10, 11), et coordonnées géographiques des centres
-- (via la collectivité de rattachement).
--
-- Alimentées par Flink en UPSERT sur la clé primaire, comme le reste
-- du schéma (CDC des tables de référence TB_REF_CENTRES_SANTE,
-- TB_REF_PROFESSIONNELS_SANTE, TB_REF_COLLECTIVITES — voir
-- connectors/debezium-postgres-json.json et flink/sql/kpi_prestations.sql).
--
-- Séparées des tables de faits (kpi_prestations_centre_jour, etc.)
-- plutôt que dupliquées dedans : une dénomination peut changer sans
-- que ça affecte les lignes déjà agrégées d'un jour passé, et la
-- jointure se fait à la lecture (vue ci-dessous), pas figée dans Flink.
--
-- Pas de contrainte de clé étrangère entre dim_centres_sante et
-- dim_collectivites : la colonne d'origine (simulateur_V5) n'en avait
-- historiquement pas non plus (voir simulateur_V5, migration
-- 20260915_0028) — on reste fidèle au comportement de la source,
-- une jointure peut légitimement ne rien retrouver.
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dim_centres_sante (
    centre_sante_code                  VARCHAR(30)     NOT NULL,
    centre_sante_denomination          VARCHAR(255)    NOT NULL,
    collectivite_code                  VARCHAR(30),
    type_etablissement_sanitaire_code  VARCHAR(30),
    maj_le                             TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_dim_centres_sante PRIMARY KEY (centre_sante_code)
);

COMMENT ON TABLE dim_centres_sante IS
    'Dimension centre de santé (dénomination), alimentée par CDC sur TB_REF_CENTRES_SANTE.';

CREATE TABLE IF NOT EXISTS dim_professionnels_sante (
    professionnel_sante_code   VARCHAR(30)     NOT NULL,
    nom                        VARCHAR(150)    NOT NULL,
    prenoms                    VARCHAR(150)    NOT NULL,
    type_code                  VARCHAR(30),
    statut                     VARCHAR(30),
    maj_le                     TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_dim_professionnels_sante PRIMARY KEY (professionnel_sante_code)
);

COMMENT ON TABLE dim_professionnels_sante IS
    'Dimension praticien (nom, prénoms séparés comme en source), alimentée par CDC sur TB_REF_PROFESSIONNELS_SANTE.';

CREATE TABLE IF NOT EXISTS dim_collectivites (
    collectivite_code           VARCHAR(30)     NOT NULL,
    collectivite_denomination   VARCHAR(150)    NOT NULL,
    collectivite_latitude       NUMERIC(9, 6),
    collectivite_longitude      NUMERIC(9, 6),
    maj_le                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_dim_collectivites PRIMARY KEY (collectivite_code)
);

COMMENT ON COLUMN dim_collectivites.collectivite_latitude IS
    'Nullable : toutes les localités n''ont pas de géocodage réussi côté simulateur (voir simulateur_V5, migration 20260914_0027).';

COMMENT ON TABLE dim_collectivites IS
    'Dimension localité (nom, coordonnées), alimentée par CDC sur TB_REF_COLLECTIVITES.';

-- ── Vue : centres avec dénomination et coordonnées ───────────────
-- LEFT JOIN, pas INNER : un centre sans collectivité connue doit
-- rester visible (juste sans position), pas disparaître du classement.
CREATE OR REPLACE VIEW v_centres_sante_geo AS
SELECT
    c.centre_sante_code,
    c.centre_sante_denomination,
    c.type_etablissement_sanitaire_code,
    g.collectivite_denomination,
    g.collectivite_latitude,
    g.collectivite_longitude
FROM dim_centres_sante AS c
LEFT JOIN dim_collectivites AS g ON g.collectivite_code = c.collectivite_code;

COMMENT ON VIEW v_centres_sante_geo IS
    'Centres de santé enrichis du nom de leur collectivité et de ses coordonnées (nullable). Jointure faite à la lecture, pas dans Flink.';

-- ── Vue : nom complet du praticien ────────────────────────────────
CREATE OR REPLACE VIEW v_professionnels_sante AS
SELECT
    professionnel_sante_code,
    nom,
    prenoms,
    nom || ' ' || prenoms AS nom_complet,
    type_code,
    statut
FROM dim_professionnels_sante;

COMMENT ON VIEW v_professionnels_sante IS
    'Praticiens avec nom complet pré-assemblé (nom || prénoms), pour éviter de refaire la concaténation dans chaque graphique Superset.';
