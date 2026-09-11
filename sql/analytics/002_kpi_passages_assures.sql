-- ═══════════════════════════════════════════════════════════════
-- Schéma analytique — ajout du 2026-09-11
--
-- KPI 2/4 fusionnés : nombre de passages (hypothèse H6 : un passage =
--   une facture ; un assuré qui revient est compté à chaque venue).
-- KPI 23/24 : top 10 des assurés par prestations servies, au global
--   et par centre de santé.
--
-- Uniquement des créations : aucune table existante n'est modifiée.
-- Rejouable sans effet de bord (IF NOT EXISTS / OR REPLACE).
--
-- Appliqué automatiquement à la création du volume ; sur une base déjà
-- existante, l'appliquer à la main (voir docs/guides/etape4_kpi_postgresql.md).
-- ═══════════════════════════════════════════════════════════════

-- ── KPI 2/4 : passages (factures) par jour ─────────────────────
-- Additif : un passage n'a qu'une date et un jeu de dimensions, donc
-- semaine et mois se déduisent par simple somme.
CREATE TABLE IF NOT EXISTS kpi_factures_jour (
    jour                        DATE            NOT NULL,
    type_facture_code           VARCHAR(30)     NOT NULL,
    regime_code                 VARCHAR(30)     NOT NULL,
    centre_sante_type_libelle   VARCHAR(255)    NOT NULL,
    nombre_factures             BIGINT          NOT NULL,
    maj_le                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_kpi_factures_jour PRIMARY KEY (
        jour, type_facture_code, regime_code, centre_sante_type_libelle
    )
);

COMMENT ON TABLE kpi_factures_jour IS
    'Nombre de passages (= factures, hypothèse H6 de docs/kpi.md) par jour de soins.';

-- ── KPI 23/24 : prestations par assuré, grain jour × assuré × centre ──
-- Le CLASSEMENT n'est pas stocké : il se calcule à la lecture, ce qui
-- permet un top 10 sur n'importe quelle période (un classement n'est
-- pas additif — le top de la semaine n'est pas la somme des tops
-- journaliers).
--
-- Donnée de santé individuelle : seul l'identifiant opaque
-- PERSONNE_UUID est conservé, jamais de nom ni de matricule. En
-- production, l'accès à cette table doit être restreint par rôle
-- (étape 7, loi n°2013-450).
CREATE TABLE IF NOT EXISTS kpi_prestations_assure_jour (
    jour                        DATE            NOT NULL,
    personne_uuid               VARCHAR(36)     NOT NULL,
    centre_sante_code           VARCHAR(30)     NOT NULL,
    nombre_prestations          BIGINT          NOT NULL,
    montant_depense             NUMERIC(18, 2)  NOT NULL,
    maj_le                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_kpi_prestations_assure_jour PRIMARY KEY (
        jour, personne_uuid, centre_sante_code
    )
);

COMMENT ON TABLE kpi_prestations_assure_jour IS
    'Prestations servies par assuré (pseudonymisé) et par centre, au jour. Accès à restreindre par rôle.';

-- ── Vues de classement, sur tout l'historique ───────────────────
-- RANK et non ROW_NUMBER : les ex-aequo sont tous affichés, le top
-- peut donc dépasser 10 lignes. Couper arbitrairement à 10 serait
-- trompeur (toutes les prestations valent 10 000 F : le montant ne
-- départage personne). Voir docs/kpi.md, KPI 23-24.
-- Pour une période donnée, Superset applique la même requête avec un
-- filtre sur `jour`.
CREATE OR REPLACE VIEW v_top10_assures AS
WITH totaux AS (
    SELECT
        personne_uuid,
        SUM(nombre_prestations) AS nombre_prestations,
        SUM(montant_depense)    AS montant_depense
    FROM kpi_prestations_assure_jour
    GROUP BY personne_uuid
),
classement AS (
    SELECT
        RANK() OVER (ORDER BY nombre_prestations DESC) AS rang,
        personne_uuid,
        nombre_prestations,
        montant_depense
    FROM totaux
)
SELECT
    rang,
    personne_uuid,
    nombre_prestations,
    montant_depense
FROM classement
WHERE rang <= 10;

CREATE OR REPLACE VIEW v_top10_assures_par_centre AS
WITH totaux AS (
    SELECT
        centre_sante_code,
        personne_uuid,
        SUM(nombre_prestations) AS nombre_prestations,
        SUM(montant_depense)    AS montant_depense
    FROM kpi_prestations_assure_jour
    GROUP BY centre_sante_code, personne_uuid
),
classement AS (
    SELECT
        centre_sante_code,
        RANK() OVER (
            PARTITION BY centre_sante_code
            ORDER BY nombre_prestations DESC
        ) AS rang,
        personne_uuid,
        nombre_prestations,
        montant_depense
    FROM totaux
)
SELECT
    centre_sante_code,
    rang,
    personne_uuid,
    nombre_prestations,
    montant_depense
FROM classement
WHERE rang <= 10;
