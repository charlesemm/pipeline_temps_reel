-- ═══════════════════════════════════════════════════════════════
-- Schéma analytique — ajout du 2026-09-21 : famille C (prescriptions, pathologies)
--
-- KPI 25, 27, 30, 31, 32, 33 de docs/kpi.md. Données de santé sensibles
-- (loi n°2013-450) : voir les hypothèses H7 à H10 de docs/kpi.md.
--
-- Trois niveaux, du moins au plus sensible :
--   1. dim_*                : référentiels (dénominations). Lisibles par la DPREST.
--   2. kpi_* et fait_*      : agrégats Flink (jour x médicament / pathologie) et
--                             copies 1:1 de la source (numéro de facture, SANS
--                             assuré). Réservés au SGD (role_qualite_nominatif).
--   3. v_*                  : vues masquées (regroupements < 5 supprimés, H10),
--                             seules exposées à la DPREST (role_kpi_lecture).
--
-- Les jointures entente -> facture -> prescription/pathologie se font ICI, à
-- la lecture, et non dans Flink : un état de jointure de plus ferait replonger
-- le TaskManager dans l'OutOfMemoryError déjà rencontré (docs/decisions.md).
--
-- Rejouable sans effet de bord (IF NOT EXISTS / OR REPLACE). Appliqué à la main
-- sur une base existante (docker-entrypoint-initdb.d ne rejoue rien).
-- ═══════════════════════════════════════════════════════════════

-- ── 1. Dimensions ──────────────────────────────────────────────
-- Clé = code seul, alors que la source versionne par (code, date de début) :
-- vérifié le 2026-09-21, aucun code n'a plusieurs versions (918 médicaments,
-- 100 pathologies, 149 DCI). Si cela change, la dernière version écrite gagne.
CREATE TABLE IF NOT EXISTS dim_medicaments (
    medicament_code             VARCHAR(30)     NOT NULL,
    medicament_denomination     VARCHAR(255)    NOT NULL,
    dci_code                    VARCHAR(30),
    maj_le                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_dim_medicaments PRIMARY KEY (medicament_code)
);

CREATE TABLE IF NOT EXISTS dim_dci (
    dci_code                    VARCHAR(30)     NOT NULL,
    dci_denomination            VARCHAR(150)    NOT NULL,
    maj_le                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_dim_dci PRIMARY KEY (dci_code)
);

CREATE TABLE IF NOT EXISTS dim_pathologies (
    pathologie_code             VARCHAR(10)     NOT NULL,
    pathologie_denomination     VARCHAR(255)    NOT NULL,
    maj_le                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_dim_pathologies PRIMARY KEY (pathologie_code)
);

-- ── 2a. Agrégats continus Flink (KPI 25, 27, 30) ───────────────
-- Additifs : semaine et mois se déduisent par simple somme. Le classement
-- n'est PAS stocké (il se calcule à la lecture, sur n'importe quelle période).
CREATE TABLE IF NOT EXISTS kpi_prescriptions_medicament_jour (
    jour                        DATE            NOT NULL,
    medicament_code             VARCHAR(30)     NOT NULL,
    nombre_prescriptions        BIGINT          NOT NULL,
    maj_le                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_kpi_prescriptions_medicament_jour PRIMARY KEY (jour, medicament_code)
);

CREATE TABLE IF NOT EXISTS kpi_pathologies_jour (
    jour                        DATE            NOT NULL,
    pathologie_code             VARCHAR(10)     NOT NULL,
    nombre_pathologies          BIGINT          NOT NULL,
    maj_le                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_kpi_pathologies_jour PRIMARY KEY (jour, pathologie_code)
);

COMMENT ON TABLE kpi_prescriptions_medicament_jour IS
    'KPI 25 et 27 : lignes de prescription par jour (DATE_DEBUT, H9) et médicament. Réservée au SGD.';
COMMENT ON TABLE kpi_pathologies_jour IS
    'KPI 30 : pathologies déclarées par jour (PATHOLOGIE_DATE_DEBUT, H9) et pathologie. Réservée au SGD.';

-- ── 2b. Faits au niveau ligne (KPI 31 à 33) ────────────────────
-- Copie 1:1 de la source, clé = clé source. Le numéro de facture est la seule
-- clé de rattachement : ni l'assuré, ni le centre, ni le praticien ne sont
-- copiés ici. Réservées au SGD.
CREATE TABLE IF NOT EXISTS fait_prescriptions (
    facture_numero              VARCHAR(50)     NOT NULL,
    prescription_code           VARCHAR(30)     NOT NULL,
    date_debut                  DATE            NOT NULL,
    maj_le                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_fait_prescriptions PRIMARY KEY (facture_numero, prescription_code, date_debut)
);

CREATE TABLE IF NOT EXISTS fait_pathologies (
    facture_numero              VARCHAR(50)     NOT NULL,
    pathologie_code             VARCHAR(10)     NOT NULL,
    date_debut                  DATE            NOT NULL,
    maj_le                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_fait_pathologies PRIMARY KEY (facture_numero, pathologie_code, date_debut)
);

CREATE TABLE IF NOT EXISTS fait_entente_facture (
    entente_prealable_id        INTEGER         NOT NULL,
    facture_numero              VARCHAR(50),
    type_demande_code           VARCHAR(30),
    date_debut                  DATE,
    maj_le                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_fait_entente_facture PRIMARY KEY (entente_prealable_id)
);

-- Clé réduite à l'entente seule : hypothèse H5 de docs/kpi.md (un seul statut
-- terminal par entente), même choix que la source ep_statuts_src de Flink.
CREATE TABLE IF NOT EXISTS fait_entente_statut (
    entente_prealable_id        INTEGER         NOT NULL,
    statut_code                 VARCHAR(30),
    maj_le                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_fait_entente_statut PRIMARY KEY (entente_prealable_id)
);

CREATE INDEX IF NOT EXISTS idx_fait_prescriptions_facture ON fait_prescriptions (facture_numero);
CREATE INDEX IF NOT EXISTS idx_fait_pathologies_facture   ON fait_pathologies (facture_numero);
CREATE INDEX IF NOT EXISTS idx_fait_entente_facture_fact  ON fait_entente_facture (facture_numero);

COMMENT ON TABLE fait_prescriptions IS
    'Une ligne par prescription (TB_FACTURES_PRESCRIPTIONS). Sans assuré. Réservée au SGD.';
COMMENT ON TABLE fait_pathologies IS
    'Une ligne par pathologie déclarée (TB_FACTURES_PATHOLOGIES). Sans assuré. Réservée au SGD.';
COMMENT ON TABLE fait_entente_facture IS
    'Lien entente préalable -> facture (H7 de docs/kpi.md). Réservée au SGD.';
COMMENT ON TABLE fait_entente_statut IS
    'Statut de chaque entente (TB_ENTENTES_PREALABLES_STATUTS). Absent = « sans réponse » (H4).';

-- ── 3. Vues ────────────────────────────────────────────────────

-- Vue INTERNE (non accordée à la DPREST) : une ligne par entente, avec le nombre
-- de prescriptions et de pathologies de sa facture. Pré-agrégées par facture
-- avant la jointure, pour ne pas multiplier les lignes (une facture porte
-- plusieurs pathologies).
CREATE OR REPLACE VIEW v_ep_clinique_detail AS
WITH presc AS (
    SELECT
        facture_numero,
        COUNT(*)                                AS nombre_prescriptions,
        COUNT(DISTINCT prescription_code)       AS nombre_medicaments_distincts
    FROM fait_prescriptions
    GROUP BY facture_numero
),
patho AS (
    SELECT
        facture_numero,
        COUNT(*)                                AS nombre_pathologies
    FROM fait_pathologies
    GROUP BY facture_numero
)
SELECT
    e.entente_prealable_id,
    e.facture_numero,
    COALESCE(e.type_demande_code, '(inconnu)')  AS type_demande_code,
    COALESCE(s.statut_code, 'sans_reponse')     AS statut_code,
    COALESCE(p.nombre_prescriptions, 0)         AS nombre_prescriptions,
    COALESCE(p.nombre_medicaments_distincts, 0) AS nombre_medicaments_distincts,
    COALESCE(g.nombre_pathologies, 0)           AS nombre_pathologies
FROM fait_entente_facture AS e
LEFT JOIN fait_entente_statut AS s ON s.entente_prealable_id = e.entente_prealable_id
LEFT JOIN presc               AS p ON p.facture_numero       = e.facture_numero
LEFT JOIN patho               AS g ON g.facture_numero       = e.facture_numero;

COMMENT ON VIEW v_ep_clinique_detail IS
    'Interne SGD : une ligne par entente avec les effectifs cliniques de sa facture (H7). Non masquée, non accordée à la DPREST.';

-- KPI 25 : médicaments prescrits par jour. Ajoutée après coup : la table Flink
-- (kpi_prescriptions_medicament_jour) est réservée au SGD, donc la connexion
-- Superset (compte dprest_lecture) ne pouvait pas la lire. Le total journalier
-- n'est pas identifiant ; les jours à moins de 5 prescriptions restent masqués (H10).
CREATE OR REPLACE VIEW v_kpi_prescriptions_jour AS
SELECT
    jour,
    SUM(nombre_prescriptions)           AS nombre_prescriptions,
    COUNT(DISTINCT medicament_code)     AS medicaments_distincts
FROM kpi_prescriptions_medicament_jour
GROUP BY jour
HAVING SUM(nombre_prescriptions) >= 5;

-- KPI 27 : top 10 des médicaments (toute période ; pour une période libre,
-- interroger kpi_prescriptions_medicament_jour côté SGD). Masquage H10.
CREATE OR REPLACE VIEW v_top10_medicaments AS
SELECT
    k.medicament_code,
    COALESCE(m.medicament_denomination, '(inconnu)')    AS medicament_denomination,
    SUM(k.nombre_prescriptions)                         AS nombre_prescriptions
FROM kpi_prescriptions_medicament_jour AS k
LEFT JOIN dim_medicaments AS m ON m.medicament_code = k.medicament_code
GROUP BY k.medicament_code, m.medicament_denomination
HAVING SUM(k.nombre_prescriptions) >= 5
ORDER BY nombre_prescriptions DESC, k.medicament_code
LIMIT 10;

-- KPI 30 : top 10 des pathologies. Masquage H10.
CREATE OR REPLACE VIEW v_top10_pathologies AS
SELECT
    k.pathologie_code,
    COALESCE(d.pathologie_denomination, '(inconnue)')   AS pathologie_denomination,
    SUM(k.nombre_pathologies)                           AS nombre_pathologies
FROM kpi_pathologies_jour AS k
LEFT JOIN dim_pathologies AS d ON d.pathologie_code = k.pathologie_code
GROUP BY k.pathologie_code, d.pathologie_denomination
HAVING SUM(k.nombre_pathologies) >= 5
ORDER BY nombre_pathologies DESC, k.pathologie_code
LIMIT 10;

-- KPI 31 : part des ententes dont la facture porte au moins une prescription.
CREATE OR REPLACE VIEW v_kpi_ep_prescriptions AS
SELECT
    type_demande_code,
    statut_code,
    COUNT(*)                                                        AS nombre_ententes,
    COUNT(*) FILTER (WHERE nombre_prescriptions > 0)                AS ententes_avec_prescription,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE nombre_prescriptions > 0) / COUNT(*)
    , 1)                                                            AS taux_avec_prescription
FROM v_ep_clinique_detail
GROUP BY type_demande_code, statut_code
HAVING COUNT(*) >= 5;

-- KPI 32 : volume de médicaments prescrits sur les ententes.
CREATE OR REPLACE VIEW v_kpi_ep_medicaments AS
SELECT
    type_demande_code,
    statut_code,
    COUNT(*)                                                        AS nombre_ententes,
    SUM(nombre_prescriptions)                                       AS nombre_medicaments_prescrits,
    ROUND(SUM(nombre_prescriptions)::numeric / COUNT(*), 2)         AS medicaments_par_entente
FROM v_ep_clinique_detail
GROUP BY type_demande_code, statut_code
HAVING COUNT(*) >= 5;

-- KPI 33 : pathologies des ententes, par statut. Corrélation, pas explication
-- (docs/kpi.md, note du KPI 33). Une pathologie est comptée une fois par
-- entente : les cellules < 5 sont masquées (H10).
CREATE OR REPLACE VIEW v_kpi_ep_pathologies AS
SELECT
    e.statut_code,
    g.pathologie_code,
    COALESCE(d.pathologie_denomination, '(inconnue)')   AS pathologie_denomination,
    COUNT(*)                                            AS nombre_pathologies
FROM v_ep_clinique_detail AS e
JOIN fait_pathologies     AS g ON g.facture_numero   = e.facture_numero
LEFT JOIN dim_pathologies AS d ON d.pathologie_code  = g.pathologie_code
GROUP BY e.statut_code, g.pathologie_code, d.pathologie_denomination
HAVING COUNT(*) >= 5;

COMMENT ON VIEW v_kpi_prescriptions_jour IS 'KPI 25, masquée (H10).';
COMMENT ON VIEW v_top10_medicaments IS 'KPI 27, masquée (H10).';
COMMENT ON VIEW v_top10_pathologies IS 'KPI 30, masquée (H10).';
COMMENT ON VIEW v_kpi_ep_prescriptions IS 'KPI 31, masquée (H10).';
COMMENT ON VIEW v_kpi_ep_medicaments IS 'KPI 32, masquée (H10).';
COMMENT ON VIEW v_kpi_ep_pathologies IS 'KPI 33, masquée (H10).';

-- ── 4. Droits (rôles créés par 009_roles_acces.sql) ────────────
-- Conditionnels : si 009 n'a pas été appliqué, ce script reste rejouable.
-- Les vues s'exécutent avec les droits de leur propriétaire : la DPREST lit
-- les vues masquées SANS avoir accès aux tables fait_* / kpi_* sous-jacentes.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_kpi_lecture') THEN
        GRANT SELECT ON
            dim_medicaments, dim_dci, dim_pathologies,
            v_kpi_prescriptions_jour,
            v_top10_medicaments, v_top10_pathologies,
            v_kpi_ep_prescriptions, v_kpi_ep_medicaments, v_kpi_ep_pathologies
        TO role_kpi_lecture;
    END IF;

    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_qualite_nominatif') THEN
        GRANT SELECT ON
            kpi_prescriptions_medicament_jour, kpi_pathologies_jour,
            fait_prescriptions, fait_pathologies,
            fait_entente_facture, fait_entente_statut,
            v_ep_clinique_detail
        TO role_qualite_nominatif;
    END IF;

    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_flink_ecriture') THEN
        GRANT SELECT, INSERT, UPDATE, DELETE ON
            dim_medicaments, dim_dci, dim_pathologies,
            kpi_prescriptions_medicament_jour, kpi_pathologies_jour,
            fait_prescriptions, fait_pathologies,
            fait_entente_facture, fait_entente_statut
        TO role_flink_ecriture;
    END IF;
END
$$;

-- ── 5. Correctif : dprest_lecture ne doit PAS lire les tables sensibles ──
-- Un droit par défaut (ALTER DEFAULT PRIVILEGES ... GRANT SELECT ON TABLES TO
-- dprest_lecture) avait été posé le 2026-09-21, avant l'étape 7d : il donnait
-- SELECT sur TOUTE nouvelle table créée par dprest, donc ici sur les tables de
-- la famille C, ce qui contourne le masquage (H10). Les droits passent
-- désormais uniquement par les rôles de groupe de 009 (role_kpi_lecture). On
-- retire le droit par défaut et on révoque explicitement sur les objets
-- sensibles (idempotent : sans effet si le droit n'existe pas).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dprest_lecture') THEN
        ALTER DEFAULT PRIVILEGES FOR ROLE dprest IN SCHEMA public
            REVOKE SELECT ON TABLES FROM dprest_lecture;
        REVOKE SELECT ON
            kpi_prescriptions_medicament_jour, kpi_pathologies_jour,
            fait_prescriptions, fait_pathologies,
            fait_entente_facture, fait_entente_statut,
            v_ep_clinique_detail
        FROM dprest_lecture;
    END IF;
END
$$;
