-- ═══════════════════════════════════════════════════════════════
-- Schéma analytique — ajout du 2026-09-17
--
-- Corrige le KPI 20 (docs/kpi.md) : « Activité par praticien-conseil »
-- doit être restreint aux agents de type `medecin_conseil`
-- (`TB_REF_AGENTS.AGENT_TYPE_CODE`), pas comptabiliser tous les agents
-- ayant répondu à une entente préalable (accueil, medecin_conseil,
-- autre confondus). `TB_REF_AGENTS` n'était jusqu'ici pas répliquée
-- par CDC (absente de `table.include.list`) — le filtre était donc
-- structurellement impossible, pas seulement oublié côté Superset.
--
-- Même mécanisme que les autres dimensions (004_dim_referentiels.sql) :
-- alimentée par CDC, jointe à la lecture (vue ci-dessous), pas dans
-- Flink — cohérent avec le choix déjà documenté pour
-- dim_centres_sante/dim_professionnels_sante (docs/decisions.md,
-- 2026-09-15).
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS dim_agents (
    agent_code       VARCHAR(30)     NOT NULL,
    agent_nom        VARCHAR(150)    NOT NULL,
    agent_prenoms    VARCHAR(150)    NOT NULL,
    agent_type_code  VARCHAR(30)     NOT NULL,
    maj_le           TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_dim_agents PRIMARY KEY (agent_code)
);

-- Correction du 2026-09-17 (même jour, avant toute utilisation réelle) :
-- version initiale de cette table oubliait AGENT_NOM/AGENT_PRENOMS,
-- pourtant présents sur TB_REF_AGENTS (repéré via l'instantané Debezium)
-- — le graphique Superset n'affichait alors que des codes, pas des noms,
-- contrairement à dim_professionnels_sante qui a le même besoin.
ALTER TABLE dim_agents ADD COLUMN IF NOT EXISTS agent_nom     VARCHAR(150) NOT NULL DEFAULT '';
ALTER TABLE dim_agents ADD COLUMN IF NOT EXISTS agent_prenoms VARCHAR(150) NOT NULL DEFAULT '';
ALTER TABLE dim_agents ALTER COLUMN agent_nom     DROP DEFAULT;
ALTER TABLE dim_agents ALTER COLUMN agent_prenoms DROP DEFAULT;

COMMENT ON TABLE dim_agents IS
    'Dimension agent CNAM (accueil | medecin_conseil | autre), alimentée par CDC sur TB_REF_AGENTS. Sert à restreindre le KPI 20 aux seuls médecins-conseils et à afficher leur nom.';
COMMENT ON COLUMN dim_agents.agent_type_code IS
    'accueil | medecin_conseil | autre — contrainte CHECK en base côté source (voir docs/dictionnaire_donnees.md).';

-- ── Vue : activité des praticiens-conseils, filtrée par type ─────
-- Remplace l'usage direct de kpi_ententes_prealables_agent_jour dans
-- le graphique Superset « Activité par praticien-conseil » (KPI 20) :
-- cette table brute mélange tous les types d'agent et ne porte que le
-- code, cette vue filtre sur medecin_conseil ET ajoute le nom complet,
-- comme l'exige la définition du KPI et par cohérence avec
-- v_professionnels_sante (même besoin, même solution).
CREATE OR REPLACE VIEW v_kpi_ep_agent_medecin_conseil AS
SELECT
    a.jour,
    a.agent_code,
    d.agent_nom || ' ' || d.agent_prenoms AS agent_nom_complet,
    a.statut_code,
    a.nombre_ententes
FROM kpi_ententes_prealables_agent_jour AS a
JOIN dim_agents AS d ON d.agent_code = a.agent_code
WHERE d.agent_type_code = 'medecin_conseil';

COMMENT ON VIEW v_kpi_ep_agent_medecin_conseil IS
    'KPI 20 correctement scopé : kpi_ententes_prealables_agent_jour restreint aux agents de type medecin_conseil. À utiliser à la place de la table brute dans Superset (voir docs/guides/etape6b_superset_ententes.md, correction du 2026-09-17).';

-- Droit de lecture (à exécuter à la main, comme le reste des GRANT de
-- ce projet — voir 004_dim_referentiels.sql, 005_qualite_anomalies.sql) :
--   GRANT SELECT ON dim_agents TO dprest_lecture;
--   GRANT SELECT ON v_kpi_ep_agent_medecin_conseil TO dprest_lecture;
