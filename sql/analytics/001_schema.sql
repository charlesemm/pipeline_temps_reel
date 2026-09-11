-- ═══════════════════════════════════════════════════════════════
-- Schéma analytique cible — KPI DPREST
--
-- Alimenté par Flink en UPSERT sur la clé primaire : la ligne de la
-- période en cours est réécrite à chaque nouvel événement (temps réel),
-- les lignes des périodes passées ne bougent plus. Rejouer le flux
-- depuis le début ne duplique donc aucune ligne — c'est l'idempotence
-- exigée par CLAUDE.md.
--
-- Le grain de base est le JOUR. Semaine, mois et plages libres se
-- dérivent par simple regroupement SQL (DATE_TRUNC), sans recalcul
-- côté Flink — voir docs/kpi.md.
--
-- Convention : les dimensions nulles côté source sont remplacées par
-- '(inconnu)' avant écriture (une clé primaire n'accepte pas NULL).
-- ═══════════════════════════════════════════════════════════════

-- ── Famille A : prestations et facturation ──────────────────────
-- KPI 1, 3, 6, 7, 8, 9, 12, 13, 14, 15 de docs/kpi.md.
-- Cardinalité maîtrisée : 4 types d'acte × 2 types de facture
-- × 2 régimes × 6 types de centre = 96 lignes par jour au maximum.
CREATE TABLE IF NOT EXISTS kpi_prestations_jour (
    jour                        DATE            NOT NULL,
    prestation_code             VARCHAR(30)     NOT NULL,
    type_facture_code           VARCHAR(30)     NOT NULL,
    regime_code                 VARCHAR(30)     NOT NULL,
    centre_sante_type_libelle   VARCHAR(255)    NOT NULL,
    nombre_prestations          BIGINT          NOT NULL,
    montant_depense             NUMERIC(18, 2)  NOT NULL,
    montant_pris_en_charge      NUMERIC(18, 2)  NOT NULL,
    montant_reste_a_charge      NUMERIC(18, 2)  NOT NULL,
    maj_le                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_kpi_prestations_jour PRIMARY KEY (
        jour, prestation_code, type_facture_code, regime_code, centre_sante_type_libelle
    )
);

COMMENT ON TABLE kpi_prestations_jour IS
    'Activité de prestations agrégée au jour, datée par FACTURE_DATE_SOINS (hypothèse H2 de docs/kpi.md).';
COMMENT ON COLUMN kpi_prestations_jour.montant_pris_en_charge IS
    'Somme de PRESTATION_MONTANT_RQ : part financée par la CMU.';
COMMENT ON COLUMN kpi_prestations_jour.montant_reste_a_charge IS
    'Somme de PRESTATION_MONTANT_ASSURE : part restant à la charge de l''assuré.';

CREATE INDEX IF NOT EXISTS idx_kpi_prestations_jour_jour
    ON kpi_prestations_jour (jour);

-- ── Famille A (suite) : classements par centre et par praticien ──
-- KPI 10 et 11. Tables séparées pour éviter de faire exploser la
-- cardinalité de la table principale (30 centres × 150 praticiens
-- multiplieraient inutilement chaque combinaison de dimensions).
CREATE TABLE IF NOT EXISTS kpi_prestations_centre_jour (
    jour                        DATE            NOT NULL,
    centre_sante_code           VARCHAR(30)     NOT NULL,
    nombre_prestations          BIGINT          NOT NULL,
    montant_depense             NUMERIC(18, 2)  NOT NULL,
    maj_le                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_kpi_prestations_centre_jour PRIMARY KEY (jour, centre_sante_code)
);

CREATE TABLE IF NOT EXISTS kpi_prestations_praticien_jour (
    jour                        DATE            NOT NULL,
    professionnel_sante_code    VARCHAR(30)     NOT NULL,
    nombre_prestations          BIGINT          NOT NULL,
    montant_depense             NUMERIC(18, 2)  NOT NULL,
    maj_le                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_kpi_prestations_praticien_jour PRIMARY KEY (jour, professionnel_sante_code)
);

-- ── Famille B : ententes préalables, suivi continu ───────────────
-- KPI 16, 18, 19, 21 de docs/kpi.md. Le taux de réponse (KPI 17) se
-- calcule à la lecture, à partir du nombre d'EP sans statut.
CREATE TABLE IF NOT EXISTS kpi_ententes_prealables_jour (
    jour                        DATE            NOT NULL,
    statut_code                 VARCHAR(30)     NOT NULL,
    type_demande_code           VARCHAR(30)     NOT NULL,
    nombre_ententes             BIGINT          NOT NULL,
    delai_moyen_jours           NUMERIC(10, 2),
    montant_engage_cmu          NUMERIC(18, 2),
    maj_le                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_kpi_ep_jour PRIMARY KEY (jour, statut_code, type_demande_code)
);

COMMENT ON COLUMN kpi_ententes_prealables_jour.statut_code IS
    'acceptee | refusee | validee_office | sans_reponse (hypothèse H4 de docs/kpi.md).';
COMMENT ON COLUMN kpi_ententes_prealables_jour.delai_moyen_jours IS
    'Délai entre la demande et sa réponse, en jours pleins (limite du schéma source, hypothèse H3).';

-- ── Famille B (suite) : activité des praticiens-conseils ─────────
-- KPI 20. Exclut les EP validées d'office, qui ne portent aucun
-- praticien identifiable (validation automatique).
CREATE TABLE IF NOT EXISTS kpi_ententes_prealables_agent_jour (
    jour                        DATE            NOT NULL,
    agent_code                  VARCHAR(30)     NOT NULL,
    statut_code                 VARCHAR(30)     NOT NULL,
    nombre_ententes             BIGINT          NOT NULL,
    maj_le                      TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_kpi_ep_agent_jour PRIMARY KEY (jour, agent_code, statut_code)
);

-- ── Famille B (suite) : KPI mensuel certifié ─────────────────────
-- Alimentée par une fenêtre Flink FERMÉE : publiée une seule fois, à
-- la clôture du mois, quand le chiffre est complet et définitif.
-- Répond à l'exigence réglementaire « avant le 5 du mois » du
-- périmètre DPREST. À distinguer de kpi_ententes_prealables_jour,
-- qui donne la même information en continu mais provisoire.
CREATE TABLE IF NOT EXISTS kpi_ententes_prealables_mois (
    mois                        DATE            NOT NULL,
    statut_code                 VARCHAR(30)     NOT NULL,
    nombre_ententes             BIGINT          NOT NULL,
    delai_moyen_jours           NUMERIC(10, 2),
    cloture_le                  TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_kpi_ep_mois PRIMARY KEY (mois, statut_code)
);

COMMENT ON TABLE kpi_ententes_prealables_mois IS
    'KPI mensuel certifié : publié à la clôture de la fenêtre mensuelle Flink, chiffre définitif.';
