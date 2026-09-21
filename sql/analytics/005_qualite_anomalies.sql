-- ═══════════════════════════════════════════════════════════════
-- Schéma analytique — ajout du 2026-09-17
--
-- Table de quarantaine des anomalies de qualité, alimentée par Flink
-- (flink/sql/kpi_prestations.sql). Comble un manque réel : le job Flink
-- actuel absorbe silencieusement les valeurs incohérentes (COALESCE
-- vers 0 ou '(inconnu)') au lieu de les isoler, contrairement à la
-- règle d'ingénierie de CLAUDE.md (« isole les rejets, ne les ignore
-- pas silencieusement »). Un prototype antérieur avait ce mécanisme
-- (voir flink/sql/pipeline_kpi_hebdo.sql, sink qualite.prestations_rejetees)
-- mais il n'avait pas été repris dans le job final.
--
-- Une seule table, générique par domaine, plutôt qu'une table par
-- domaine (prestation / facture / entente préalable) : les colonnes
-- utiles diffèrent d'un domaine à l'autre, et une table par domaine
-- aurait fini avec la plupart des colonnes NULL selon le cas. La ligne
-- source complète est conservée dans `donnee_brute` (JSON en texte)
-- pour répondre au besoin de diagnostic : voir une anomalie, c'est
-- pouvoir en inspecter TOUTE la ligne d'origine, pas seulement la
-- valeur fautive.
--
-- `donnee_brute` en TEXT, pas JSONB : le connecteur JDBC de Flink
-- écrit une valeur STRING classique ; un DDL déclaré JSONB imposerait
-- un cast explicite côté pilote JDBC (piège de mapping de type déjà
-- rencontré ailleurs dans ce projet, voir docs/decisions.md) pour un
-- bénéfice nul ici : personne n'interroge l'intérieur de ce JSON en
-- SQL, il est seulement affiché tel quel dans Superset. Si un jour ça
-- change, caster à la lecture (`donnee_brute::jsonb`) suffit.
--
-- Limite connue : upsert JDBC sur (domaine, cle_metier, motif_anomalie).
-- Si la ligne source est corrigée après coup, l'anomalie déjà détectée
-- reste dans cette table (les vues d'entrée ne réémettent pas de ligne
-- de rétractation pour un cas qui a cessé d'être anormal) — utile ici
-- (garder la trace qu'une anomalie EST arrivée), mais à savoir avant de
-- lire cette table comme "anomalies actuellement non corrigées".
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS qualite_anomalies (
    domaine             VARCHAR(30)     NOT NULL,
    motif_anomalie      VARCHAR(50)     NOT NULL,
    cle_metier          VARCHAR(100)    NOT NULL,
    donnee_brute        TEXT            NOT NULL,
    detecte_le          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    CONSTRAINT pk_qualite_anomalies PRIMARY KEY (domaine, cle_metier, motif_anomalie)
);

COMMENT ON TABLE qualite_anomalies IS
    'Quarantaine des lignes anormales détectées par Flink (montant manquant/négatif, date de soins manquante, ententes préalables incohérentes...) — voir flink/sql/kpi_prestations.sql.';
COMMENT ON COLUMN qualite_anomalies.domaine IS
    'prestation | facture | entente_prealable.';
COMMENT ON COLUMN qualite_anomalies.cle_metier IS
    'Identifiant métier de la ligne fautive (ex. facture_numero/prestation_code, ou entente_prealable_id) — pour retrouver la ligne source.';
COMMENT ON COLUMN qualite_anomalies.donnee_brute IS
    'La ligne source complète, telle que reçue par Flink (avant toute conversion COALESCE), au format JSON en texte.';

CREATE INDEX IF NOT EXISTS idx_qualite_anomalies_detecte_le
    ON qualite_anomalies (detecte_le);

-- Droit de lecture pour Grafana/Superset (compte lecture seule, voir
-- docs/decisions.md § comptes par service) :
--   GRANT SELECT ON qualite_anomalies TO dprest_lecture;
-- Volontairement PAS ajoutée ici en tant que GRANT automatique : à
-- exécuter à la main, comme le reste des GRANT de ce projet (voir
-- 004_dim_referentiels.sql, aucun GRANT inline non plus).
--
-- Volontairement PAS à ajouter à l'accès du rôle Gamma (compte
-- `dprest`) côté Superset : cette table expose des lignes brutes
-- (personne_uuid, montants individuels) à des fins de diagnostic
-- technique, pas un KPI agrégé destiné à la DPREST. Réservez le
-- dashboard qui l'utilise aux comptes Admin/Alpha (SGD) — voir
-- docs/guides/etape6c_superset_qualite.md.
