-- ═══════════════════════════════════════════════════════════════
-- Schéma analytique — ajout du 2026-09-17
--
-- KPI 19 (délai moyen de traitement d'une entente préalable) : passage
-- de l'unité JOUR à l'unité HEURE. H3 de docs/kpi.md (« délai mesuré en
-- jours pleins ») donnait 0 pour la quasi-totalité des EP : le
-- simulateur traite chaque demande en quelques minutes/heures, sans
-- jamais franchir minuit — 13 339 EP sur 13 341 mesurées à exactement
-- 0 jour plein le 2026-09-17 (voir docs/decisions.md pour le détail de
-- cette vérification). Pas un bug de calcul : l'unité choisie était
-- trop grossière pour un pipeline qui traite en temps réel.
-- ═══════════════════════════════════════════════════════════════

ALTER TABLE kpi_ententes_prealables_jour
    RENAME COLUMN delai_moyen_jours TO delai_moyen_heures;

COMMENT ON COLUMN kpi_ententes_prealables_jour.delai_moyen_heures IS
    'Délai entre la demande et sa réponse, en heures pleines (hypothèse H3 révisée le 2026-09-17 — anciennement en jours, voir docs/decisions.md).';

-- La vue dépend de la colonne renommée ci-dessus : refaite entièrement
-- (DROP puis CREATE, pas CREATE OR REPLACE) car le nom de colonne en
-- sortie change aussi, ce que PostgreSQL refuse sur un simple REPLACE.
DROP VIEW IF EXISTS v_kpi_ententes_prealables_mois;

CREATE VIEW v_kpi_ententes_prealables_mois AS
SELECT
    DATE_TRUNC('month', jour)::date                                AS mois,
    statut_code,
    SUM(nombre_ententes)                                           AS nombre_ententes,
    ROUND(
        SUM(COALESCE(delai_moyen_heures, 0) * nombre_ententes)
        / NULLIF(SUM(CASE WHEN delai_moyen_heures IS NOT NULL THEN nombre_ententes ELSE 0 END), 0)
    , 2)                                                            AS delai_moyen_heures,
    SUM(montant_engage_cmu)                                        AS montant_engage_cmu
FROM kpi_ententes_prealables_jour
WHERE jour < DATE_TRUNC('month', CURRENT_DATE)
GROUP BY 1, 2;

COMMENT ON VIEW v_kpi_ententes_prealables_mois IS
    'KPI mensuel certifié (KPI 16-19, 21 de docs/kpi.md) : uniquement les mois entièrement clos. Dérivé du grain jour, pas alimenté par Flink — voir docs/decisions.md. Délai en heures depuis le 2026-09-17.';

GRANT SELECT ON v_kpi_ententes_prealables_mois TO dprest_lecture;
