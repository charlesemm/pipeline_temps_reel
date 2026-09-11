-- ═══════════════════════════════════════════════════════════════
-- Schéma analytique — ajout du 2026-09-11
--
-- KPI mensuel certifié des ententes préalables (KPI 16-19, 21), sous
-- forme de VUE dérivée du grain journalier plutôt que d'une fenêtre
-- Flink fermée, initialement prévue dans 001_schema.sql
-- (`kpi_ententes_prealables_mois`, table physique laissée en place
-- mais non alimentée — voir docs/decisions.md pour la justification
-- de ce changement d'approche).
--
-- « Certifié » = ne porte que sur des mois entièrement clos : le mois
-- en cours est exclu par construction (WHERE), jamais recalculé au
-- fil de l'eau. Comme kpi_ententes_prealables_jour n'est plus modifié
-- une fois le jour passé, cette vue est stable pour un mois clos.
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW v_kpi_ententes_prealables_mois AS
SELECT
    DATE_TRUNC('month', jour)::date                                AS mois,
    statut_code,
    SUM(nombre_ententes)                                           AS nombre_ententes,
    ROUND(
        SUM(COALESCE(delai_moyen_jours, 0) * nombre_ententes)
        / NULLIF(SUM(CASE WHEN delai_moyen_jours IS NOT NULL THEN nombre_ententes ELSE 0 END), 0)
    , 2)                                                            AS delai_moyen_jours,
    SUM(montant_engage_cmu)                                        AS montant_engage_cmu
FROM kpi_ententes_prealables_jour
WHERE jour < DATE_TRUNC('month', CURRENT_DATE)
GROUP BY 1, 2;

COMMENT ON VIEW v_kpi_ententes_prealables_mois IS
    'KPI mensuel certifié (KPI 16-19, 21 de docs/kpi.md) : uniquement les mois entièrement clos. Dérivé du grain jour, pas alimenté par Flink — voir docs/decisions.md.';
