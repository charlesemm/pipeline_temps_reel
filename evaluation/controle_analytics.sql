-- Requêtes de contrôle sur la BASE ANALYTIQUE (KPI calculés par Flink).
-- À comparer avec evaluation/controle_source.sql, requête par requête :
-- les numéros de section se correspondent.
-- Voir docs/guides/verification.md.

\echo '=== 2. Totaux financiers (a comparer avec la source) ==='
SELECT
    SUM(nombre_prestations)     AS nombre_prestations,
    SUM(montant_depense)        AS montant_facture,
    SUM(montant_pris_en_charge) AS pris_en_charge_cmu,
    SUM(montant_reste_a_charge) AS reste_a_charge_assure
FROM kpi_prestations_jour;

\echo ''
\echo '=== 3. Repartition par type acte / type facture / regime ==='
SELECT
    prestation_code          AS type_acte,
    type_facture_code        AS type_facture,
    regime_code              AS regime,
    SUM(nombre_prestations)  AS nombre,
    SUM(montant_depense)     AS montant
FROM kpi_prestations_jour
GROUP BY 1, 2, 3
ORDER BY 4 DESC;

\echo ''
\echo '=== 4. Activite par jour de soins ==='
SELECT
    jour                     AS jour_soins,
    SUM(nombre_prestations)  AS nombre_prestations,
    SUM(montant_depense)     AS montant
FROM kpi_prestations_jour
GROUP BY 1 ORDER BY 1;

\echo ''
\echo '=== 6. Taux de couverture CMU (KPI 12, 13, 14) ==='
SELECT
    SUM(montant_pris_en_charge) AS pris_en_charge_cmu,
    SUM(montant_reste_a_charge) AS reste_a_charge_assure,
    ROUND(100.0 * SUM(montant_pris_en_charge) / NULLIF(SUM(montant_depense), 0), 1) AS taux_couverture_pct
FROM kpi_prestations_jour;

\echo ''
\echo '=== 7. Vue HEBDOMADAIRE, derivee du grain journalier ==='
SELECT
    DATE_TRUNC('week', jour)::date AS semaine,
    SUM(nombre_prestations)        AS prestations,
    SUM(montant_depense)           AS montant_facture,
    ROUND(100.0 * SUM(montant_pris_en_charge) / NULLIF(SUM(montant_depense), 0), 1) AS taux_couverture_pct
FROM kpi_prestations_jour
GROUP BY 1 ORDER BY 1;

\echo ''
\echo '=== 8. Top 10 centres de sante (KPI 10) ==='
SELECT
    centre_sante_code,
    SUM(nombre_prestations) AS prestations,
    SUM(montant_depense)    AS montant
FROM kpi_prestations_centre_jour
GROUP BY 1 ORDER BY 3 DESC LIMIT 10;

\echo ''
\echo '=== 9. Top 10 praticiens (KPI 11) ==='
SELECT
    professionnel_sante_code,
    SUM(nombre_prestations) AS prestations,
    SUM(montant_depense)    AS montant
FROM kpi_prestations_praticien_jour
GROUP BY 1 ORDER BY 3 DESC LIMIT 10;

\echo ''
\echo '=== 10. Controle IDEMPOTENCE : aucune ligne ne doit etre dupliquee ==='
\echo '(la cle primaire l interdit ; ce controle verifie que le compte de lignes'
\echo ' reste stable meme apres plusieurs executions du job)'
SELECT
    (SELECT COUNT(*) FROM kpi_prestations_jour)           AS lignes_jour,
    (SELECT COUNT(*) FROM kpi_prestations_centre_jour)    AS lignes_centre,
    (SELECT COUNT(*) FROM kpi_prestations_praticien_jour) AS lignes_praticien;

\echo ''
\echo '=== 11. Passages (factures) par jour (KPI 2/4) ==='
SELECT
    jour                  AS jour_soins,
    SUM(nombre_factures)  AS nombre_factures
FROM kpi_factures_jour
GROUP BY 1 ORDER BY 1;

\echo ''
\echo '=== 12. Top 10 assures, ex-aequo inclus (KPI 23) ==='
SELECT
    COUNT(*)                                                           AS lignes_top10,
    MAX(nombre_prestations)                                            AS max_prestations,
    md5(string_agg(personne_uuid || ':' || nombre_prestations, ',' ORDER BY personne_uuid)) AS empreinte
FROM v_top10_assures;

\echo ''
\echo '=== 13. Top 10 assures par centre, empreinte globale (KPI 24) ==='
SELECT
    COUNT(*) AS lignes_top10,
    md5(string_agg(centre_sante_code || ':' || personne_uuid || ':' || nombre_prestations, ','
        ORDER BY centre_sante_code, personne_uuid)) AS empreinte
FROM v_top10_assures_par_centre;

\echo ''
\echo '=== 14. EP sans reponse (KPI 17, hypothese H4) ==='
SELECT COALESCE(SUM(nombre_ententes), 0) AS ep_sans_reponse
FROM kpi_ententes_prealables_jour
WHERE statut_code = 'sans_reponse';

\echo ''
\echo '=== 15. Montant engage par statut (KPI 22) ==='
SELECT statut_code, SUM(montant_engage_cmu) AS montant_engage_cmu
FROM kpi_ententes_prealables_jour
GROUP BY 1 ORDER BY 1;

\echo ''
\echo '=== 16. Activite par agent, hors validee_office (KPI 20) ==='
SELECT COUNT(DISTINCT agent_code) AS agents, SUM(nombre_ententes) AS lignes
FROM kpi_ententes_prealables_agent_jour;

\echo ''
\echo '=== 17. EP par type de demande (KPI 21) ==='
SELECT type_demande_code, SUM(nombre_ententes) AS nombre
FROM kpi_ententes_prealables_jour
GROUP BY 1 ORDER BY 1;

\echo ''
\echo '=== 18. KPI mensuel certifie (mois clos uniquement) ==='
SELECT * FROM v_kpi_ententes_prealables_mois ORDER BY mois, statut_code;
