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
\echo '=== 14b. Delai moyen de traitement (KPI 19, corrige le 2026-09-17 : formule ET unite) ==='
\echo '(exclut les EP sans_reponse du denominateur ; en HEURES, pas en jours -'
\echo ' voir docs/guides/etape6b_superset_ententes.md et sql/analytics/007_kpi_ep_delai_heures.sql)'
SELECT
    ROUND(
        SUM(delai_moyen_heures * nombre_ententes)
        / NULLIF(SUM(CASE WHEN delai_moyen_heures IS NOT NULL THEN nombre_ententes ELSE 0 END), 0)
    , 2) AS delai_moyen_heures
FROM kpi_ententes_prealables_jour;

\echo ''
\echo '=== 15. Montant engage par statut (KPI 22) ==='
SELECT statut_code, SUM(montant_engage_cmu) AS montant_engage_cmu
FROM kpi_ententes_prealables_jour
GROUP BY 1 ORDER BY 1;

\echo ''
\echo '=== 16. Activite par agent, TOUS TYPES CONFONDUS (diagnostic, pas le KPI 20) ==='
\echo '(volontairement pas filtre par type - sert a mesurer l ecart avec la section 16b'
\echo ' ci-dessous, qui elle applique la vraie definition du KPI 20)'
SELECT COUNT(DISTINCT agent_code) AS agents, SUM(nombre_ententes) AS lignes
FROM kpi_ententes_prealables_agent_jour;

\echo ''
\echo '=== 16b. Activite par praticien-conseil, KPI 20 corrige le 2026-09-17 ==='
\echo '(restreint aux agents AGENT_TYPE_CODE = medecin_conseil, avec nom complet -'
\echo ' voir sql/analytics/006_dim_agents.sql)'
SELECT COUNT(DISTINCT agent_code) AS agents_medecin_conseil, SUM(nombre_ententes) AS lignes
FROM v_kpi_ep_agent_medecin_conseil;

\echo ''
\echo '=== 16c. Detail nominatif, verification lisibilite (KPI 20) ==='
SELECT agent_nom_complet, SUM(nombre_ententes) AS nombre_ententes
FROM v_kpi_ep_agent_medecin_conseil
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;

\echo ''
\echo '=== 17. EP par type de demande (KPI 21) ==='
SELECT type_demande_code, SUM(nombre_ententes) AS nombre
FROM kpi_ententes_prealables_jour
GROUP BY 1 ORDER BY 1;

\echo ''
\echo '=== 18. KPI mensuel certifie (mois clos uniquement) ==='
SELECT * FROM v_kpi_ententes_prealables_mois ORDER BY mois, statut_code;

\echo ''
\echo '=== 19. Anomalies detectees, par domaine et motif (voir sql/analytics/005_qualite_anomalies.sql) ==='
SELECT domaine, motif_anomalie, COUNT(*) AS nombre_lignes
FROM qualite_anomalies
GROUP BY 1, 2
ORDER BY 1, 2;

\echo ''
\echo '=== 20. Prescriptions par jour (KPI 25, famille C) ==='
SELECT
    jour,
    SUM(nombre_prescriptions)  AS nombre_prescriptions
FROM kpi_prescriptions_medicament_jour
GROUP BY 1 ORDER BY 1;

\echo ''
\echo '=== 21. Top 10 medicaments prescrits, code + effectif (KPI 27, non masque) ==='
SELECT
    medicament_code,
    SUM(nombre_prescriptions)  AS nombre_prescriptions
FROM kpi_prescriptions_medicament_jour
GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 10;

\echo ''
\echo '=== 22. Top 10 pathologies, code + effectif (KPI 30, non masque) ==='
SELECT
    pathologie_code,
    SUM(nombre_pathologies)  AS nombre_pathologies
FROM kpi_pathologies_jour
GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 10;

\echo ''
\echo '=== 23. Ententes avec prescription associee, par type et statut (KPI 31, non masque : vue interne SGD) ==='
SELECT
    type_demande_code,
    statut_code,
    COUNT(*)                                          AS nombre_ententes,
    COUNT(*) FILTER (WHERE nombre_prescriptions > 0)  AS ententes_avec_prescription
FROM v_ep_clinique_detail
GROUP BY 1, 2 ORDER BY 1, 2;

\echo ''
\echo '=== 24. Medicaments et pathologies portes par les ententes, par statut (KPI 32 et 33, non masque) ==='
SELECT
    statut_code,
    SUM(nombre_prescriptions)  AS nombre_medicaments_prescrits,
    SUM(nombre_pathologies)    AS nombre_pathologies
FROM v_ep_clinique_detail
GROUP BY 1 ORDER BY 1;

\echo ''
\echo '=== 25. Controle IDEMPOTENCE famille C : aucune cle ne doit etre dupliquee (attendu : 0 ligne) ==='
SELECT 'fait_prescriptions' AS table_controlee, COUNT(*) AS doublons FROM (
    SELECT facture_numero, prescription_code, date_debut FROM fait_prescriptions GROUP BY 1, 2, 3 HAVING COUNT(*) > 1) d
UNION ALL
SELECT 'kpi_prescriptions_medicament_jour', COUNT(*) FROM (
    SELECT jour, medicament_code FROM kpi_prescriptions_medicament_jour GROUP BY 1, 2 HAVING COUNT(*) > 1) d;
