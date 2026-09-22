-- Requêtes de contrôle sur la BASE SOURCE (simulateur_V5).
-- Servent de référence indépendante pour vérifier les KPI calculés par
-- le pipeline. Voir docs/guides/verification.md.

\echo '=== 1. Volume brut de la source ==='
SELECT
    (SELECT COUNT(*) FROM "TB_FACTURES")             AS factures,
    (SELECT COUNT(*) FROM "TB_FACTURES_PRESTATIONS") AS prestations,
    (SELECT COUNT(*) FROM "TB_ENTENTES_PREALABLES")  AS ententes_prealables;

\echo ''
\echo '=== 2. Totaux financiers (a comparer avec la base analytique) ==='
SELECT
    COUNT(*)                                  AS nombre_prestations,
    SUM(p."PRESTATION_MONTANT_DEPENSE")       AS montant_facture,
    SUM(p."PRESTATION_MONTANT_RQ")            AS pris_en_charge_cmu,
    SUM(p."PRESTATION_MONTANT_ASSURE")        AS reste_a_charge_assure
FROM "TB_FACTURES_PRESTATIONS" p
JOIN "TB_FACTURES" f ON f."FACTURE_NUMERO" = p."FACTURE_NUMERO";

\echo ''
\echo '=== 3. Repartition par type acte / type facture / regime ==='
SELECT
    p."PRESTATION_CODE"    AS type_acte,
    f."TYPE_FACTURE_CODE"  AS type_facture,
    f."REGIME_CODE"        AS regime,
    COUNT(*)               AS nombre,
    SUM(p."PRESTATION_MONTANT_DEPENSE") AS montant
FROM "TB_FACTURES_PRESTATIONS" p
JOIN "TB_FACTURES" f ON f."FACTURE_NUMERO" = p."FACTURE_NUMERO"
GROUP BY 1, 2, 3
ORDER BY 4 DESC;

\echo ''
\echo '=== 4. Activite par jour de soins ==='
SELECT
    f."FACTURE_DATE_SOINS"              AS jour_soins,
    COUNT(*)                            AS nombre_prestations,
    SUM(p."PRESTATION_MONTANT_DEPENSE") AS montant
FROM "TB_FACTURES_PRESTATIONS" p
JOIN "TB_FACTURES" f ON f."FACTURE_NUMERO" = p."FACTURE_NUMERO"
GROUP BY 1 ORDER BY 1;

\echo ''
\echo '=== 5. Ententes prealables par statut (KPI 18, 19) ==='
\echo '(delai en HEURES pleines depuis le 2026-09-17 - voir sql/analytics/007_kpi_ep_delai_heures.sql)'
SELECT
    s."STATUT_CODE"  AS statut,
    COUNT(*)         AS nombre,
    ROUND(AVG(FLOOR(EXTRACT(EPOCH FROM (s."STATUT_DATE_DEBUT" - e."ENTENTE_PREALABLE_DATE_DEBUT"))/3600)), 2) AS delai_moyen_heures_entieres
FROM "TB_ENTENTES_PREALABLES_STATUTS" s
JOIN "TB_ENTENTES_PREALABLES" e ON e."ENTENTE_PREALABLE_ID" = s."ENTENTE_PREALABLE_ID"
GROUP BY 1 ORDER BY 2 DESC;

\echo ''
\echo '=== 14. EP sans reponse (KPI 17, hypothese H4) ==='
SELECT COUNT(*) AS ep_sans_reponse
FROM "TB_ENTENTES_PREALABLES" e
WHERE NOT EXISTS (SELECT 1 FROM "TB_ENTENTES_PREALABLES_STATUTS" s WHERE s."ENTENTE_PREALABLE_ID" = e."ENTENTE_PREALABLE_ID");

\echo ''
\echo '=== 15. Montant engage par statut (KPI 22) ==='
SELECT s."STATUT_CODE" AS statut, SUM(a."ACTE_MEDICAL_MONTANT_CMU") AS montant_engage_cmu
FROM "TB_ENTENTES_PREALABLES_ACTES_MEDICAUX" a
JOIN "TB_ENTENTES_PREALABLES_STATUTS" s ON s."ENTENTE_PREALABLE_ID" = a."ENTENTE_PREALABLE_ID"
GROUP BY 1 ORDER BY 1;

\echo ''
\echo '=== 16. Activite par agent, hors validee_office (KPI 20) ==='
SELECT COUNT(DISTINCT "AGENT_CODE") AS agents, COUNT(*) AS lignes
FROM "TB_ENTENTES_PREALABLES_STATUTS"
WHERE "AGENT_CODE" IS NOT NULL;

\echo ''
\echo '=== 17. EP par type de demande (KPI 21) ==='
SELECT "TYPE_DEMANDE_CODE" AS type_demande, COUNT(*) AS nombre
FROM "TB_ENTENTES_PREALABLES"
GROUP BY 1 ORDER BY 1;

\echo ''
\echo '=== 11. Passages (factures) par jour (KPI 2/4) ==='
SELECT
    "FACTURE_DATE_SOINS" AS jour_soins,
    COUNT(*)             AS nombre_factures
FROM "TB_FACTURES"
GROUP BY 1 ORDER BY 1;

\echo ''
\echo '=== 12. Top 10 assures, ex-aequo inclus (KPI 23) ==='
WITH totaux AS (
    SELECT
        f."PERSONNE_UUID"::text AS personne_uuid,
        COUNT(*)                AS nombre_prestations
    FROM "TB_FACTURES_PRESTATIONS" p
    JOIN "TB_FACTURES" f ON f."FACTURE_NUMERO" = p."FACTURE_NUMERO"
    GROUP BY 1
),
classement AS (
    SELECT
        RANK() OVER (ORDER BY nombre_prestations DESC) AS rang,
        personne_uuid,
        nombre_prestations
    FROM totaux
)
SELECT
    COUNT(*)                                                           AS lignes_top10,
    MAX(nombre_prestations)                                            AS max_prestations,
    md5(string_agg(personne_uuid || ':' || nombre_prestations, ',' ORDER BY personne_uuid)) AS empreinte
FROM classement
WHERE rang <= 10;

\echo ''
\echo '=== 13. Top 10 assures par centre, empreinte globale (KPI 24) ==='
WITH totaux AS (
    SELECT
        f."CENTRE_SANTE_CODE"   AS centre_sante_code,
        f."PERSONNE_UUID"::text AS personne_uuid,
        COUNT(*)                AS nombre_prestations
    FROM "TB_FACTURES_PRESTATIONS" p
    JOIN "TB_FACTURES" f ON f."FACTURE_NUMERO" = p."FACTURE_NUMERO"
    GROUP BY 1, 2
),
classement AS (
    SELECT
        centre_sante_code,
        RANK() OVER (PARTITION BY centre_sante_code ORDER BY nombre_prestations DESC) AS rang,
        personne_uuid,
        nombre_prestations
    FROM totaux
)
SELECT
    COUNT(*) AS lignes_top10,
    md5(string_agg(centre_sante_code || ':' || personne_uuid || ':' || nombre_prestations, ','
        ORDER BY centre_sante_code, personne_uuid)) AS empreinte
FROM classement
WHERE rang <= 10;

\echo ''
\echo '=== 20. Prescriptions par jour (KPI 25, famille C, H8 et H9) ==='
SELECT
    "DATE_DEBUT"  AS jour,
    COUNT(*)      AS nombre_prescriptions
FROM "TB_FACTURES_PRESCRIPTIONS"
GROUP BY 1 ORDER BY 1;

\echo ''
\echo '=== 21. Top 10 medicaments prescrits, code + effectif (KPI 27, non masque) ==='
SELECT
    "PRESCRIPTION_CODE"  AS medicament_code,
    COUNT(*)             AS nombre_prescriptions
FROM "TB_FACTURES_PRESCRIPTIONS"
GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 10;

\echo ''
\echo '=== 22. Top 10 pathologies, code + effectif (KPI 30, non masque) ==='
SELECT
    "PATHOLOGIE_CODE"  AS pathologie_code,
    COUNT(*)           AS nombre_pathologies
FROM "TB_FACTURES_PATHOLOGIES"
GROUP BY 1 ORDER BY 2 DESC, 1 LIMIT 10;

\echo ''
\echo '=== 23. Ententes avec prescription associee, par type et statut (KPI 31, H7, non masque) ==='
SELECT
    COALESCE(e."TYPE_DEMANDE_CODE", '(inconnu)')  AS type_demande_code,
    COALESCE(s."STATUT_CODE", 'sans_reponse')     AS statut_code,
    COUNT(*)                                      AS nombre_ententes,
    COUNT(*) FILTER (WHERE EXISTS (
        SELECT 1 FROM "TB_FACTURES_PRESCRIPTIONS" p WHERE p."FACTURE_NUMERO" = e."FACTURE_NUMERO"
    ))                                            AS ententes_avec_prescription
FROM "TB_ENTENTES_PREALABLES" e
LEFT JOIN "TB_ENTENTES_PREALABLES_STATUTS" s ON s."ENTENTE_PREALABLE_ID" = e."ENTENTE_PREALABLE_ID"
GROUP BY 1, 2 ORDER BY 1, 2;

\echo ''
\echo '=== 24. Medicaments et pathologies portes par les ententes, par statut (KPI 32 et 33, non masque) ==='
WITH ep AS (
    SELECT
        e."FACTURE_NUMERO"                         AS facture_numero,
        COALESCE(s."STATUT_CODE", 'sans_reponse')  AS statut_code
    FROM "TB_ENTENTES_PREALABLES" e
    LEFT JOIN "TB_ENTENTES_PREALABLES_STATUTS" s ON s."ENTENTE_PREALABLE_ID" = e."ENTENTE_PREALABLE_ID"
),
presc AS (
    SELECT ep.statut_code, COUNT(*) AS n
    FROM ep JOIN "TB_FACTURES_PRESCRIPTIONS" p ON p."FACTURE_NUMERO" = ep.facture_numero
    GROUP BY 1
),
patho AS (
    SELECT ep.statut_code, COUNT(*) AS n
    FROM ep JOIN "TB_FACTURES_PATHOLOGIES" g ON g."FACTURE_NUMERO" = ep.facture_numero
    GROUP BY 1
)
SELECT
    st.statut_code,
    COALESCE(presc.n, 0)  AS nombre_medicaments_prescrits,
    COALESCE(patho.n, 0)  AS nombre_pathologies
FROM (SELECT DISTINCT statut_code FROM ep) st
LEFT JOIN presc ON presc.statut_code = st.statut_code
LEFT JOIN patho ON patho.statut_code = st.statut_code
ORDER BY 1;
