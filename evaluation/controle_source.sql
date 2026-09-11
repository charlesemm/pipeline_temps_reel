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
\echo '=== 5. Ententes prealables par statut (KPI 18) ==='
SELECT
    s."STATUT_CODE"  AS statut,
    COUNT(*)         AS nombre,
    ROUND(AVG(s."STATUT_DATE_DEBUT" - e."ENTENTE_PREALABLE_DATE_DEBUT"), 2) AS delai_moyen_jours
FROM "TB_ENTENTES_PREALABLES_STATUTS" s
JOIN "TB_ENTENTES_PREALABLES" e ON e."ENTENTE_PREALABLE_ID" = s."ENTENTE_PREALABLE_ID"
GROUP BY 1 ORDER BY 2 DESC;

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
