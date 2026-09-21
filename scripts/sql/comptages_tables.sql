-- Comptage exact des lignes de chaque table du schéma public.
-- Utilisé par backup-analytics.ps1 (manifeste) et restore-test-analytics.ps1
-- (comparaison après restauration). Sortie : nom_table,nombre_lignes
SELECT
    t.table_name AS table_name,
    (
        XPATH(
            '/row/c/text()',
            QUERY_TO_XML(
                FORMAT('SELECT COUNT(*) AS c FROM %I.%I', t.table_schema, t.table_name),
                FALSE,
                TRUE,
                ''
            )
        )
    )[1]::TEXT::BIGINT AS row_count
FROM information_schema.tables AS t
WHERE t.table_schema = 'public'
  AND t.table_type = 'BASE TABLE'
ORDER BY t.table_name;
