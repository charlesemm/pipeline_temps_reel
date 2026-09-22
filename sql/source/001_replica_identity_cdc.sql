-- ═══════════════════════════════════════════════════════════════
-- Base SOURCE (simulateur_V5, echo_db) — REPLICA IDENTITY FULL des tables suivies par CDC
--
-- Debezium (plugin pgoutput) ne publie l'état « avant » d'un UPDATE que si la
-- table est en REPLICA IDENTITY FULL. Sans lui, le format debezium-json de
-- Flink échoue : « The "before" field of UPDATE message is null » (incident
-- du 2026-09-11, revenu le 2026-09-21 après la recréation de la base : ce
-- réglage n'était dans aucune migration du simulateur).
--
-- Une table de plus dans connectors/debezium-postgres-json.json
-- (table.include.list) = une ligne de plus ici, AVANT l'intégration Flink.
--
-- À appliquer en tant que propriétaire des tables (compte `echo`) :
--   podman exec -i simulateur_v5-postgres-1 psql -U echo -d echo_db < sql/source/001_replica_identity_cdc.sql
-- Rejouable sans effet de bord. Réversible : REPLICA IDENTITY DEFAULT.
-- Ne modifie aucune donnée ; augmente seulement le volume du journal de réplication.
-- ═══════════════════════════════════════════════════════════════

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY[
        -- Famille A : factures et prestations
        'TB_FACTURES', 'TB_FACTURES_PRESTATIONS', 'TB_FACTURES_REJETS', 'TB_FACTURES_STATUTS',
        -- Famille B : ententes préalables
        'TB_ENTENTES_PREALABLES', 'TB_ENTENTES_PREALABLES_STATUTS',
        'TB_ENTENTES_PREALABLES_ACTES_MEDICAUX', 'TB_ENTENTES_PREALABLES_PRESTATIONS',
        -- Référentiels
        'TB_REF_CENTRES_SANTE', 'TB_REF_PROFESSIONNELS_SANTE', 'TB_REF_COLLECTIVITES',
        'TB_REF_AGENTS', 'TB_REF_ASSURES',
        -- Famille C : prescriptions et pathologies (2026-09-21)
        'TB_FACTURES_PRESCRIPTIONS', 'TB_FACTURES_PATHOLOGIES',
        'TB_REF_MEDICAMENTS', 'TB_REF_DCI', 'TB_REF_PATHOLOGIES'
    ]
    LOOP
        EXECUTE format('ALTER TABLE public.%I REPLICA IDENTITY FULL', t);
    END LOOP;
END
$$;
