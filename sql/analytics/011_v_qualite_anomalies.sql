-- ═══════════════════════════════════════════════════════════════
-- Schéma analytique — ajout du 2026-09-24
--
-- Vue de restitution de la quarantaine pour la DPREST : toutes les
-- colonnes de qualite_anomalies SAUF donnee_brute (la ligne source
-- complète, nominative, réservée au SGD — voir 005 et 009).
--
-- Pourquoi une vue et pas le droit par colonne déjà posé en 009
-- (GRANT SELECT (domaine, motif_anomalie, cle_metier, detecte_le)) :
-- Superset lit la liste des colonnes d'une table dans le catalogue, droits
-- ou non, et « Drill to detail » demande ensuite TOUTES ces colonnes ; sur
-- la table, la requête échouerait sur donnee_brute (permission refusée).
-- La vue n'expose que ce que la DPREST a le droit de voir, rien de plus à
-- filtrer côté Superset. Elle ajoute aussi `famille`, créée en 008 après
-- le droit par colonne de 009.
--
-- cle_metier reste un identifiant technique (numéro de facture, code
-- agent, personne_uuid pour une anomalie d'identité) : même niveau
-- d'exposition que v_top10_assures, sans nom ni numéro de sécurité sociale.
--
-- Idempotent.
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW v_qualite_anomalies AS
SELECT
    domaine,
    famille,
    motif_anomalie,
    cle_metier,
    detecte_le
FROM qualite_anomalies;

COMMENT ON VIEW v_qualite_anomalies IS
    'Quarantaine qualité sans la ligne source nominative (donnee_brute) : lisible par la DPREST.';

GRANT SELECT ON v_qualite_anomalies TO role_kpi_lecture;
