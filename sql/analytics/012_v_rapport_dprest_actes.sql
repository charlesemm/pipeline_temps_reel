-- Vue de restitution pour le rapport DPREST "Actes médicaux / Montants" (grille historique
-- Excel, demande du 2026-09-30). Format long (jour x indicateur x valeur), pensé pour un
-- visuel matriciel Power BI : indicateur en ligne, jour (ou son année/mois, regroupés côté
-- Power BI) en colonne. Additif : sommable sur n'importe quelle période sans biais.
--
-- Hypothèses à confirmer avec le métier avant publication (voir docs/kpi.md) :
--   - "Nombre de consultations" = prestation_code CONS-GEN + CONS-SPE uniquement, pas tout le
--     bucket type_facture_code='AMB' (qui inclut aussi SOI-PAN "soins/pansement" et URG-ACC
--     "urgence" — vérifié par SELECT DISTINCT le 2026-09-30, voir docs/decisions.md).
--   - "Nombre d'examen de labo-imagerie" = type_facture_code='BIO', qui regroupe en réalité les
--     codes BIO-* (biologie) ET IMG-* (imagerie) — aucune distinction n'existe pour les séparer.
--
-- Volontairement absents de cette vue (comptages distincts, pas additifs — voir les mesures DAX
-- recommandées dans docs/kpi.md) : nombre d'assurés traités, nombre de centres fréquentés.
-- Volontairement absents du schéma source (aucune donnée, voir docs/kpi.md, limites connues) :
-- indigents traités, pharmacies d'intérieur, taux de service bons %, nombre de pharmacies
-- (TB_REF_PHARMACIES existe côté simulateur mais n'est pas répliquée par CDC).

CREATE OR REPLACE VIEW v_rapport_dprest_actes AS
SELECT jour,
       'Nombre de consultations' AS indicateur,
       SUM(nombre_prestations) FILTER (WHERE prestation_code IN ('CONS-GEN', 'CONS-SPE')) AS valeur
FROM kpi_prestations_jour
GROUP BY jour
UNION ALL
SELECT jour,
       'Nombre de prescriptions (Pharmacie)',
       SUM(nombre_prestations) FILTER (WHERE type_facture_code = 'PHA')
FROM kpi_prestations_jour
GROUP BY jour
UNION ALL
SELECT jour,
       'Nombre d''examen de labo-imagerie',
       SUM(nombre_prestations) FILTER (WHERE type_facture_code = 'BIO')
FROM kpi_prestations_jour
GROUP BY jour
UNION ALL
SELECT jour,
       'Nombre de soins dentaires',
       SUM(nombre_prestations) FILTER (WHERE type_facture_code = 'DEN')
FROM kpi_prestations_jour
GROUP BY jour
UNION ALL
SELECT jour,
       'Nombre d''hospitalisation',
       SUM(nombre_prestations) FILTER (WHERE type_facture_code = 'HOS')
FROM kpi_prestations_jour
GROUP BY jour
UNION ALL
SELECT jour,
       'Montants',
       SUM(montant_depense)
FROM kpi_prestations_jour
GROUP BY jour;
