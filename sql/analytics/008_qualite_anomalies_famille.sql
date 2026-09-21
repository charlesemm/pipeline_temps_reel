-- ═══════════════════════════════════════════════════════════════
-- Schéma analytique — ajout du 2026-09-17 (suite de 005_qualite_anomalies.sql)
--
-- Ajoute la colonne `famille`, absente du premier jet : `domaine` dit sur
-- QUELLE TABLE porte une anomalie (prestation/facture/agent/assure/
-- entente_prealable), mais ne permet pas de répondre à la question
-- « quelles anomalies touchent une DATE, un MONTANT, un NOM ? » — un même
-- domaine (ex. prestation) mélange des anomalies de nature différente
-- (montant aberrant, quantité incohérente, code de référentiel inconnu).
--
-- Reprend telles quelles les six familles de la console d'injection du
-- simulateur (simulateur_V5/anomalies/catalogue.py, dict FAMILLES) :
-- MONTANTS, DATES, IDENTITE, QUANTITES, FORMAT, REFERENTIEL. Un motif ad
-- hoc (pas un code du catalogue, ex. date_soins_manquante) se voit
-- attribuer la famille la plus proche, pour rester filtrable au même
-- endroit que les vrais codes catalogue.
--
-- Étend aussi `domaine` à une 5e valeur : 'assure' (voir
-- flink/sql/kpi_prestations.sql, TB_REF_ASSURES ajoutée au connecteur
-- Debezium le même jour pour détecter les anomalies d'identité : numéro
-- de sécu, date de naissance, nom).
-- ═══════════════════════════════════════════════════════════════

ALTER TABLE qualite_anomalies
    ADD COLUMN IF NOT EXISTS famille VARCHAR(20) NOT NULL DEFAULT 'INDETERMINEE';

-- Recalcule la famille des lignes déjà présentes (avant le déploiement du
-- job Flink mis à jour), sur la base du motif seul. Idempotent : rejouer
-- cette migration ne change rien si `famille` est déjà juste.
UPDATE qualite_anomalies SET famille = CASE
    WHEN motif_anomalie IN ('MONTANT_ABERRANT', 'MONTANT_HORS_BAREME', 'REPARTITION_FAUSSEE',
                             'montant_depense_manquant', 'montant_engage_negatif') THEN 'MONTANTS'
    WHEN motif_anomalie IN ('DATE_ANTIDATEE', 'DATE_SOINS_FUTURE', 'DATE_HORS_DROITS',
                             'DATE_NAISSANCE_ABERRANTE', 'date_soins_manquante') THEN 'DATES'
    WHEN motif_anomalie IN ('NUMERO_SECU_INVALIDE', 'CHAMP_OBLIGATOIRE_VIDE',
                             'DOUBLON_EXACT', 'DOUBLON_APPROCHANT') THEN 'IDENTITE'
    WHEN motif_anomalie IN ('QUANTITE_NULLE', 'QUANTITE_EXCESSIVE') THEN 'QUANTITES'
    WHEN motif_anomalie IN ('EMAIL_INVALIDE', 'ENCODAGE_CASSE', 'TENTATIVE_INJECTION',
                             'type_demande_manquant') THEN 'FORMAT'
    WHEN motif_anomalie IN ('PRESTATION_ORPHELINE', 'TYPE_CENTRE_INCONNU') THEN 'REFERENTIEL'
    ELSE famille
END
WHERE famille = 'INDETERMINEE';

ALTER TABLE qualite_anomalies ALTER COLUMN famille DROP DEFAULT;

COMMENT ON COLUMN qualite_anomalies.famille IS
    'Aspect de la donnée en cause : MONTANTS | DATES | IDENTITE | QUANTITES | FORMAT | REFERENTIEL (mêmes familles que la console d''injection du simulateur, simulateur_V5/anomalies/catalogue.py).';
COMMENT ON COLUMN qualite_anomalies.domaine IS
    'prestation | facture | agent | assure | entente_prealable.';

CREATE INDEX IF NOT EXISTS idx_qualite_anomalies_famille
    ON qualite_anomalies (famille);

-- GRANT SELECT ON qualite_anomalies TO dprest_lecture; -- déjà accordé en 005, rien à refaire.
