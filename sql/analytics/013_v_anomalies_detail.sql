-- ═══════════════════════════════════════════════════════════════
-- Schéma analytique — ajout du 2026-10-01
--
-- Vues de détail de la quarantaine : la ligne source d'une anomalie,
-- stockée en JSON dans qualite_anomalies.donnee_brute, éclatée en une
-- colonne typée par champ. Une vue par domaine, parce que les champs
-- diffèrent entièrement d'un domaine à l'autre (une vue unique aurait
-- surtout des colonnes vides).
--
-- Solution transitoire : à terme, le job Flink écrira directement des
-- tables détaillées par domaine (sink à refaire). Les vues permettent de
-- restituer le détail tout de suite, sans redéployer Flink.
--
-- Les clés JSON lues ici sont exactement celles écrites par les
-- JSON_OBJECT de flink/sql/kpi_prestations.sql (blocs « Anomalies 1/5 »
-- à « 5/5 ») ; une clé renommée d'un côté donnerait une colonne NULL
-- sans erreur. Conversions de type alignées sur ce que Flink émet :
--   - jour_soins, assure_date_naissance : CAST(DATE AS STRING) -> 'YYYY-MM-DD' -> ::date
--   - date_creation_facture : TO_TIMESTAMP -> 'YYYY-MM-DD HH:MM:SS.fff' -> ::timestamp
--   - date_debut (entente) : chaîne ISO 8601 Debezium '...T...Z' -> ::timestamptz
--   - facture_date_soins_brute : entier Debezium (jours depuis 1970), gardé
--     en texte (il ne vaut que NULL pour le motif date_soins_manquante)
--   - montants, quantités, taux : nombre JSON -> ::numeric
-- Le domaine facture a deux formes de JSON (bloc A date_soins_manquante,
-- bloc B dates/centre) : la vue réunit les deux jeux de clés, une clé
-- absente vaut NULL.
--
-- Accès : données nominatives (nom, numéro de sécurité sociale, date de
-- naissance, personne_uuid) -> role_qualite_nominatif (SGD) uniquement,
-- comme qualite_anomalies.donnee_brute (voir 009). La DPREST garde
-- v_qualite_anomalies (011), sans la ligne source.
--
-- Idempotent.
-- ═══════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW v_anomalies_prestation AS
WITH src AS (
    SELECT
        qa.famille,
        qa.motif_anomalie,
        qa.cle_metier,
        qa.detecte_le,
        qa.donnee_brute::jsonb AS j
    FROM qualite_anomalies AS qa
    WHERE qa.domaine = 'prestation'
)
SELECT
    src.famille,
    src.motif_anomalie,
    src.cle_metier,
    src.detecte_le,
    src.j ->> 'facture_numero'                        AS facture_numero,
    src.j ->> 'prestation_code'                       AS prestation_code,
    src.j ->> 'professionnel_sante_code'              AS professionnel_sante_code,
    src.j ->> 'centre_sante_code'                     AS centre_sante_code,
    (src.j ->> 'jour_soins')::date                    AS jour_soins,
    (src.j ->> 'montant_depense')::numeric            AS montant_depense,
    (src.j ->> 'montant_rq')::numeric                 AS montant_rq,
    (src.j ->> 'montant_assure')::numeric             AS montant_assure,
    (src.j ->> 'quantite_prescrite')::numeric         AS quantite_prescrite,
    (src.j ->> 'quantite_servie')::numeric            AS quantite_servie,
    (src.j ->> 'taux_remboursement')::numeric         AS taux_remboursement,
    src.j ->> 'type_facture_code'                     AS type_facture_code,
    src.j ->> 'regime_code'                           AS regime_code,
    src.j ->> 'personne_uuid'                         AS personne_uuid
FROM src;

COMMENT ON VIEW v_anomalies_prestation IS
    'Anomalies du domaine prestation, ligne source (donnee_brute) éclatée en colonnes. Nominatif : SGD uniquement.';


CREATE OR REPLACE VIEW v_anomalies_facture AS
WITH src AS (
    SELECT
        qa.famille,
        qa.motif_anomalie,
        qa.cle_metier,
        qa.detecte_le,
        qa.donnee_brute::jsonb AS j
    FROM qualite_anomalies AS qa
    WHERE qa.domaine = 'facture'
)
SELECT
    src.famille,
    src.motif_anomalie,
    src.cle_metier,
    src.detecte_le,
    src.j ->> 'facture_numero'                        AS facture_numero,
    src.j ->> 'type_facture_code'                     AS type_facture_code,
    src.j ->> 'regime_code'                           AS regime_code,
    src.j ->> 'centre_sante_code'                     AS centre_sante_code,
    src.j ->> 'centre_sante_type_code'                AS centre_sante_type_code,
    src.j ->> 'centre_sante_type_libelle'             AS centre_sante_type_libelle,
    (src.j ->> 'jour_soins')::date                    AS jour_soins,
    (src.j ->> 'date_creation_facture')::timestamp    AS date_creation_facture,
    src.j ->> 'facture_date_soins_brute'              AS facture_date_soins_brute,
    src.j ->> 'personne_uuid'                         AS personne_uuid
FROM src;

COMMENT ON VIEW v_anomalies_facture IS
    'Anomalies du domaine facture, ligne source (donnee_brute) éclatée en colonnes. Nominatif : SGD uniquement.';


CREATE OR REPLACE VIEW v_anomalies_agent AS
WITH src AS (
    SELECT
        qa.famille,
        qa.motif_anomalie,
        qa.cle_metier,
        qa.detecte_le,
        qa.donnee_brute::jsonb AS j
    FROM qualite_anomalies AS qa
    WHERE qa.domaine = 'agent'
)
SELECT
    src.famille,
    src.motif_anomalie,
    src.cle_metier,
    src.detecte_le,
    src.j ->> 'agent_code'                            AS agent_code,
    src.j ->> 'agent_nom'                             AS agent_nom,
    src.j ->> 'agent_prenoms'                         AS agent_prenoms,
    src.j ->> 'agent_email'                           AS agent_email,
    src.j ->> 'agent_type_code'                       AS agent_type_code
FROM src;

COMMENT ON VIEW v_anomalies_agent IS
    'Anomalies du domaine agent, ligne source (donnee_brute) éclatée en colonnes. Nominatif : SGD uniquement.';


CREATE OR REPLACE VIEW v_anomalies_assure AS
WITH src AS (
    SELECT
        qa.famille,
        qa.motif_anomalie,
        qa.cle_metier,
        qa.detecte_le,
        qa.donnee_brute::jsonb AS j
    FROM qualite_anomalies AS qa
    WHERE qa.domaine = 'assure'
)
SELECT
    src.famille,
    src.motif_anomalie,
    src.cle_metier,
    src.detecte_le,
    src.j ->> 'personne_uuid'                         AS personne_uuid,
    src.j ->> 'assure_numero_identifiant'             AS assure_numero_identifiant,
    src.j ->> 'numero_secu'                           AS numero_secu,
    src.j ->> 'assure_nom'                            AS assure_nom,
    src.j ->> 'assure_prenoms'                        AS assure_prenoms,
    (src.j ->> 'assure_date_naissance')::date         AS assure_date_naissance
FROM src;

COMMENT ON VIEW v_anomalies_assure IS
    'Anomalies du domaine assure, ligne source (donnee_brute) éclatée en colonnes. Nominatif : SGD uniquement.';


CREATE OR REPLACE VIEW v_anomalies_entente_prealable AS
WITH src AS (
    SELECT
        qa.famille,
        qa.motif_anomalie,
        qa.cle_metier,
        qa.detecte_le,
        qa.donnee_brute::jsonb AS j
    FROM qualite_anomalies AS qa
    WHERE qa.domaine = 'entente_prealable'
)
SELECT
    src.famille,
    src.motif_anomalie,
    src.cle_metier,
    src.detecte_le,
    src.j ->> 'entente_prealable_id'                  AS entente_prealable_id,
    src.j ->> 'type_demande_code'                     AS type_demande_code,
    (src.j ->> 'date_debut')::timestamptz             AS date_debut,
    src.j ->> 'statut_code'                           AS statut_code,
    src.j ->> 'agent_code'                            AS agent_code,
    (src.j ->> 'montant_engage_cmu')::numeric         AS montant_engage_cmu
FROM src;

COMMENT ON VIEW v_anomalies_entente_prealable IS
    'Anomalies du domaine entente_prealable, ligne source (donnee_brute) éclatée en colonnes. Nominatif : SGD uniquement.';


GRANT SELECT ON
    v_anomalies_prestation,
    v_anomalies_facture,
    v_anomalies_agent,
    v_anomalies_assure,
    v_anomalies_entente_prealable
TO role_qualite_nominatif;
