-- ---------------------------------------------------------------------------
-- 009 : contrôle d'accès par rôle (étape 7d)
--
-- Sépare trois profils de privilèges (rôles de GROUPE, sans connexion) et les
-- rattache aux comptes de connexion :
--
--   role_kpi_lecture         KPI, dimensions, vues ; qualite_anomalies SANS la
--                            colonne donnee_brute (ligne source complète en JSON :
--                            nom, numéro de sécurité sociale, date de naissance).
--                            -> dprest_lecture (Superset DPREST, Grafana)
--   role_qualite_nominatif   qualite_anomalies COMPLÈTE, donnee_brute incluse.
--                            Réservé aux comptes habilités du SGD.
--                            -> sgd_qualite
--   role_flink_ecriture      SELECT/INSERT/UPDATE/DELETE sur les 12 tables écrites
--                            par le job kpi-continu, rien d'autre (fin du compte
--                            superutilisateur dprest pour Flink).
--                            -> flink_writer
--
-- Idempotent : peut être rejoué sans effet de bord. Les comptes de connexion
-- sont créés seulement si les mots de passe sont fournis (variables psql
-- flink_pw et sgd_qualite_pw) par scripts/apply-roles-analytics.ps1 ; sur une
-- base neuve initialisée par docker-entrypoint-initdb.d, seuls les groupes sont
-- créés. Voir docs/guides/etape7d_roles.md.
-- ---------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_kpi_lecture') THEN
        CREATE ROLE role_kpi_lecture NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_qualite_nominatif') THEN
        CREATE ROLE role_qualite_nominatif NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'role_flink_ecriture') THEN
        CREATE ROLE role_flink_ecriture NOLOGIN;
    END IF;
END
$$;

-- Lecture métier : tables et vues non nominatives.
GRANT SELECT ON
    kpi_prestations_jour,
    kpi_prestations_centre_jour,
    kpi_prestations_praticien_jour,
    kpi_factures_jour,
    kpi_prestations_assure_jour,
    kpi_ententes_prealables_jour,
    kpi_ententes_prealables_agent_jour,
    kpi_ententes_prealables_mois,
    dim_centres_sante,
    dim_professionnels_sante,
    dim_collectivites,
    dim_agents,
    v_top10_assures,
    v_top10_assures_par_centre,
    v_kpi_ententes_prealables_mois,
    v_kpi_ep_agent_medecin_conseil,
    v_centres_sante_geo,
    v_professionnels_sante
TO role_kpi_lecture;

-- Quarantaine : colonnes non nominatives seulement pour la lecture métier.
GRANT SELECT (domaine, motif_anomalie, cle_metier, detecte_le)
    ON qualite_anomalies
    TO role_kpi_lecture;

GRANT SELECT ON qualite_anomalies TO role_qualite_nominatif;

GRANT SELECT, INSERT, UPDATE, DELETE ON
    kpi_prestations_jour,
    kpi_prestations_centre_jour,
    kpi_prestations_praticien_jour,
    kpi_factures_jour,
    kpi_prestations_assure_jour,
    kpi_ententes_prealables_jour,
    kpi_ententes_prealables_agent_jour,
    dim_centres_sante,
    dim_professionnels_sante,
    dim_collectivites,
    dim_agents,
    qualite_anomalies
TO role_flink_ecriture;

-- Migration : dprest_lecture perd l'accès direct à toute la table de
-- quarantaine (donnee_brute comprise) et passe par le rôle de groupe.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dprest_lecture') THEN
        REVOKE SELECT ON qualite_anomalies FROM dprest_lecture;
        GRANT role_kpi_lecture TO dprest_lecture;
    END IF;
END
$$;

-- Comptes de connexion (mots de passe fournis par le script d'application).
\if :{?flink_pw}
SELECT
    NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'flink_writer') AS create_flink
\gset
\if :create_flink
CREATE ROLE flink_writer LOGIN PASSWORD :'flink_pw';
\else
ALTER ROLE flink_writer PASSWORD :'flink_pw';
\endif
GRANT role_flink_ecriture TO flink_writer;
\endif

\if :{?sgd_qualite_pw}
SELECT
    NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'sgd_qualite') AS create_sgd
\gset
\if :create_sgd
CREATE ROLE sgd_qualite LOGIN PASSWORD :'sgd_qualite_pw';
\else
ALTER ROLE sgd_qualite PASSWORD :'sgd_qualite_pw';
\endif
GRANT role_qualite_nominatif TO sgd_qualite;
GRANT role_kpi_lecture TO sgd_qualite;
\endif
