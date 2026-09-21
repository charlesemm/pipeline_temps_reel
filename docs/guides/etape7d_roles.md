# Étape 7d — Contrôle d'accès par rôle (base analytique)

## Décision

Les identités réelles restent visibles pour la DPREST (décision du 2026-09-21) : **pas de pseudonymisation**.
La protection repose sur la séparation des rôles. À ce jour, les tops d'assurés n'exposent qu'un identifiant
opaque (`personne_uuid`) ; aucun nom d'assuré n'est écrit dans les tables analytiques. Les données nominatives
ne se trouvent que dans `qualite_anomalies.donnee_brute` (ligne source complète en JSON : nom, numéro de
sécurité sociale, date de naissance), d'où la séparation ci-dessous.

## Rôles (`sql/analytics/009_roles_acces.sql`, appliqué par `scripts/apply-roles-analytics.ps1`)

| Compte | Groupe de privilèges | Droits |
|---|---|---|
| `dprest_lecture` (Superset DPREST, Grafana) | `role_kpi_lecture` | `SELECT` sur les KPI, dimensions et vues ; sur `qualite_anomalies` **sans** `donnee_brute` |
| `sgd_qualite` (SGD, habilité) | `role_qualite_nominatif` + `role_kpi_lecture` | idem + `donnee_brute` |
| `flink_writer` (job `kpi-continu`) | `role_flink_ecriture` | `SELECT/INSERT/UPDATE/DELETE` sur les 12 tables qu'il alimente, rien d'autre (plus de superutilisateur) |
| `dprest` | propriétaire | superutilisateur, réservé à l'administration |

Mots de passe : `FLINK_WRITER_PASSWORD`, `SGD_QUALITE_PASSWORD` dans `.env`. Flink les reçoit à la soumission
(placeholder `__FLINK_WRITER_PASSWORD__` remplacé par `start-stack.ps1`).

## Superset

Une seconde connexion, « PostgreSQL analytique - qualité SGD (nominatif) », utilise `sgd_qualite`, sans
exposition dans SQL Lab. Le tableau de qualité (guide 6c), qui affiche `donnee_brute`, doit s'appuyer sur cette
connexion et être réservé au rôle SGD ; les tableaux DPREST restent sur la connexion `dprest_lecture`.
**À faire à la main dans l'interface** : créer le dataset `qualite_anomalies` sur la connexion nominative et
attribuer l'accès à un rôle SGD. À ce jour Superset ne contient que 2 datasets, aucun sur la quarantaine.

## Vérifié le 2026-09-21

`dprest_lecture` : KPI lisibles, `donnee_brute` refusée. `sgd_qualite` : `donnee_brute` lisible, `DELETE` refusé.
`flink_writer` : `DROP TABLE` refusé, lecture des vues refusée. Job Flink rejoué avec ce compte : 2 118
prestations et 2 119 factures des deux côtés (source et analytique), aucun doublon.
Tests : `pytest tests/test_securite.py`.

## Limites

- `sgd_admin` (DBeaver) est membre du superutilisateur `dprest` : c'est un compte d'administration, tracé dans
  le journal d'audit (7c), pas un compte de consultation.
- `cle_metier` reste lisible par la DPREST : c'est la clé métier de la ligne en anomalie, pas une donnée
  nominative, à confirmer si sa nature change.
