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
| ~~`sgd_qualite`~~ (supprimé le 2026-10-02) | `role_qualite_nominatif` conservé sans compte de connexion | `donnee_brute` lue par `sgd_admin` (DBeaver) |
| `flink_writer` (job `kpi-continu`) | `role_flink_ecriture` | `SELECT/INSERT/UPDATE/DELETE` sur les 12 tables qu'il alimente, rien d'autre (plus de superutilisateur) |
| `dprest` | propriétaire | superutilisateur, réservé à l'administration |

Mot de passe : `FLINK_WRITER_PASSWORD` dans `.env`. Flink les reçoit à la soumission
(placeholder `__FLINK_WRITER_PASSWORD__` remplacé p## Superset

Mise à jour du 2026-10-02 : la seconde connexion « PostgreSQL analytique - qualité SGD (nominatif) » (compte
`sgd_qualite`) a été supprimée avec ses 23 jeux de données et son unique graphique, hors de tout tableau de bord :
aucun usage relevé (aucune requête SQL Lab sur 30 jours). Superset n'a plus qu'une connexion, `dprest_lecture`,
sans accès à `donnee_brute`. Voir `docs/decisions.md` (2026-10-02).

un sur la quarantaine.

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
