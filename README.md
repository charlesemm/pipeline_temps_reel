# pipeline_temps_reel

Pipeline de données temps réel pour la mise à disposition des données de prestations à la Direction
des Prestations (DPREST) — mémoire de fin d'études ESATIC, réalisé au Service de Gouvernance des
Données (SGD) de la CNAM Côte d'Ivoire.

Remplace le circuit manuel actuel (e-mail → script SQL Oracle ad hoc sur MIRKA → export Excel →
calcul manuel des KPI) par un pipeline temps réel en architecture Kappa, 100 % synthétique :

```
simulateur_V5 (PostgreSQL, schéma MIRKA)
        │ CDC
        ▼
Debezium → Kafka + Schema Registry → Flink → PostgreSQL analytique
                                                      │
                                        Grafana (supervision) + Superset (KPI DPREST)
```

Le contexte complet du projet (architecture cible, contraintes légales, règles d'ingénierie) est
détaillé dans [CLAUDE.md](CLAUDE.md). Le suivi des étapes est dans [docs/PLAN.md](docs/PLAN.md), les
choix techniques justifiés dans [docs/decisions.md](docs/decisions.md), les définitions des KPI dans
[docs/kpi.md](docs/kpi.md).

## État du projet

| Étape | État |
|---|---|
| 0 — Environnement et squelette | ✅ Terminé |
| 1 — Raccordement à `simulateur_V5` (CDC) | ✅ Terminé |
| 2 — Kafka, Schema Registry, Debezium | ✅ Terminé |
| 3 — Jobs Flink | ✅ Terminé |
| 4 — Schéma analytique et KPI | ✅ Terminé |
| 5 — Supervision Grafana | ✅ Terminé |
| 6 — Dashboards Superset | ✅ Terminé |
| 7 — Sécurité, chiffrement, rôles | ✅ Terminé |
| 8 — Évaluation et comparaison avec l'existant | ⬜ À faire |

Détail complet dans [docs/PLAN.md](docs/PLAN.md).

## Source des données

La base source (PostgreSQL, schéma reproduisant MIRKA) et le générateur de flux ne sont **pas dans ce
dépôt** : ils existent déjà dans le projet séparé `simulateur_V5`, déployé indépendamment via Podman.
Ce dépôt capture cette base existante (CDC) et construit le reste du pipeline jusqu'aux dashboards —
voir `docs/PLAN.md`, étape 1.

## Démarrer et arrêter la stack

Podman (pas Docker Desktop) — voir `docs/decisions.md` pour la justification. Le guide de référence,
avec toutes les commandes, les interfaces web et les pièges connus, est
[docs/guides/demarrage_arret.md](docs/guides/demarrage_arret.md).

En résumé :

```powershell
.\scripts\start-stack.ps1   # VM Podman + simulateur_V5 + pipeline + connecteurs + job Flink
.\scripts\stop-stack.ps1    # arrêt propre, sans rien supprimer
```

Pas de stack laissée allumée en permanence : on démarre les services nécessaires à la session de
travail en cours, puis on les arrête.

## Structure du projet

```
docker-compose.yml   # Services du pipeline (Kafka, Schema Registry, Kafka Connect, Flink, PostgreSQL...)
connectors/           # Configuration JSON des connecteurs Debezium
kafka-connect/        # Image Kafka Connect + plugin Debezium
flink/                # Image Flink + jobs SQL (KPI, agrégats continus)
sql/analytics/        # Schéma analytique cible (tables KPI, numéroté par évolution)
akhq/                 # Configuration de l'UI Kafka (AKHQ)
monitoring/           # Prometheus, dashboards Grafana (provisioning) — étape 5
superset/             # Export des dashboards DPREST — étape 6
evaluation/           # Scripts de contrôle et de mesure (latence, fraîcheur, comparaison source/pipeline)
scripts/              # Scripts PowerShell de démarrage / arrêt
docs/                 # Plan, décisions, dictionnaire de données, KPI, guides pas à pas
  guides/             # Un guide par étape + démarrage/arrêt, vérification, consultation des données
```

## Vérifier que le pipeline fonctionne

Le principe : ne jamais se fier aux statuts « vert » des composants, toujours comparer les chiffres
produits par le pipeline à un calcul indépendant sur la base source. La méthode complète, avec les
scripts de contrôle, est dans [docs/guides/verification.md](docs/guides/verification.md).

```powershell
podman exec pipeline_temps_reel-postgres-analytics-1 psql -U dprest -d dprest_analytics -f evaluation/controle_analytics.sql
podman exec simulateur_v5-postgres-1 psql -U echo -d echo_db -f evaluation/controle_source.sql
```

## Documentation

- [docs/PLAN.md](docs/PLAN.md) — suivi des étapes, ce qui a été fait, problèmes rencontrés
- [docs/decisions.md](docs/decisions.md) — choix techniques et justifications (pour le mémoire)
- [docs/kpi.md](docs/kpi.md) — définitions des KPI, hypothèses assumées, limites connues
- [docs/dictionnaire_donnees.md](docs/dictionnaire_donnees.md) — schéma de la base source
- [docs/guides/](docs/guides/) — guides pas à pas (démarrage/arrêt, chaque étape, vérification, consultation des données)
