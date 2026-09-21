# Guide — Étape 6 : dashboards métier DPREST avec Superset

Ce que fait cette étape : donner à la DPREST une restitution des KPI équivalente — et plus fraîche —
que l'export Excel actuel, sous forme de tableaux de bord navigables, sans écrire de SQL.

Définitions des KPI : `docs/kpi.md`. Schéma des tables : `sql/analytics/`. Choix techniques justifiés :
`docs/decisions.md`.

Prérequis : étapes 0 à 4 terminées, stack démarrée (`docs/guides/demarrage_arret.md`).

---

## Ce qui a été mis en place

| Élément | Rôle |
|---|---|
| `superset-db` | Base PostgreSQL dédiée aux métadonnées de Superset (dashboards, utilisateurs, connexions) — distincte de `postgres-analytics`, qui ne contient que les données KPI |
| `superset-redis` | Cache des résultats de requêtes et de l'état des filtres. Pas de worker Celery asynchrone (pas d'export planifié) |
| `superset-init` | Migrations + création du compte admin. Tourne une fois au premier démarrage, puis s'arrête |
| `superset` | Serveur web, port **8088** |
| `superset/Containerfile` | Image dérivée d'`apache/superset` : ajoute `psycopg2-binary`, absent de l'image officielle |
| `.env` | Secrets (clé secrète, mots de passe) — **non versionné**, seul endroit du projet où c'est le cas (voir pourquoi dans `docs/decisions.md`) |

---

## 1. Se connecter

```
http://<IP_VM>:8088
```

Identifiants : dans le fichier `.env` à la racine du projet (`SUPERSET_ADMIN_USER`,
`SUPERSET_ADMIN_PASSWORD`). Ce fichier n'est pas dans Git — si tu ne l'as plus, régénère les secrets
en suivant `docs/decisions.md` (section RAM/secrets de l'étape 6) puis relance
`podman compose up -d --force-recreate superset-init superset`.

Contrairement à Grafana, **le mot de passe survit aux redémarrages** : `superset-db` a un volume
persistant (`superset_db_data`), donc pas besoin de le réinitialiser à chaque session.

---

## 2. Ce qu'on y trouve

### Les deux tableaux de bord (menu **Dashboards**)

**Reconstruction du 2026-09-14** : les dashboards ont été refaits intégralement à la main dans
l'interface, en suivant deux guides pas-à-pas dédiés — le script d'origine mentionné plus bas
n'était plus versionné nulle part dans le dépôt, donc pas rejouable. Marche à suivre complète :

- `docs/guides/etape6a_superset_prestations.md` — dashboard **DPREST - Prestations et facturation**
  (KPI 1-15, 23-24 ; 7 jeux de données, 16 graphiques).
- `docs/guides/etape6b_superset_ententes.md` — dashboard **DPREST - Ententes préalables**
  (KPI 16-22 ; 3 jeux de données, 8 graphiques).

Ces deux guides couvrent l'intégralité des 24 KPI de `docs/kpi.md`, avec un type de graphique choisi
par nature de donnée (nombre unique, taux borné, répartition, classement, série temporelle) et une
palette de couleurs unique partagée entre les deux dashboards.

> Paragraphe d'origine, conservé pour mémoire de ce qui avait été fait à l'étape 6 initiale (2026-09-11) :
> les deux tableaux de bord et leurs 14 graphiques avaient été créés par script contre l'API REST de
> Superset, décrit dans `docs/decisions.md`. Ce script n'ayant pas été versionné, cette approche n'est
> plus la référence — voir les deux guides ci-dessus.

### Consulter une période précise

Superset propose un filtre temporel natif sur la colonne `jour` de chaque jeu de données. Depuis un
tableau de bord : icône **Filters** (en haut à gauche) → ajouter un filtre temporel → choisir la
plage. Comme pour Grafana, aucune requête n'est relancée manuellement : le grain journalier stocké
dans les tables KPI suffit à répondre à n'importe quelle période, semaine ou mois compris.

### Explorer soi-même (SQL Lab)

Menu **SQL Lab** → choisir la base « PostgreSQL analytique (dprest_analytics) » → écrire une requête
libre. Utile pour vérifier un chiffre affiché sur un dashboard, ou construire un nouveau graphique.

---

## 3. Créer un nouveau graphique

1. **Charts** → **+ Chart**
2. Choisir un jeu de données (les 10 déjà créés apparaissent dans la liste)
3. Choisir un type de visualisation (Table, Pie Chart, Big Number, etc.)
4. Configurer métriques et regroupements, **Run Query** pour prévisualiser
5. **Save** → l'ajouter à un tableau de bord existant ou à un nouveau

---

## 4. Vérifier que les chiffres sont justes

Même principe que partout ailleurs dans ce projet : ne jamais se fier à l'affichage seul, comparer à
une requête de contrôle indépendante.

```powershell
podman exec pipeline_temps_reel-postgres-analytics-1 psql -U dprest -d dprest_analytics -c "SELECT SUM(nombre_prestations) FROM kpi_prestations_jour;"
```

Comparer avec ce qu'affiche le graphique « Prestations totales » du tableau de bord « Prestations et
facturation ». Doit être strictement identique (pas de latence à ce niveau : Superset lit directement
la base, sans copie intermédiaire).

---

## Pièges rencontrés (et corrigés) — utiles si tu rejoues cette étape

1. **`apache/superset` sans pilote PostgreSQL.** L'image officielle ne fonctionne qu'avec SQLite par
   défaut. `superset-init` échoue avec `ModuleNotFoundError: No module named 'psycopg2'` dès la
   connexion à sa propre base de métadonnées. → image dérivée (`superset/Containerfile`).

2. **RAM insuffisante pour démarrer Superset.** Base dédiée + Redis + serveur web, en plus du reste de
   la stack, ne rentrent pas sur une VM à 6 Go. → VM passée à 7 Go (`.wslconfig`), et Grafana/
   Prometheus/AKHQ arrêtés pendant les phases de construction si besoin :
   `podman compose stop grafana prometheus akhq`.

3. **Secrets dans `.env`, pas dans `docker-compose.yml`.** Contrairement aux autres services (mots de
   passe de développement en clair, documentés et acceptés), Superset gère de vrais comptes
   utilisateurs → règle stricte de `CLAUDE.md` appliquée ici uniquement.

---

## Critère de validation de l'étape 6

- [x] Connexion à `postgres-analytics` testée et fonctionnelle.
- [x] 10 jeux de données créés (7 tables + 3 vues).
- [x] 14 graphiques créés, vérifiés individuellement contre le SQL de contrôle.
- [x] 2 tableaux de bord assemblés, correspondant aux deux familles de KPI de `docs/kpi.md`.
- [x] Chiffres affichés identiques au SQL de contrôle (prestations totales, taux de couverture,
      ventilation des ententes préalables, montant engagé).
- [ ] Contrôle d'accès par rôle pour la DPREST — prévu à l'étape 7.
- [ ] Sélecteur de période activé par défaut sur les tableaux de bord (fonctionne, pas encore réglé
      comme filtre par défaut) — à faire au prochain passage.
