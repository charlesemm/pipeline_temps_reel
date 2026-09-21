# Guide détaillé — Superset, dashboard « DPREST - Prestations et facturation »

Ce guide part du principe que vous n'avez **jamais utilisé Superset**. Chaque étape indique le menu
exact, le bouton exact (nom tel qu'affiché à l'écran, en français puisque c'est la langue de votre
instance) et ce qu'il faut saisir.

Architecture générale, connexion, identifiants : voir `docs/guides/etape6_superset.md`. Définition
précise de chaque KPI (formule, périmètre) : `docs/kpi.md`. Ce guide-ci ne fait qu'expliquer *comment
cliquer* pour construire les graphiques qui affichent ces KPI.

**Portée de ce fichier** : la famille A (Prestations et facturation), KPI 1, 2/4, 3, 5 à 15, 23, 24 —
17 KPI, 16 graphiques, 1 dashboard. La famille B (Ententes préalables) est dans
`docs/guides/etape6b_superset_ententes.md`, à faire après celui-ci.

**État de départ** : instance Superset reconstruite à neuf le 2026-09-14 (conteneurs `superset`,
`superset-db`, `superset-redis` recréés, volume `superset_db_data` effacé — voir `docs/decisions.md`).

**Mise à jour du 2026-09-15** : la connexion à `dprest_analytics` (« PostgreSQL analytique
(dprest_analytics) », compte `dprest_lecture`) **existe déjà** — recréée directement en base pendant
le dépannage du formulaire (voir `docs/decisions.md`, incident CSRF `reverse-proxy`). Vous n'avez donc
**pas** besoin de refaire l'étape de connexion : allez directement à l'étape 1. Si un jour cette
connexion disparaît à nouveau (ex. après un nouveau reset du volume Superset), la marche à suivre
manuelle est : **Paramètres → Connexions aux bases de données → + DATABASE → PostgreSQL**, Host
`postgres-analytics`, Port `5432`, Database `dprest_analytics`, Username `dprest_lecture`, Password =
`ANALYTICS_RO_PASSWORD` du fichier `.env` (jamais collé ailleurs que dans ce formulaire), Display Name
**PostgreSQL analytique (dprest_analytics)**.

---

## Étape 1 — Créer les 7 jeux de données de la famille A

Un « jeu de données » (*dataset*) dans Superset est une table ou une vue PostgreSQL rendue disponible
pour construire des graphiques dessus. On les crée tous maintenant, on construira les graphiques après.

Pour chacune des 7 lignes du tableau ci-dessous :

1. Menu du haut → **Datasets** → bouton **+ DATASET** (en haut à droite).
2. **Database** : choisir *PostgreSQL analytique (dprest_analytics)* dans la liste déroulante (déjà
   connectée depuis la création initiale de l'étape 6 — si elle n'apparaît pas, voir
   `docs/guides/etape6_superset.md`, section connexion).
3. **Schema** : `public`.
4. **See table schema** / champ table : sélectionner le nom exact dans la colonne « Nom réel » ci-dessous.
5. Bouton **ADD** (ou **CREATE DATASET AND CREATE CHART**, mais choisissez juste **ADD** ici — on
   créera les graphiques à l'étape 2, un par un, pour garder le contrôle sur le type et les couleurs).

| # | Nom réel (table/vue PostgreSQL) | Sert pour |
|---|---|---|
| 1 | `kpi_prestations_jour` | KPI 1, 3, 6, 7, 8, 9, 12, 13, 14, 15 |
| 2 | `kpi_prestations_centre_jour` | KPI 5, 10 |
| 3 | `kpi_prestations_praticien_jour` | KPI 11 |
| 4 | `kpi_factures_jour` | KPI 2/4 |
| 5 | `kpi_prestations_assure_jour` | (support des vues 6 et 7, pas utilisée seule) |
| 6 | `v_top10_assures` | KPI 23 |
| 7 | `v_top10_assures_par_centre` | KPI 24 |

> Les vues (6 et 7) apparaissent dans la même liste déroulante que les tables — Superset ne fait pas
> la différence à l'affichage, c'est normal.

**Deux jeux de données supplémentaires, déjà créés pour vous (rien à faire ici)** — ajoutés le
2026-09-15 en même temps que les dimensions centres/praticiens (`docs/decisions.md`) :

| Nom (Custom SQL, pas une table) | Contenu | Utilisé à |
|---|---|---|
| `v_top_centres_sante_nomme` | `kpi_prestations_centre_jour` joint à `dim_centres_sante` | Étape 3.9 |
| `v_top_praticiens_nomme` | `kpi_prestations_praticien_jour` joint à `dim_professionnels_sante`, avec `nom_complet` déjà assemblé | Étape 3.10 |

Ils apparaissent dans **Datasets** comme les autres, repérables par une icône légèrement différente
(dataset "virtuel" basé sur une requête, pas sur une table physique) — c'est normal, ne les recréez pas.

**Dimensions brutes également disponibles**, si vous voulez construire d'autres graphiques plus tard
(ex. une carte des centres) : `dim_centres_sante`, `dim_professionnels_sante`, `dim_collectivites`
(coordonnées géographiques, `collectivite_latitude`/`longitude` — nullables, ~8 % des localités non
géocodées côté simulateur, un centre concerné apparaîtra juste sans position, pas en erreur).

---

## Étape 2 — Couleurs : ce qu'on applique une fois, pour tout le dashboard

Plutôt que de choisir une couleur par graphique (source du rendu incohérent que vous n'aimiez pas),
on fixe **un seul jeu de couleurs, validé, appliqué à tout le dashboard d'un coup**, à l'étape 4. Pour
l'instant, retenez juste ceci pour la suite : quand un réglage de couleur individuel est demandé dans
un graphique (rare, seulement pour les jauges), utilisez :

- **Bleu** `#2a78d6` — couleur neutre par défaut (courbes, barres à une seule série).
- **Vert** `#0ca30c` — zone « bon » d'une jauge/seuil (ex. taux de couverture élevé).
- **Orange** `#fab219` — zone « attention ».
- **Rouge** `#d03b3b` — zone « critique ».

---

## Étape 3 — Les 16 graphiques

Pour chaque graphique : menu **Charts** → **+ GRAPHIQUE** (haut à droite) → choisir le **jeu de
données** indiqué → choisir le **type de visualisation** indiqué (champ de recherche en haut de la
fenêtre de choix, tapez le nom anglais donné ci-dessous) → **CREATE NEW CHART**. Puis configurez selon
les instructions, cliquez **RUN QUERY** (bouton bleu, en haut du panneau de configuration) pour
prévisualiser, vérifiez le chiffre contre le SQL de contrôle indiqué, puis **SAVE** (en haut à droite)
en donnant exactement le nom demandé (le nom sert à les retrouver à l'étape 4).

### 3.1 — KPI 1 : Nombre de prestations

- Jeu de données : `kpi_prestations_jour`. Type : **Big Number**.
- Onglet **DATA** → **METRIC** : cliquer, choisir **Simple** → colonne `nombre_prestations` →
  agrégation **SUM**.
- Nom du graphique : **Prestations totales**.
- Contrôle : `SELECT SUM(nombre_prestations) FROM kpi_prestations_jour;`

### 3.2 — KPI 2/4 : Nombre de passages

- Jeu de données : `kpi_factures_jour`. Type : **Big Number**.
- METRIC : SUM sur `nombre_factures`.
- Nom : **Passages (factures) totaux**.
- Contrôle : `SELECT SUM(nombre_factures) FROM kpi_factures_jour;`

### 3.3 — KPI 3 : Montant total facturé

- Jeu de données : `kpi_prestations_jour`. Type : **Big Number**.
- METRIC : SUM sur `montant_depense`.
- Onglet **CUSTOMIZE** → **Number format** : saisir `,.0f` (nombre complet, séparateur milliers,
  sans décimale). Pas de champ dédié à une devise dans cette version : mettez l'unité dans le **nom**
  du graphique plutôt que dans un champ de configuration.
- Nom : **Montant total facturé (F CFA)**.
- Contrôle : `SELECT SUM(montant_depense) FROM kpi_prestations_jour;`

### 3.4 — KPI 5 : Nombre de centres actifs

- Jeu de données : `kpi_prestations_centre_jour`. Type : **Big Number**.
- METRIC : cliquer **Simple**, colonne `centre_sante_code`, agrégation **COUNT DISTINCT**.
- Nom : **Centres actifs**.
- Contrôle : `SELECT COUNT(DISTINCT centre_sante_code) FROM kpi_prestations_centre_jour;`

### 3.5 — KPI 6 : Prestations par type d'acte

- Jeu de données : `kpi_prestations_jour`. Type : **Bar Chart** (pas de camembert : 4 catégories se
  comparent mieux en hauteur de barre qu'en angle de secteur).
- **X-AXIS** : colonne `prestation_code`.
- **METRICS** : SUM sur `nombre_prestations`.
- Onglet **CUSTOMIZE** : **Sort bars** cochée (tri décroissant), **Show value** cochée (affiche le
  chiffre au-dessus de chaque barre — utile ici, seulement 4 barres).
- Nom : **Prestations par type d'acte**.
- Contrôle : `SELECT prestation_code, SUM(nombre_prestations) FROM kpi_prestations_jour GROUP BY 1 ORDER BY 2 DESC;`

### 3.6 — KPI 7 : Prestations par type de facture

- Identique à 3.5, mais **X-AXIS** : `type_facture_code`. Nom : **Prestations par type de facture**.

### 3.7 — KPI 8 : Prestations par régime

- Identique à 3.5, **X-AXIS** : `regime_code`. Nom : **Prestations par régime**.

### 3.8 — KPI 9 : Prestations par type de centre

- Identique à 3.5, **X-AXIS** : `centre_sante_type_libelle`. Nom : **Prestations par type de centre**.
- Avec 6 catégories aux libellés longs, dans l'onglet **CUSTOMIZE**, cochez **Bar orientation :
  horizontal** pour que les libellés restent lisibles sans se chevaucher.

### 3.9 — KPI 10 : Top centres de santé

**Ajout du 2026-09-15** : le nom du centre (pas seulement son code) est maintenant disponible,
via la dimension `dim_centres_sante` alimentée par CDC (voir `sql/analytics/004_dim_referentiels.sql`
et `docs/decisions.md`). Un jeu de données Custom SQL a déjà été créé pour vous —
**`v_top_centres_sante_nomme`** — qui joint `kpi_prestations_centre_jour` à cette dimension. Utilisez-le
directement, pas la table brute.

- Jeu de données : **`v_top_centres_sante_nomme`**. Type : **Table**.
- **COLUMNS** : `centre_sante_denomination` (le nom, plus lisible que le code), puis en métriques SUM
  sur `nombre_prestations` et SUM sur `montant_depense`.
- Onglet **DATA** → **SORT BY** : la métrique `SUM(nombre_prestations)`, ordre **descending**.
- **Row limit** : 10.
- Nom : **Top 10 centres de santé**.
- Contrôle : `SELECT d.centre_sante_denomination, SUM(c.nombre_prestations), SUM(c.montant_depense) FROM kpi_prestations_centre_jour c LEFT JOIN dim_centres_sante d ON d.centre_sante_code = c.centre_sante_code GROUP BY 1 ORDER BY 2 DESC LIMIT 10;`
- Si un centre affiche un nom vide : sa dénomination n'a pas encore été répliquée par CDC (rare, cas
  transitoire) — pas une erreur de la requête.

### 3.10 — KPI 11 : Top praticiens

De même, le jeu de données **`v_top_praticiens_nomme`** joint `kpi_prestations_praticien_jour` à
`dim_professionnels_sante` et calcule déjà `nom_complet` (nom + prénoms).

- Jeu de données : **`v_top_praticiens_nomme`**. Type : **Table**.
- **COLUMNS** : `nom_complet`, puis en métriques SUM sur `nombre_prestations` et SUM sur
  `montant_depense`.
- Mêmes réglages que 3.9 (tri décroissant sur `nombre_prestations`, Row limit 10).
- Nom : **Top 10 praticiens**.

### 3.11 — Passages par jour (évolution)

- Jeu de données : `kpi_factures_jour`. Type : **Line Chart**.
- **X-AXIS** : colonne `jour` (Superset la reconnaît automatiquement comme axe temporel).
- **METRICS** : SUM sur `nombre_factures`.
- Nom : **Passages par jour**.
- C'est le seul graphique de la famille A qui montre une tendance dans le temps — utile pour visualiser
  la fraîcheur/l'activité au fil des jours, en complément du panneau Grafana équivalent.

### 3.12 — KPI 14 : Taux de couverture CMU

- Jeu de données : `kpi_prestations_jour`. Type : **Gauge Chart** (un taux borné entre 0 et 100 % se
  lit mieux sur une jauge qu'en nombre brut : on voit immédiatement où on se situe par rapport à la
  limite).
- **METRIC** : cliquer sur l'onglet **Custom SQL** (à côté de Simple) dans le panneau métrique, coller :
  ```sql
  SUM(montant_pris_en_charge) / NULLIF(SUM(montant_depense), 0) * 100
  ```
- Onglet **CUSTOMIZE** → **Min** : 0, **Max** : 100.
- **Intervals** : `50,80,100` — **Interval colors** : rouge, orange, vert (dans cet ordre) en cliquant
  sur chaque pastille et en collant les codes de l'étape 2.
- Nom : **Taux de couverture CMU**.
- Contrôle : `SELECT SUM(montant_pris_en_charge) / SUM(montant_depense) * 100 FROM kpi_prestations_jour;` (doit donner ~82).

### 3.13 — KPI 12/13 : Répartition du montant facturé

- Jeu de données : `kpi_prestations_jour`. Type : **Bar Chart**, avec **STACK** activé (barre
  empilée : la bonne forme pour montrer deux montants qui, ensemble, font 100 % du total).
- **X-AXIS** : laisser vide ou mettre une valeur constante (un seul groupe) — dans **BREAKDOWNS**,
  vous ne pouvez pas empiler deux *métriques différentes* nativement en Bar Chart classique : utilisez
  plutôt **METRICS** avec deux entrées : SUM sur `montant_pris_en_charge` et SUM sur
  `montant_reste_a_charge`. Superset affichera automatiquement les deux comme deux barres/segments
  distincts, une couleur chacun (légende visible).
- Nom : **Montant pris en charge vs reste à charge**.
- Contrôle : `SELECT SUM(montant_pris_en_charge), SUM(montant_reste_a_charge) FROM kpi_prestations_jour;`

### 3.14 — KPI 15 : Montant moyen par prestation (F CFA)

- Jeu de données : `kpi_prestations_jour`. Type : **Big Number**.
- METRIC → **Custom SQL** :
  ```sql
  SUM(montant_depense) / NULLIF(SUM(nombre_prestations), 0)
  ```
- Nom : **Montant moyen par prestation (F CFA)**.
- Contrôle attendu : ~10 000 F (toutes les prestations du simulateur sont facturées au même montant —
  normal, voir la limite n°4 de `docs/kpi.md`).

### 3.15 — KPI 23 : Top 10 des assurés

- Jeu de données : `v_top10_assures`. Type : **Table**.
- **COLUMNS** : `rang`, `personne_uuid`, `nombre_prestations`, `montant_depense` — pas de métrique à
  ajouter, la vue calcule déjà le classement, on affiche les colonnes telles quelles.
- Onglet **DATA** → **SORT BY** : colonne `rang`, ascending.
- Nom : **Top 10 des assurés**.
- Rappel sécurité (`docs/kpi.md`, KPI 23-24) : `personne_uuid` est un identifiant opaque, jamais un
  nom. Ne rajoutez aucune colonne d'identité dans ce graphique.

### 3.16 — KPI 24 : Top 10 des assurés par centre

- Jeu de données : `v_top10_assures_par_centre`. Type : **Table**.
- **COLUMNS** : `centre_sante_code`, `rang`, `personne_uuid`, `nombre_prestations`, `montant_depense`.
- Onglet **DATA** → activez un **filtre natif** sur `centre_sante_code` (voir étape 4.3) plutôt que de
  faire 30 graphiques — un seul tableau, filtrable par centre depuis le dashboard.
- Nom : **Top 10 des assurés par centre**.

---

## Étape 4 — Assembler le dashboard

1. Menu **Dashboards** → **+ DASHBOARD**.
2. Titre (en haut, cliquer sur « Ajouter le titre du tableau de bord ») : **DPREST - Prestations et facturation**.
3. Panneau de droite : les 16 graphiques sauvegardés apparaissent sous l'onglet **Charts**. Glissez-les
   un par un dans la zone centrale, dans cet ordre suggéré (par ligne, en glissant côte à côte pour les
   petits formats) :
   - Ligne 1 (Big Numbers, 5 cases) : Prestations totales, Passages totaux, Montant total facturé,
     Montant moyen par prestation (F CFA), Centres actifs.
   - Ligne 2 (répartitions, 4 cases) : par type d'acte, par type de facture, par régime, par type de centre.
   - Ligne 3 (classements, 2 cases larges) : Top 10 centres, Top 10 praticiens.
   - Ligne 4 : Passages par jour (pleine largeur).
   - Ligne 5 (financier, 2 cases) : Taux de couverture CMU, Montant pris en charge vs reste à charge.
   - Ligne 6 (assurés, 2 cases larges) : Top 10 des assurés, Top 10 des assurés par centre.
4. **Filtre de période** (recommandé, corrige la limite notée dans `etape6_superset.md`) : icône
   **entonnoir/Filters** en haut à gauche → **+ Add/Edit Filters** → **+ Add filter** → type
   **Time range**, colonne `jour` → **Apply**. Ce filtre s'appliquera à tous les graphiques qui
   utilisent une colonne `jour`.
5. **Couleurs cohérentes** : bouton **⋮** (trois points, en haut à droite) → **Edit properties** →
   onglet **Colors** → dans **Color Scheme**, choisissez un même jeu de couleurs pour tout le
   dashboard (ex. *Superset Colors*, le seul réglage qui garantit que chaque catégorie garde la même
   couleur sur tous les graphiques du dashboard, au lieu d'un jeu recalculé par graphique).
6. Bouton **SAVE** (en haut à droite), puis **⋮** → **Publish** (ou basculez le badge « brouillon » en
   « publié » à côté du titre) — un dashboard non publié n'apparaît pas pour les autres comptes,
   c'est ce qui a causé la confusion notée dans `docs/decisions.md` lors de la mise en place initiale.

---

## Étape 5 — Vérifier

Reprenez le principe de `etape6_superset.md` : chaque chiffre affiché doit être identique à une requête
SQL de contrôle indépendante, lancée via **SQL Lab** (menu **SQL** → **SQL Lab**, base *PostgreSQL
analytique*) ou en ligne de commande :

```powershell
podman exec pipeline_temps_reel-postgres-analytics-1 psql -U dprest -d dprest_analytics -c "SELECT SUM(nombre_prestations) FROM kpi_prestations_jour;"
```

Cochez, pour chacun des 16 graphiques : chiffre affiché = chiffre du contrôle SQL correspondant (donné
dans chaque section ci-dessus).

---

## Suite

Une fois ce dashboard validé, passez à `docs/guides/etape6b_superset_ententes.md` pour la famille
Ententes préalables — l'étape 0 (remise à zéro) ne se refait pas, elle est déjà faite.
