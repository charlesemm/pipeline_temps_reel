# Guide détaillé — Superset, dashboard « DPREST - Clinique » (KPI 25, 27, 30, 31, 32, 33)

À faire après `etape6a` et `etape6b` (même instance, même connexion, même palette de couleurs). Même
niveau de détail : aucune connaissance préalable de Superset supposée.

Définitions des KPI et hypothèses H7 à H10 : `docs/kpi.md`, section « Famille C — Prescriptions et
pathologies ». Déploiement du flux qui alimente ces vues : `docs/guides/famille_c_deploiement.md`.

**Données de santé sensibles (loi n°2013-450).** Ce dashboard est **réservé au SGD** tant que la DPREST
ne l'a pas validé : voir l'étape 4.

---

## Avant de commencer

1. **Le flux doit avoir fini son rattrapage** (job Flink `RUNNING`, tables `kpi_prescriptions_medicament_jour`
   et `kpi_pathologies_jour` alimentées). Sinon les graphiques seront vides ou incomplets, sans que
   rien ne soit cassé.
2. Connexion à Superset avec le compte **`sgd_admin`** (rôle Admin), voir `secrets/identifiants_comptes.md`.
3. La connexion « PostgreSQL analytique (dprest_analytics) » existe déjà. Elle lit avec le compte
   `dprest_lecture`, qui **ne voit que les vues masquées et les dimensions** : c'est voulu (H10).
   Conséquence : dans Superset, vous ne pouvez pas interroger les tables `fait_*` ni
   `kpi_prescriptions_medicament_jour`. Les contrôles « non masqués » se font hors Superset (étape 5).

**Règle de masquage (H10)** : chaque vue supprime les regroupements de **moins de 5**. Si un graphique est
vide ou plus petit qu'attendu, c'est soit le rattrapage qui n'est pas fini, soit des effectifs trop faibles.

---

## Étape 1 — Créer les 6 jeux de données

**Datasets** → **+ DATASET** → base *PostgreSQL analytique (dprest_analytics)* → schéma `public` → choisir la
vue → **CREATE DATASET**. (Les vues apparaissent dans la même liste que les tables.)

| # | Nom réel | Sert pour | Grain |
|---|---|---|---|
| 1 | `v_kpi_prescriptions_jour` | KPI 25 | jour |
| 2 | `v_top10_medicaments` | KPI 27 | toute période, 10 lignes |
| 3 | `v_top10_pathologies` | KPI 30 | toute période, 10 lignes |
| 4 | `v_kpi_ep_prescriptions` | KPI 31 | type de demande × statut |
| 5 | `v_kpi_ep_medicaments` | KPI 32 | type de demande × statut |
| 6 | `v_kpi_ep_pathologies` | KPI 33 | statut × pathologie |

Si une vue n'apparaît pas dans la liste : `sql/analytics/010_kpi_clinique.sql` n'a pas été appliqué, ou le
droit n'a pas été accordé (`role_kpi_lecture`). Voir `docs/guides/famille_c_deploiement.md`.

---

## Étape 2 — Les graphiques

Même mode opératoire que les guides précédents : **Charts** → **+ GRAPHIQUE** → choisir le jeu de données →
choisir le type → configurer → **RUN QUERY** → **SAVE** avec le nom exact indiqué.

### 2.1 — KPI 25 : Médicaments prescrits (deux graphiques)

**a) Total**
- Jeu de données : `v_kpi_prescriptions_jour`. Type : **Big Number**.
- METRIC : SUM sur `nombre_prescriptions`.
- Nom : **Médicaments prescrits (total)**.

**b) Évolution par jour**
- Jeu de données : `v_kpi_prescriptions_jour`. Type : **Time-series Bar Chart** (ou Line Chart).
- **X-AXIS** : `jour`, **Time grain** : Day. **METRICS** : SUM sur `nombre_prescriptions`.
- Axe Y : intitulé « Médicaments prescrits ».
- Nom : **Médicaments prescrits par jour**.
- Rappel H8 : le « nombre de médicaments » est un nombre de **lignes de prescription**, pas de boîtes
  (la quantité prescrite vaut toujours 1). Rappel H9 : le jour est la **date de début de la prescription**.
- Un jour à moins de 5 prescriptions est masqué : le total affiché peut être inférieur de quelques unités
  au total réel (voir l'étape 5).

### 2.2 — KPI 27 : Top 10 des médicaments prescrits

- Jeu de données : `v_top10_medicaments`. Type : **Table**.
- **QUERY MODE** : Aggregate. **DIMENSIONS** : `medicament_denomination`. **METRICS** : SUM sur
  `nombre_prescriptions`.
- **Row limit** : 10. Tri : la métrique, ordre décroissant.
- Onglet **CUSTOMIZE** : cocher **Show cell bars** (les dénominations sont longues, un tableau se lit mieux
  qu'un histogramme).
- Nom : **Top 10 médicaments prescrits**.
- La vue porte sur **toute la période** : aucun filtre de date ne s'y applique (elle n'a pas de colonne
  `jour`). Pour classer sur une période libre, le SGD interroge `kpi_prescriptions_medicament_jour` hors Superset.
- Les écarts entre le 1er et le 10e sont faibles (données synthétiques quasi uniformes, limite 6 de
  `docs/kpi.md`) : ne présentez pas ce classement comme un résultat clinique.

### 2.3 — KPI 30 : Top 10 des pathologies

- Jeu de données : `v_top10_pathologies`. Type : **Table**.
- **DIMENSIONS** : `pathologie_denomination`. **METRICS** : SUM sur `nombre_pathologies`.
- **Row limit** : 10, tri décroissant, **Show cell bars** cochée.
- Nom : **Top 10 pathologies**. Même remarque de période et de prudence que le KPI 27.

### 2.4 — KPI 31 : Ententes avec prescription associée

- Jeu de données : `v_kpi_ep_prescriptions`. Type : **Bar Chart**.
- **X-AXIS** : `statut_code`. **DIMENSIONS** (séries) : `type_demande_code`.
- **METRICS** → onglet **Custom SQL** (taux pondéré, pas une moyenne de taux) :
  ```sql
  SUM(ententes_avec_prescription) * 100.0 / NULLIF(SUM(nombre_ententes), 0)
  ```
- Onglet **CUSTOMIZE** : **Show value** cochée, axe Y intitulé « % d'ententes ».
- Nom : **Ententes avec prescription associée (%)**.
- Variante en Big Number pour le taux global (même formule, sans X-AXIS ni DIMENSIONS) :
  **Ententes avec prescription associée (global)**.
- Lecture : le rattachement passe par la **facture** de l'entente (H7). Une entente n'a pas de médicaments
  propres dans la source.

### 2.5 — KPI 32 : Médicaments prescrits sur les ententes

**a) Volume par statut**
- Jeu de données : `v_kpi_ep_medicaments`. Type : **Bar Chart**.
- **X-AXIS** : `statut_code`. **DIMENSIONS** : `type_demande_code`. **METRICS** : SUM sur
  `nombre_medicaments_prescrits`.
- Nom : **Médicaments prescrits sur les ententes, par statut**.

**b) Moyenne par entente**
- Même jeu de données. Type : **Table**. **DIMENSIONS** : `statut_code`, `type_demande_code`.
- **METRICS** → Custom SQL :
  ```sql
  SUM(nombre_medicaments_prescrits)::numeric / NULLIF(SUM(nombre_ententes), 0)
  ```
- Nom : **Médicaments par entente**.
- Le statut `sans_reponse` (3 cas au 2026-09-21) est masqué par le seuil de 5 : c'est attendu.

### 2.6 — KPI 33 : Pathologies des ententes

- Jeu de données : `v_kpi_ep_pathologies`. Type : **Pivot Table**.
- **ROWS** : `pathologie_denomination`. **COLUMNS** : `statut_code`. **METRICS** : SUM sur `nombre_pathologies`.
- Onglet **CUSTOMIZE** : totaux de lignes et de colonnes cochés. **Row limit** : 20.
- Nom : **Pathologies des ententes, par statut**.
- **Corrélation, pas explication** : voir la note du KPI 33 dans `docs/kpi.md`. Ne concluez pas qu'une
  pathologie « cause » un refus.

---

## Étape 3 — Assembler le dashboard

1. **Dashboards** → **+ DASHBOARD**. Titre : **DPREST - Clinique (SGD)**.
2. Disposition suggérée :
   - Ligne 0 : bloc **Markdown** avec le logo CNAM, même code que les deux autres dashboards :
     `<img src="/assets/logo-cnam.png" alt="Logo CNAM" height="80">` (adresse relative, ne change pas avec l'IP de la VM).
   - Ligne 1 (Big Numbers) : Médicaments prescrits (total), Ententes avec prescription associée (global).
   - Ligne 2 : Médicaments prescrits par jour (pleine largeur).
   - Ligne 3 : Top 10 médicaments, Top 10 pathologies.
   - Ligne 4 : Ententes avec prescription associée (%), Médicaments prescrits sur les ententes par statut,
     Médicaments par entente.
   - Ligne 5 : Pathologies des ententes par statut (pleine largeur).
3. **Filtres** :
   - Période sur `jour` : ne s'applique qu'aux deux graphiques du KPI 25 (les autres vues n'ont pas de date).
   - Filtre sur `statut_code` : s'applique aux KPI 31, 32, 33. Ajoutez-le comme filtre natif.
4. **⋮** → **Edit properties** → onglet **Colors** → même **Color Scheme** que les dashboards Prestations et
   Ententes préalables.
5. **SAVE**. **Ne le publiez pas pour la DPREST** tant que le SGD n'a pas validé les KPI cliniques.

---

## Étape 4 — Réserver l'accès au SGD

Le compte `dprest_lecteur` a le rôle **Gamma** : il ne voit un dashboard que si son rôle a le droit *datasource
access* sur ses jeux de données. **N'accordez ce droit sur aucun des 6 jeux de données ci-dessus**, ni à
`Gamma`, ni à un rôle DPREST. Ainsi le dashboard reste invisible pour la DPREST, sans configuration
supplémentaire. Vérifiez en vous connectant avec `dprest_lecteur` : la liste des dashboards ne doit pas
contenir « DPREST - Clinique (SGD) ».

Quand la DPREST valide les KPI : créer un rôle (Settings → List Roles → + ) avec *datasource access* sur les 6
jeux de données et *can read* sur `Dashboard` et `Chart`, l'affecter à `dprest_lecteur`, puis retirer le suffixe
« (SGD) » du titre.

---

## Étape 5 — Vérifier

Comparez chaque chiffre du dashboard à une requête indépendante. Les vues sont **masquées** (H10), les tables
sous-jacentes **non** : on compare donc les deux, puis on compare à la source.

**1. Dans SQL Lab (compte `dprest_lecture`, vues masquées seulement)** :
```sql
SELECT SUM(nombre_prescriptions) FROM v_kpi_prescriptions_jour;
```

**2. Hors Superset, en propriétaire (tables non masquées)** :
```powershell
podman exec pipeline_temps_reel-postgres-analytics-1 psql -U dprest -d dprest_analytics -c "SELECT SUM(nombre_prescriptions) FROM kpi_prescriptions_medicament_jour;"
```

**3. Source** : `evaluation/controle_source.sql`, sections 20 à 24, et leur équivalent analytique
`evaluation/controle_analytics.sql`, mêmes numéros. Les sections 20 à 24 doivent donner **exactement** les
mêmes chiffres deux à deux ; la section 25 doit renvoyer 0 doublon.

Les écarts **normaux** entre une vue masquée et la table : les jours à moins de 5 prescriptions (KPI 25), les
groupes de moins de 5 ententes (KPI 31, 32), les pathologies à moins de 5 cas par statut (KPI 33), les
médicaments et pathologies sous le seuil dans les classements. Un écart **supérieur à ces effectifs masqués**
est un vrai problème.

Les valeurs de référence dépendent du volume de la source au moment de la mesure : comparez les sources
entre elles plutôt qu'à des valeurs figées.

---

## Étape 6 — Exporter (à ne pas oublier)

Les dashboards Superset vivent dans la base de métadonnées de Superset : un reset des volumes les efface.
**⋮** → **Export** sur le dashboard, puis enregistrer le fichier dans `superset/exports/` et le commiter.

---

## Pièges connus

| Symptôme | Cause probable |
|---|---|
| Graphique vide, aucune erreur | Rattrapage Flink pas fini, ou tous les regroupements sont sous le seuil de 5 |
| `permission denied for relation ...` | Un jeu de données pointe sur une table `fait_*` ou `kpi_*` au lieu d'une vue : refaites-le sur la vue |
| La colonne `famille` ou `centre` n'existe pas | Sans rapport avec ce guide : c'est la table `qualite_anomalies` (guide 6c) |
| Le total du graphique est légèrement inférieur au total réel | Jours ou groupes masqués par le seuil de 5 (étape 5) |
| Le filtre de période ne change rien | Normal pour les vues sans colonne `jour` (étape 3) |
| `column ... does not exist` après une modification de vue | **Sync columns from source** sur le jeu de données, puis rouvrir la métrique du graphique (voir le piège du guide 6b, KPI 19) |
