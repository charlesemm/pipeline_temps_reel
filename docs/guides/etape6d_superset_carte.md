# Guide détaillé — Superset, carte des centres de santé (deck.gl)

À faire après `docs/guides/etape6a_superset_prestations.md` (les dimensions géographiques utilisées
ici — `dim_centres_sante`, `dim_collectivites`, vue `v_centres_sante_geo` — sont créées par
`sql/analytics/004_dim_referentiels.sql`, déjà en place depuis le 2026-09-15).

**Pourquoi deck.gl et pas le type « Country Map »** : le plugin natif Superset **Country Map** ne
couvre qu'une liste fermée de pays avec un fichier TopoJSON embarqué dans le frontend (France, USA,
Maroc, Brésil...) — la Côte d'Ivoire n'y figure pas, et l'ajouter demanderait de forker et reconstruire
`superset-frontend` (voir `docs/decisions.md`, discussion du 2026-09-17). **deck.gl Scatterplot**
affiche des points géolocalisés sur un fond de carte mondial standard, sans dépendre d'un découpage
administratif par pays — ça marche pour n'importe quel pays, y compris la Côte d'Ivoire, avec les
coordonnées déjà présentes dans `dim_collectivites`.

**Limite à connaître avant de commencer** : `dim_collectivites.collectivite_latitude/longitude` sont
nullables — environ 8 % des localités ne sont pas géocodées côté simulateur (voir commentaire dans
`sql/analytics/004_dim_referentiels.sql`). Ces centres-là n'apparaîtront pas sur la carte, pas en erreur.

---

## Étape 1 — Le jeu de données (Custom SQL)

Contrairement aux datasets des guides précédents (une table ou une vue existante), celui-ci se crée
directement avec une requête, comme `v_top_centres_sante_nomme` à l'étape 3.9 d'`etape6a`.

1. **SQL** → **SQL Lab** → base *PostgreSQL analytique (dprest_analytics)*, schéma `public`.
2. Coller et exécuter :
   ```sql
   SELECT
       c.centre_sante_code,
       c.centre_sante_denomination,
       c.collectivite_denomination,
       c.collectivite_latitude,
       c.collectivite_longitude,
       SUM(k.nombre_prestations) AS nombre_prestations,
       SUM(k.montant_depense)    AS montant_depense
   FROM v_centres_sante_geo AS c
   JOIN kpi_prestations_centre_jour AS k ON k.centre_sante_code = c.centre_sante_code
   WHERE c.collectivite_latitude IS NOT NULL
     AND c.collectivite_longitude IS NOT NULL
   GROUP BY 1, 2, 3, 4, 5;
   ```
   Une ligne par centre géocodé, déjà agrégée (totaux tous jours confondus) — pas besoin de laisser
   Superset ré-agréger côté chart.
3. Bouton **Enregistrer** → **Enregistrer le dataset** → nom **`v_centres_sante_carte`**.

---

## Étape 2 — Le graphique

1. **Charts** → **+ GRAPHIQUE** → jeu de données `v_centres_sante_carte` → type **deck.gl Scatterplot**
   (chercher « deck.gl » dans le champ de recherche des types).
2. Onglet **DONNÉES** (dans Superset 4.1.1, les champs longitude/latitude sont un seul contrôle
   combiné, pas deux séparés — piège rencontré le 2026-09-17, un utilisateur avait mis
   `centre_sante_code` dans les deux emplacements par défaut) :
   - **LONGITUDE & LATITUDE** : supprimer tout contenu par défaut, puis ajouter **dans cet ordre**
     `collectivite_longitude` **d'abord**, `collectivite_latitude` **ensuite** — l'ordre des deux
     badges compte, Superset lit le premier comme longitude.
   - **FILTRES** : n'en ajouter aucun sur `collectivite_latitude`/`collectivite_longitude` — la
     requête SQL du dataset (étape 1) exclut déjà les valeurs NULL, un filtre ici est inutile et
     source de confusion.
   - **POINT SIZE** : par défaut une valeur fixe (`1000`) — basculer sur la colonne
     `nombre_prestations`, pour que la taille du point reflète l'activité du centre.
   - **NOMBRE DE RANGÉES MAXI** : la valeur par défaut (50000) convient, largement au-dessus des ~30
     centres actuels — pas besoin de la réduire.
3. Toujours dans **DONNÉES**, section **Map** :
   - **UNITÉ DE POINT** : `Pixels` (pas `Square meters`, le défaut — un rayon en mètres est illisible
     à l'échelle d'un pays).
   - **Zoom automatique** : cocher, le fond de carte se recentre alors sur les points affichés.
   - **Color** : une couleur unique suffit ici (contrairement aux graphiques de répartition par statut
     des guides précédents, un seul type d'entité est représenté).
4. Activer le **Tooltip** (icône dédiée ou onglet DONNÉES selon la version) sur
   `centre_sante_denomination`, `collectivite_denomination`, `nombre_prestations`, `montant_depense` —
   c'est ce qui rend la carte utile au survol, pas seulement décorative.
5. Nom du graphique : **Répartition géographique des centres de santé**.

---

## Étape 3 — Centrer la carte sur la Côte d'Ivoire

Le fond de carte deck.gl se centre par défaut sur la moyenne des points, donc ça devrait déjà tomber
au bon endroit avec vos données. Si besoin d'un centrage/zoom initial fixe (utile pour que le
graphique s'affiche pareil à chaque ouverture, pas dépendant du zoom laissé par la dernière personne
qui l'a modifié) :

- Onglet **CUSTOMIZE** → **Viewport** → **Fix to** (ou équivalent selon version) → régler manuellement
  latitude ≈ `7.5`, longitude ≈ `-5.5`, zoom ≈ `6` (cadre la Côte d'Ivoire en entier) → **Save current
  viewport**.

---

## Étape 4 — Ajouter au dashboard

Ajoutez ce graphique au dashboard **DPREST - Prestations et facturation** (`etape6a`), par exemple en
pleine largeur après la ligne des Top 10 (section 3.9/3.10) — c'est un complément visuel au classement
textuel des centres, pas un KPI chiffré supplémentaire.

---

## Étape 5 — Vérifier

```powershell
podman exec pipeline_temps_reel-postgres-analytics-1 psql -U dprest -d dprest_analytics -c "SELECT COUNT(*) FROM v_centres_sante_carte;"
```

Comparer au nombre de points affichés sur la carte. Si un centre connu n'apparaît pas : vérifier que sa
collectivité a des coordonnées (`SELECT * FROM dim_collectivites WHERE collectivite_code = '...';`) —
absence de géocodage, pas un bug du graphique.
