# Guide détaillé — Superset, dashboard « DPREST - Ententes préalables »

À faire après `docs/guides/etape6a_superset_prestations.md` (mêmes 7 jeux de données déjà créés dans
la même instance, pas besoin de les refaire). Même niveau de détail : aucune connaissance préalable
de Superset supposée.

Définitions des KPI : `docs/kpi.md`, section « Famille B — Ententes préalables » (KPI 16 à 22).
Rappel important de cette section : le KPI mensuel (16-19, 21 au grain mois) est **volontairement pas
temps réel** — il ne porte que sur les mois entièrement clos, pour donner un chiffre réglementaire
définitif. Ne le confondez pas avec les mêmes KPI au grain jour, qui eux sont continus.

---

## Étape 1 — Créer les 3 jeux de données de la famille B

Même procédure qu'à l'étape 1 du guide précédent : **Datasets** → **+ DATASET** → base *PostgreSQL
analytique (dprest_analytics)* → schéma `public` → choisir la table/vue → **ADD**.

| # | Nom réel | Sert pour |
|---|---|---|
| 1 | `kpi_ententes_prealables_jour` | KPI 16, 17, 18, 19, 21, 22 |
| 2 | `kpi_ententes_prealables_agent_jour` | KPI 20 |
| 3 | `v_kpi_ententes_prealables_mois` | KPI mensuel certifié (16-19, 21 au grain mois) |

---

## Étape 2 — Les 8 graphiques

Même mode opératoire que le guide précédent : **Charts** → **+ GRAPHIQUE** → choisir le jeu de
données → choisir le type → configurer → **RUN QUERY** → vérifier contre le contrôle SQL → **SAVE**
avec le nom exact indiqué.

### 2.1 — KPI 16 : Nombre d'EP générées

- Jeu de données : `kpi_ententes_prealables_jour`. Type : **Big Number**.
- METRIC : SUM sur `nombre_ententes`.
- Nom : **Ententes préalables générées**.
- Contrôle : `SELECT SUM(nombre_ententes) FROM kpi_ententes_prealables_jour;`

### 2.2 — KPI 17 : Taux de réponse

- Jeu de données : `kpi_ententes_prealables_jour`. Type : **Gauge Chart** (même logique qu'à l'étape
  3.12 du guide précédent : un taux borné à 100 % se lit mieux sur une jauge).
- METRIC → onglet **Custom SQL** :
  ```sql
  100 - SUM(CASE WHEN statut_code = 'sans_reponse' THEN nombre_ententes ELSE 0 END)
        * 100.0 / NULLIF(SUM(nombre_ententes), 0)
  ```
  (traduit directement l'hypothèse H4 de `docs/kpi.md` : une EP « sans réponse » est celle qui n'a
  aucune ligne de statut — voir ce document si vous voulez comprendre pourquoi cette formule et pas
  une autre).
- **Min** 0, **Max** 100. **Intervals** `0,95,100`, couleurs rouge/orange/vert (une EP sans réponse est
  un signal fort dès qu'il y en a — seuil d'alerte volontairement haut, à 95 %, contrairement à la
  jauge de couverture CMU).
- Nom : **Taux de réponse aux EP**.
- Contrôle attendu : ~99,98 % (3 sans réponse sur 12 340, d'après `docs/kpi.md`).

### 2.3 — KPI 18 : Ventilation par statut

- Jeu de données : `kpi_ententes_prealables_jour`. Type : **Bar Chart**.
- **X-AXIS** : `statut_code`. **METRICS** : SUM sur `nombre_ententes`.
- Onglet **CUSTOMIZE** : **Sort bars** cochée, **Show value** cochée (4 catégories : acceptée, refusée,
  validée d'office, sans réponse — avec cette dernière proche de zéro, la valeur affichée en clair
  évite qu'elle disparaisse visuellement).
- Nom : **Ententes préalables par statut**.
- Contrôle : `SELECT statut_code, SUM(nombre_ententes) FROM kpi_ententes_prealables_jour GROUP BY 1 ORDER BY 2 DESC;`

### 2.4 — KPI 19 : Délai moyen de traitement

- Jeu de données : `kpi_ententes_prealables_jour`. Type : **Big Number**.
- METRIC → **Custom SQL** (moyenne pondérée par le nombre d'EP de chaque ligne, pas une moyenne simple
  des moyennes journalières — sinon un jour à faible volume pèserait autant qu'un jour à fort volume) :
  ```sql
  SUM(delai_moyen_heures * nombre_ententes)
  / NULLIF(SUM(CASE WHEN delai_moyen_heures IS NOT NULL THEN nombre_ententes ELSE 0 END), 0)
  ```
  **Correction du 2026-09-17 (formule)** : la version précédente divisait par `SUM(nombre_ententes)`
  sur *toutes* les lignes, y compris `statut_code = 'sans_reponse'` (qui n'a par définition aucun
  délai, `delai_moyen_heures` y est `NULL` — voir H4 de `docs/kpi.md`). Le numérateur ignorait déjà
  ces lignes (`NULL * x` = `NULL`, écarté par `SUM`), mais le dénominateur les comptait quand même : le
  délai moyen affiché était donc systématiquement sous-estimé. La formule corrigée exclut ces lignes
  des deux côtés — c'est exactement celle déjà utilisée dans `v_kpi_ententes_prealables_mois`
  (`sql/analytics/003_kpi_ep_mois_vue.sql` + `007_kpi_ep_delai_heures.sql`), qui elle n'a jamais eu ce
  problème.

  **Correction du 2026-09-17 (unité)** : colonne renommée `delai_moyen_jours` → `delai_moyen_heures`
  (`sql/analytics/007_kpi_ep_delai_heures.sql`). En jours pleins, le résultat était 0,00 pour 13 339
  EP sur 13 341 — pas un bug, mais une unité trop grossière : le simulateur traite chaque EP en
  quelques minutes/heures, sans jamais franchir minuit (voir H3 révisée, `docs/kpi.md`). Si vous avez
  déjà ce graphique en base, rouvrez-le (**Charts** → **Délai moyen de traitement** → onglet **DATA**
  → **Custom SQL**) et remplacez entièrement la formule (nom de colonne ET logique de calcul ont
  changé).

  **Piège rencontré** : `ERROR: column "delai_moyen_jours" does not exist` en lançant le graphique,
  même après avoir fait **Sync columns from source** sur le dataset. Cause : synchroniser les colonnes
  du dataset ne met PAS à jour la définition d'une métrique déjà enregistrée sur ce graphique — une
  métrique **Simple** (choisie dans une liste déroulante, agrégation `AVG`/`SUM` sur une colonne)
  garde en mémoire le nom de colonne au moment où elle a été créée, colonne qui n'existe plus. Marche à
  suivre : ouvrir le graphique en édition → panneau **DATA** → cliquer sur la métrique cassée pour la
  rouvrir → si c'est une métrique **Simple**, resélectionner la colonne dans la liste (elle propose
  maintenant `delai_moyen_heures`) ; si c'est censé être la moyenne pondérée de ce guide, **basculer
  sur l'onglet Custom SQL** et coller la formule ci-dessus — une simple `AVG(delai_moyen_heures)` sans
  pondération redonnerait le même biais que la formule d'origine du 2026-09-17 (voir plus haut,
  correction « formule »).
- Pas de champ dédié à l'unité dans cette version de Superset : mettez-la dans le nom du graphique.
- Nom : **Délai moyen de traitement (heures)**.
- Contrôle : `evaluation/controle_analytics.sql`, section 14b (formule identique).

### 2.5 — KPI 20 : Activité par praticien-conseil

- Jeu de données : **`v_kpi_ep_agent_medecin_conseil`** (PAS `kpi_ententes_prealables_agent_jour`
  directement — voir correction ci-dessous). Type : **Table**.
- **COLUMNS** : `agent_nom_complet` (le nom, comme pour les centres/praticiens à l'étape 3.9/3.10
  d'`etape6a` — pas `agent_code`, illisible en dashboard). **Metric** : SUM sur `nombre_ententes`.
- Onglet **DATA** → **SORT BY** : la métrique, descending. **Row limit** : 30 (marge au-dessus des
  ~20 médecins-conseils actuels).
- Nom : **Activité par praticien-conseil**.
- Rappel (`docs/kpi.md`, KPI 20) : cette table exclut déjà les EP `validee_office`, qui ne portent
  aucun praticien identifiable (validation automatique) — ne rajoutez pas de filtre dessus, c'est déjà
  fait en amont par Flink.

**Correction du 2026-09-17** : ce graphique utilisait jusqu'ici directement
`kpi_ententes_prealables_agent_jour`, qui recense **tous** les agents ayant répondu à une EP — agents
`accueil` et `autre` inclus, pas seulement les médecins-conseils. La définition du KPI 20
(`docs/kpi.md`) exige pourtant de restreindre à `AGENT_TYPE_CODE = 'medecin_conseil'`. Cause racine :
`TB_REF_AGENTS` (qui porte ce type) n'était même pas répliquée par CDC — impossible de filtrer une
donnée qu'on ne recevait pas. Corrigé en trois endroits : `connectors/debezium-postgres-json.json`
(table ajoutée), `flink/sql/kpi_prestations.sql` (nouvelle dimension `dim_agents`),
`sql/analytics/006_dim_agents.sql` (vue `v_kpi_ep_agent_medecin_conseil`, qui fait le filtre à la
lecture ET assemble `agent_nom_complet`, sur le même principe que `v_professionnels_sante`). Si vous
avez déjà créé ce graphique sur l'ancien dataset, changez le jeu de données (bouton **⋮** du graphique
en mode édition, ou recréez-le) plutôt que de simplement ajouter un filtre — ni `agent_type_code` ni
le nom de l'agent n'existent dans `kpi_ententes_prealables_agent_jour`, seul le nouveau dataset les porte.
- Contrôle : `SELECT agent_nom_complet, SUM(nombre_ententes) FROM v_kpi_ep_agent_medecin_conseil GROUP BY 1 ORDER BY 2 DESC;`
  — comparez le nombre d'agents distincts retournés à `evaluation/controle_analytics.sql`, section 16b.
  Mesuré le 2026-09-17 sur les données actuelles : 20 agents, tous déjà de type `medecin_conseil` (le
  filtre ne change donc rien au total ici, voir `docs/kpi.md` pour le détail de cette mesure).

### 2.6 — KPI 21 : EP par type de demande

- Jeu de données : `kpi_ententes_prealables_jour`. Type : **Bar Chart**.
- **X-AXIS** : `type_demande_code`. **METRICS** : SUM sur `nombre_ententes`.
- Nom : **Ententes préalables par type de demande**.
- Contrôle : `SELECT type_demande_code, SUM(nombre_ententes) FROM kpi_ententes_prealables_jour GROUP BY 1 ORDER BY 2 DESC;`

### 2.7 — KPI 22 : Montant engagé via EP

- Jeu de données : `kpi_ententes_prealables_jour`. Type : **Big Number**.
- METRIC → **Custom SQL** (n'inclut que les EP acceptées ou validées d'office — les refusées sont à 0 F
  de toute façon, mais le filtre explicite documente l'intention, voir `docs/kpi.md` note sur le KPI 22) :
  ```sql
  SUM(CASE WHEN statut_code IN ('acceptee', 'validee_office') THEN montant_engage_cmu ELSE 0 END)
  ```
- Nom : **Montant engagé via EP (F CFA)**.

### 2.8 — KPI mensuel certifié

- Jeu de données : `v_kpi_ententes_prealables_mois`. Type : **Table**.
- **COLUMNS** : `mois`, `statut_code`, `nombre_ententes`, `delai_moyen_heures`, `montant_engage_cmu` —
  toutes les colonnes de la vue, telles quelles (pas de métrique à recalculer, la vue le fait déjà).
- Onglet **DATA** → **SORT BY** : `mois`, descending.
- Nom : **KPI mensuel certifié EP**.
- Ce tableau restera vide tant qu'aucun mois calendaire n'est entièrement clos dans les données du
  simulateur — normal, voir la limite n°1 de `docs/kpi.md` (profondeur d'historique).

---

## Étape 3 — Assembler le dashboard

1. **Dashboards** → **+ DASHBOARD**. Titre : **DPREST - Ententes préalables**.
2. Disposition suggérée :
   - Ligne 1 (Big Numbers, 4 cases) : Ententes préalables générées, Délai moyen de traitement, Montant
     engagé via EP, et la jauge Taux de réponse aux EP.
   - Ligne 2 (répartitions, 2 cases) : par statut, par type de demande.
   - Ligne 3 : Activité par praticien-conseil (pleine largeur ou grande case).
   - Ligne 4 : KPI mensuel certifié EP (pleine largeur — c'est le tableau réglementaire, à mettre en
     évidence, pas noyé dans le reste).
3. Filtre de période sur `jour` : même procédure qu'à l'étape 4.4 du guide précédent (icône Filters →
   **+ Add filter** → Time range → colonne `jour`). Il ne s'appliquera pas au tableau « KPI mensuel
   certifié EP », qui est basé sur `mois` — c'est attendu, ce tableau a son propre rythme (fermeture
   de mois), pas la même logique de fraîcheur que le reste.
4. **⋮** → **Edit properties** → onglet **Colors** → même **Color Scheme** que le dashboard
   Prestations et facturation (choisissez exactement le même nom dans la liste) — garantit que, par
   exemple, une couleur de statut ne change pas de sens d'un dashboard à l'autre.
5. **SAVE**, puis publier (basculer le badge « brouillon » → « publié »).

---

## Étape 4 — Vérifier

Même principe : comparer chaque chiffre affiché à une requête SQL de contrôle indépendante (données
constatées le 2026-09-11 dans `docs/kpi.md`, section Famille B) :

```powershell
podman exec pipeline_temps_reel-postgres-analytics-1 psql -U dprest -d dprest_analytics -c "SELECT statut_code, SUM(nombre_ententes) FROM kpi_ententes_prealables_jour GROUP BY 1;"
```

Attendu : acceptées 357, refusées 72, validées d'office 38 (chiffres de `docs/kpi.md` — peuvent avoir
changé si le simulateur a tourné depuis la rédaction de ce document ; l'important est que le graphique
et la requête donnent **le même** chiffre entre eux, pas qu'ils correspondent à ces valeurs figées).

---

## Fait

Les deux dashboards DPREST (Prestations et facturation, Ententes préalables) couvrent maintenant les
24 KPI de `docs/kpi.md`, avec un type de graphique choisi pour ce que chaque KPI donne à voir (nombre
unique, taux borné, répartition, classement, série temporelle) plutôt que reproduit à l'identique
pour tous, et une palette de couleurs unique partagée entre les deux.

Point encore ouvert, à traiter à l'étape 7 (sécurité) si ce n'est pas déjà fait : contrôle d'accès par
rôle pour distinguer un compte DPREST des autres comptes Superset (voir `docs/decisions.md`).
