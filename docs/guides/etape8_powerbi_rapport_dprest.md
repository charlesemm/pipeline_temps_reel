# Guide détaillé — Power BI, reproduire la grille DPREST (Actes médicaux / Montants / Accès aux prestations)

Ce guide part du principe que la connexion Power BI → `dprest_analytics` fonctionne déjà (voir
l'incident résolu le 2026-09-30 : serveur `127.0.0.1:15433`, syntaxe deux-points). Il explique
comment reproduire la grille historique (capture d'écran du 2026-09-30) dans un visuel Power BI,
ligne par ligne, avec la formule exacte et son degré de confiance.

**Rappel de méthode (`CLAUDE.md`)** : chaque chiffre affiché doit être vérifiable par une requête
SQL indépendante. Ce guide donne systématiquement la requête de contrôle à côté de chaque mesure.

---

## 0. Constat préalable — ce qui est disponible, ce qui ne l'est pas

La grille demandée comporte 15 lignes. Après exploration du schéma réel (source `simulateur_V5` +
base `dprest_analytics`, faite le 2026-09-30, jamais supposée) :

| Ligne | Disponible ? | Pourquoi |
|---|---|---|
| Nombre de consultations | ✅ | `TB_FACTURES.TYPE_FACTURE_CODE` / `prestation_code` |
| Nombre de prescriptions (Pharmacie) | ✅ | idem |
| Nombre d'examen de labo-imagerie | ✅ | idem (regroupe biologie et imagerie, pas de distinction) |
| Nombre de soins dentaires | ✅ | idem |
| Nombre d'hospitalisation | ✅ | idem |
| Montants | ✅ | `kpi_prestations_jour.montant_depense` |
| Nombre d'assurés traités | ✅ | `kpi_prestations_assure_jour`, comptage **distinct** |
| Nombre de localités où les prestations ont eu lieu | ⚠️ | chaîne de jointure centre → collectivité → localité à valider, non couverte ici |
| Nombre de centres de soins fréquentés | ✅ | `kpi_prestations_centre_jour`, comptage **distinct** |
| Nombre de pharmacies | ⚠️ | `TB_REF_PHARMACIES` existe côté simulateur (46 lignes) mais **n'est pas répliquée par CDC** — absente de `dprest_analytics` tant que `connectors/debezium-postgres-json.json` n'est pas modifié |
| Nombre de pharmacies d'intérieur | ❌ | aucune colonne ne distingue une pharmacie « d'intérieur » dans `TB_REF_PHARMACIES` |
| Nombre d'établissements sanitaires dans le réseau CMU | ⚠️ | `dim_centres_sante` (1 510 lignes) — mais aucune colonne n'indique une adhésion au « réseau CMU » : tous les centres du schéma sont traités comme conventionnés par hypothèse, à confirmer |
| Nombre d'agents d'accueil CMU en poste | ✅ | `dim_agents WHERE agent_type_code='accueil'` (1 510 constatés) |
| Nombre d'indigents traités | ❌ | aucun statut « indigent » nulle part dans le schéma (ni régime, ni flag assuré) |
| Taux de service bons % | ❌ | aucune notion de « bons » dans le schéma (existe `TB_REFUS_ACCUEIL`, mais ce n'est probablement pas la même chose — à confirmer avec le métier avant d'improviser une formule) |

Les lignes ❌ sont volontairement laissées vides dans ce guide (décision du 2026-09-30) : mieux vaut
un vide visible et documenté qu'un chiffre inventé. Les lignes ⚠️ sont faisables mais avec une
hypothèse à faire valider — le guide les construit quand même, en le signalant à chaque fois.

---

## 1. Créer la vue SQL pour les lignes additives (Actes médicaux + Montants)

Six des lignes (consultations, prescriptions, labo-imagerie, soins dentaires, hospitalisation,
montants) sont **additives** : leur somme sur une période n'importe quelle période est correcte,
qu'elle soit journalière, mensuelle ou annuelle. Elles peuvent donc être pré-agrégées par jour dans
une vue SQL, que Power BI resommera ensuite librement selon la période affichée.

Le fichier `sql/analytics/012_v_rapport_dprest_actes.sql` (déjà écrit dans le dépôt, pas encore
appliqué à la base) contient cette vue. Pour l'appliquer :

```powershell
podman cp sql/analytics/012_v_rapport_dprest_actes.sql pipeline_temps_reel-postgres-analytics-1:/tmp/012.sql
podman exec pipeline_temps_reel-postgres-analytics-1 psql -U dprest -d dprest_analytics -f /tmp/012.sql
```

**Contrôle** (à faire après création, comparer au résultat de la vue) :
```sql
SELECT SUM(nombre_prestations) FROM kpi_prestations_jour WHERE prestation_code IN ('CONS-GEN','CONS-SPE');
-- doit être identique à : SELECT SUM(valeur) FROM v_rapport_dprest_actes WHERE indicateur = 'Nombre de consultations';
```

**Hypothèse à faire valider avant publication** : « Nombre de consultations » ne prend que les codes
`CONS-GEN`/`CONS-SPE`, pas tout le regroupement `type_facture_code='AMB'` (qui inclut aussi
`SOI-PAN` et `URG-ACC` — vérifié par `SELECT DISTINCT` le 2026-09-30). Si la DPREST veut plutôt tout
l'ambulatoire, remplacer le filtre par `type_facture_code = 'AMB'` dans la vue.

### Dans Power BI

1. **Accueil** → **Obtenir des données** → **Base de données PostgreSQL**.
2. Serveur : `127.0.0.1:15433` (ou l'IP de la VM Podman si `localhost` ne fonctionne pas — voir
   `docs/guides/consulter_les_donnees.md`). Base : `dprest_analytics`.
3. Dans le navigateur, cocher **v_rapport_dprest_actes** → **Charger**.
4. Cette table a 3 colonnes : `jour` (date), `indicateur` (texte), `valeur` (nombre). C'est un format
   « long », pas encore la grille — l'étape 4 ci-dessous construit le visuel matriciel qui la
   transforme visuellement en tableau croisé.

---

## 2. Mesures DAX pour les comptages distincts (assurés, centres fréquentés)

**Piège à éviter** : si on pré-agrège « nombre d'assurés traités » par jour dans une vue SQL puis
qu'on somme ces totaux journaliers dans Power BI, un assuré vu lundi **et** mardi compte deux fois.
Il faut au contraire laisser Power BI recalculer le compte distinct sur la période affichée, à partir
des données au grain le plus fin (une ligne par jour × assuré, ou jour × centre).

1. **Obtenir des données** → même connexion → cocher en plus **kpi_prestations_assure_jour** et
   **kpi_prestations_centre_jour** → **Charger**.
2. Dans le volet **Données**, clic droit sur `kpi_prestations_assure_jour` → **Nouvelle mesure** :
   ```dax
   Assurés traités = DISTINCTCOUNT(kpi_prestations_assure_jour[personne_uuid])
   ```
3. Clic droit sur `kpi_prestations_centre_jour` → **Nouvelle mesure** :
   ```dax
   Centres fréquentés = DISTINCTCOUNT(kpi_prestations_centre_jour[centre_sante_code])
   ```
4. Ces deux mesures se recalculent automatiquement quand vous filtrez par année/mois dans le visuel
   (étape 4) — c'est tout l'intérêt de `DISTINCTCOUNT` par rapport à une somme pré-calculée.

**Contrôle** (sur toute la période disponible) :
```sql
SELECT COUNT(DISTINCT personne_uuid) FROM kpi_prestations_assure_jour;
SELECT COUNT(DISTINCT centre_sante_code) FROM kpi_prestations_centre_jour;
```

---

## 3. Mesures DAX pour les effectifs « réseau » (pas une activité datée)

Trois lignes ne sont pas une activité de la période mais un **effectif constaté** (combien de centres,
combien d'agents d'accueil existent actuellement) — elles doivent afficher la **même valeur sur
chaque colonne de période**, pas une somme.

1. **Obtenir des données** → cocher **dim_centres_sante** et **dim_agents** → **Charger**.
2. Mesure sur `dim_centres_sante` :
   ```dax
   Établissements réseau CMU = COUNTROWS(dim_centres_sante)
   ```
   **Hypothèse à faire valider** : ceci compte *tous* les centres connus du schéma (1 510 constatés
   le 2026-09-30), faute d'une colonne « adhésion au réseau CMU ». Si un jour cette colonne existe
   côté source, remplacer par `CALCULATE(COUNTROWS(dim_centres_sante), dim_centres_sante[reseau_cmu] = TRUE)`.
3. Mesure sur `dim_agents` :
   ```dax
   Agents d'accueil CMU en poste = CALCULATE(COUNTROWS(dim_agents), dim_agents[agent_type_code] = "accueil")
   ```
4. **Nombre de pharmacies** : impossible tant que `TB_REF_PHARMACIES` n'est pas répliquée par CDC.
   Pour l'ajouter plus tard : ajouter `public.TB_REF_PHARMACIES` à `table.include.list` dans
   `connectors/debezium-postgres-json.json`, redéployer le connecteur, créer une dimension
   `dim_pharmacies` côté `sql/analytics/` (même principe que `004_dim_referentiels.sql`), puis une
   mesure `COUNTROWS(dim_pharmacies)` ici. Hors périmètre de ce guide.

**Contrôle** :
```sql
SELECT COUNT(*) FROM dim_centres_sante;
SELECT COUNT(*) FROM dim_agents WHERE agent_type_code = 'accueil';
```

---

## 4. Construire le visuel matriciel (reproduire la grille)

1. Insérer un visuel **Matrice** (icône grille dans le volet Visualisations).
2. **Lignes** : glisser `indicateur` de `v_rapport_dprest_actes`, puis **ajouter manuellement** dans
   le même champ les 4 mesures des étapes 2 et 3 n'existent pas nativement comme valeurs de texte —
   pour les faire apparaître comme des lignes supplémentaires du même tableau, deux options :
   - **Option simple (recommandée pour démarrer)** : faire deux visuels Matrice séparés, un pour
     « Actes médicaux + Montants » (source : `v_rapport_dprest_actes`, lignes = `indicateur`, valeurs
     = `valeur`), un pour « Accès aux prestations » (lignes = mesures glissées une par une : `Assurés
     traités`, `Centres fréquentés`, `Établissements réseau CMU`, `Agents d'accueil CMU en poste`).
     Empiler les deux visuels verticalement sur la page pour reproduire visuellement la grille unique.
   - **Option avancée** : construire une table de mapping « une ligne par indicateur » (via
     **Modélisation** → **Nouvelle table**) qui unpivot les 4 mesures en un format long compatible
     avec `v_rapport_dprest_actes`, pour n'avoir qu'une seule matrice. Plus propre visuellement, plus
     de travail DAX (`UNION`/`SELECTCOLUMNS`) — à faire une fois l'option simple validée.
3. **Colonnes** : glisser `jour` (ou une colonne Année/Mois si vous avez une table de dates dans le
   modèle) — c'est ce qui reproduit les colonnes « 026 » de la grille.
4. **Valeurs** : `valeur` (pour le premier visuel) ou chaque mesure (pour le second).
5. Pour les 3 lignes ❌ (indigents, pharmacies d'intérieur, taux de service bons %) : les ajouter
   comme lignes vides dans la légende du visuel (texte libre, pas une mesure) si vous voulez garder
   la structure visuelle complète de la grille d'origine, en attendant la clarification métier.

---

## 5. Rafraîchissement — ce visuel n'est pas « temps réel » par défaut

**Point important, à connaître avant de présenter ce rapport** : ni Power BI Desktop ni le format de
connexion décrit dans ce guide ne rafraîchissent automatiquement les chiffres pendant que le
simulateur continue de produire des prestations.

### Power BI Desktop (ce guide)
- **Import** (mode utilisé aux étapes 1 à 4, le plus simple) : les données sont copiées dans le
  fichier au moment du chargement. Il faut cliquer **Actualiser** (ruban **Accueil**) à chaque fois
  pour voir les prestations générées depuis le dernier chargement — aucun rafraîchissement automatique.
- **DirectQuery** (option disponible à l'étape « Obtenir des données », à la place d'Import) : chaque
  interaction avec un visuel envoie la requête SQL en direct à `dprest_analytics`, donc toujours à
  jour tant que le fichier reste ouvert. Contrepartie : plus lent si les tables grossissent, et les
  mesures `DISTINCTCOUNT` de la section 2 (assurés, centres fréquentés) sont plus coûteuses en
  DirectQuery qu'en Import. À réserver à une démo live plutôt qu'à un usage courant.

### Power BI Service (si le rapport est un jour publié en ligne)
Un rafraîchissement planifié (ex. toutes les heures) y est possible, **mais** `dprest_analytics`
n'écoute que sur `127.0.0.1:15433` — accessible uniquement depuis ce poste. Le service cloud ne peut
pas l'atteindre sans une **passerelle de données locale** (*on-premises data gateway*, service
Windows gratuit à installer sur ce poste, qui relaie les requêtes du cloud vers la base locale). Sans
cette passerelle, une publication en ligne ne donne qu'un instantané figé au moment de la publication,
pas un rafraîchissement planifié.

**Recommandation pour ce projet** (simulateur local, phase de démonstration/mémoire, pas de
production) : rester en **Import + actualisation manuelle** pour le travail courant — c'est le plus
simple, et suffisant pour vérifier des chiffres contre les requêtes de contrôle de ce guide. Réserver
DirectQuery au moment d'une démonstration live où l'on veut montrer le compteur bouger en direct.

---

## 6. À documenter une fois validé

Si vous validez les hypothèses ci-dessus avec la DPREST (définition de « consultation », périmètre
« réseau CMU », formule éventuelle pour indigents/bons), reportez les décisions dans `docs/kpi.md` —
ce guide ne fait qu'exposer ce qui est techniquement possible aujourd'hui, pas trancher les
définitions métier.
