# Guide détaillé — Superset, dashboard « SGD - Qualité des données »

À faire après `docs/guides/etape6b_superset_ententes.md`. Prérequis : `sql/analytics/005_qualite_anomalies.sql`
appliquée sur `dprest_analytics`, et `flink/sql/kpi_prestations.sql` (version du 2026-09-17, sinks
`qualite_anomalies` + détections) redéployé sur le job Flink.

**Différence importante avec les deux dashboards précédents** : celui-ci n'est **pas** destiné à la
DPREST. Il montre des lignes brutes (montants individuels, `personne_uuid`) à des fins de diagnostic
technique, pas des KPI agrégés. **Ne l'ajoutez pas aux permissions du rôle Gamma / compte `dprest`**
(voir `docs/decisions.md`, § 3 « Rôle DPREST en lecture seule ») — il reste visible seulement par
Admin/Alpha (SGD).

---

## Étape 1 — Le jeu de données

**Datasets** → **+ DATASET** → base *PostgreSQL analytique (dprest_analytics)* → schéma `public` →
table `qualite_anomalies` → **ADD**.

Rappel des colonnes (voir `sql/analytics/005_qualite_anomalies.sql` et
`008_qualite_anomalies_famille.sql`) :

| Colonne | Contenu |
|---|---|
| `domaine` | `prestation` \| `facture` \| `agent` \| `assure` \| `entente_prealable` — sur QUELLE TABLE porte l'anomalie |
| `famille` | `MONTANTS` \| `DATES` \| `IDENTITE` \| `QUANTITES` \| `FORMAT` \| `REFERENTIEL` — QUEL ASPECT de la donnée est en cause (ajoutée le 2026-09-17, mêmes familles que la console d'injection du simulateur) |
| `motif_anomalie` | voir tableau ci-dessous |
| `cle_metier` | identifiant de la ligne fautive, pour la retrouver côté source |
| `donnee_brute` | **la ligne source complète, en JSON texte** — c'est la colonne qui répond au besoin « je veux toutes les infos d'une ligne en anomalie » |
| `detecte_le` | horodatage de détection par Flink |

C'est la colonne **`famille`** qu'il faut utiliser pour répondre à « je veux voir les anomalies sur
les dates », « sur les noms », etc. — `domaine` seul ne le permet pas (un même domaine, ex.
`prestation`, mélange des anomalies de nature différente : montant, quantité, référentiel).

### Motifs détectés (`motif_anomalie`)

**Correction du 2026-09-17** : alignés sur `simulateur_V5/anomalies/catalogue.py` (le catalogue exact
des anomalies que le simulateur peut injecter, table `TB_REF_ANOMALIES`) — un code en MAJUSCULES ci-
dessous est un code du catalogue ; un motif en minuscules est un contrôle de complétude général, pas
une anomalie injectée volontairement.

| `motif_anomalie` | Domaine | Famille | Signification |
|---|---|---|---|
| `MONTANT_ABERRANT` | prestation | MONTANTS | Montant négatif ou > 500 000 F (50x le tarif normal) |
| `QUANTITE_NULLE` | prestation | QUANTITES | Quantité servie = 0 alors qu'une quantité était prescrite |
| `QUANTITE_EXCESSIVE` | prestation | QUANTITES | Quantité servie > quantité prescrite |
| `MONTANT_HORS_BAREME` | prestation | MONTANTS | Taux de remboursement hors de [0, 100] |
| `REPARTITION_FAUSSEE` | prestation | MONTANTS | Part CMU + part assuré ≠ montant dépensé |
| `PRESTATION_ORPHELINE` | prestation | REFERENTIEL | Code de prestation absent de la liste des 30 codes valides |
| `DATE_ANTIDATEE` | facture | DATES | Date de soins > 60 jours avant la création du dossier |
| `DATE_SOINS_FUTURE` | facture | DATES | Date de soins postérieure à la création du dossier |
| `TYPE_CENTRE_INCONNU` | facture | REFERENTIEL | Type de centre absent de la liste des 18 codes valides |
| `EMAIL_INVALIDE` | agent | FORMAT | Adresse e-mail sans forme `...@...` |
| `NUMERO_SECU_INVALIDE` | assure | IDENTITE | Numéro de sécu absent du format 13 caractères / préfixe `394` |
| `DATE_NAISSANCE_ABERRANTE` | assure | IDENTITE | Date de naissance future, ou plus que centenaire |
| `CHAMP_OBLIGATOIRE_VIDE` | assure | IDENTITE | `ASSURE_NOM` vide |
| `ENCODAGE_CASSE` | assure | FORMAT | Nom corrompu (mojibake, ou remplacé par `?`) |
| `TENTATIVE_INJECTION` | assure | FORMAT | Charge SQL/XSS/LDAP/template/traversal glissée dans le nom |
| `date_soins_manquante` | facture | DATES | Contrôle de complétude (pas dans le catalogue) |
| `montant_depense_manquant` | prestation | MONTANTS | Contrôle de complétude (pas dans le catalogue) |
| `type_demande_manquant` | entente_prealable | FORMAT | Contrôle ad hoc (pas dans le catalogue) |
| `montant_engage_negatif` | entente_prealable | MONTANTS | Contrôle ad hoc (pas dans le catalogue) |

**4 codes du catalogue restent hors de portée** : `DOUBLON_EXACT` / `DOUBLON_APPROCHANT` (comparent
une fiche assuré aux autres déjà connues — une auto-jointure, pas une simple CASE WHEN ligne à ligne,
reste à écrire), `DATE_HORS_DROITS` (cible `TB_ASSURES_DROITS`, absente de
`connectors/debezium-postgres-json.json`), et `FORMAT_DATE_INCOHERENT` (structurellement réservé à la
campagne du simulateur, jamais au moteur temps réel).

**Ajout du 2026-09-17** : `TB_REF_ASSURES` a rejoint le connecteur Debezium (voir
`connectors/debezium-postgres-json.json` et `flink/sql/kpi_prestations.sql`, table `dim_assures_src`)
pour couvrir les 5 autres codes IDENTITE ci-dessus. Seules les colonnes utiles à la détection
(numéro de sécu, nom, date de naissance) transitent par le pipeline — aucune n'est exposée dans un
KPI DPREST, uniquement dans cette table de quarantaine réservée à l'équipe technique (voir Étape 3).

**Pour tester** : le module d'injection du simulateur est actuellement désactivé
(`TB_CONFIG_ANOMALIES.ENABLED = false`, constaté le 2026-09-17). Il faut l'activer (et fixer un taux
par type dans `TB_REF_ANOMALIES`) via le tableau de bord ou l'API de `simulateur_V5` pour voir des
lignes apparaître ici.

---

## Étape 2 — Les graphiques

### 2.1 — Nombre d'anomalies par famille

- Type : **Bar Chart**. **X-AXIS** : `famille`. **METRICS** : COUNT(*).
- Nom : **Anomalies par famille (dates / montants / identité / ...)**.
- C'est le graphique qui répond à « combien d'anomalies sur les dates, sur les noms, sur les
  montants ? » — ajouté le 2026-09-17 avec la colonne `famille` (voir Étape 1).

### 2.1bis — Nombre d'anomalies par domaine

- Type : **Bar Chart**. **X-AXIS** : `domaine`. **METRICS** : COUNT(*).
- Nom : **Anomalies par domaine**.
- Contrôle : section 19 de `evaluation/controle_analytics.sql`.

### 2.2 — Nombre d'anomalies par motif

- Type : **Bar Chart**, **Bar orientation: horizontal** (les noms de motif sont longs).
- **X-AXIS** : `motif_anomalie`. **METRICS** : COUNT(*). **Sort bars** cochée.
- Nom : **Anomalies par motif**.

### 2.3 — Détail des lignes en anomalie (le graphique qui compte ici)

- Type : **Table**.
- **COLUMNS** : `detecte_le`, `domaine`, `motif_anomalie`, `cle_metier`, `donnee_brute` — toutes les
  colonnes, dans cet ordre (les 4 premières pour trier/filtrer, la dernière pour tout le détail).
- Onglet **DATA** → **SORT BY** : `detecte_le`, descending (les anomalies les plus récentes en haut).
- **Row limit** : 1000 (à ajuster si le volume d'anomalies dépasse ce chiffre en pratique).
- Nom : **Détail des anomalies**.
- `donnee_brute` s'affiche comme du texte JSON brut dans la cellule — Superset ne le met pas en forme,
  mais tout le contenu de la ligne source y est. Cliquer sur la cellule pour l'agrandir si tronquée.

### 2.4 — Filtres natifs

- Icône **Filters** → **+ Add filter** → colonne `famille` (Select, valeurs multiples) — le filtre
  principal : « montre-moi seulement les anomalies sur les dates / les noms / les montants ».
- Un second filtre sur `domaine` permet d'affiner (ex. `famille = DATES` puis `domaine = facture`
  pour ne garder que les dates de soins, pas les dates de naissance).
- Un troisième filtre sur `motif_anomalie` est optionnel, utile une fois que plusieurs motifs
  coexistent au sein d'une même famille.

---

## Étape 3 — Assembler le dashboard

1. **Dashboards** → **+ DASHBOARD**. Titre : **SGD - Qualité des données**.
2. Disposition : ligne 1 (les 3 Bar Charts — famille, domaine, motif — côte à côte), ligne 2 (le
   tableau de détail, pleine largeur — c'est le graphique qui répond au besoin de diagnostic, à
   privilégier en hauteur).
3. **SAVE**, puis publier.
4. **Restriction d'accès** : ⋮ → **Edit properties** → onglet **Accès** → **Propriétaires** : laissez
   uniquement les comptes techniques (SGD/admin). Ne créez **aucune** permission Gamma dessus.

---

## Étape 4 — Vérifier

```powershell
podman exec pipeline_temps_reel-postgres-analytics-1 psql -U dprest -d dprest_analytics -c "SELECT domaine, motif_anomalie, famille, COUNT(*) FROM qualite_anomalies GROUP BY 1, 2, 3 ORDER BY 1, 2;"
```

Comparer avec les graphiques 2.1/2.1bis/2.2. Pour vérifier qu'une ligne précise remonte bien avec tout son
détail : injecter une anomalie connue via le simulateur (ex. un montant négatif), puis retrouver sa
`cle_metier` dans le tableau 2.3 et comparer `donnee_brute` à la ligne source réelle.

**Limite à mentionner en soutenance** : la table `qualite_anomalies` est alimentée par upsert JDBC sur
`(domaine, cle_metier, motif_anomalie)`. Si la ligne source est corrigée après coup, l'anomalie déjà
enregistrée n'est pas retirée automatiquement (les flux d'entrée ne réémettent pas de rétractation pour
une ligne qui a cessé d'être anormale) — cette table trace donc *les anomalies qui sont arrivées*, pas
*les anomalies actuellement non corrigées*. Volontairement laissé ainsi pour ce pilote (voir le
commentaire dans `sql/analytics/005_qualite_anomalies.sql`) ; une vraie mise en production voudrait
sans doute un statut (`ouverte` / `corrigee`) plutôt qu'une suppression silencieuse.
