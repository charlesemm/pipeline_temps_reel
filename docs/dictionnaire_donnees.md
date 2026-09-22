# Dictionnaire de données — base source (simulateur_V5)

Schéma réel de la base PostgreSQL alimentée par `simulateur_V5`, tel que défini dans
`simulateur_V5/schema_initial.sql` (migration Alembic `20260814_0001`, fichier encodé en UTF-16LE à
la source). Ce document ne redéfinit rien : il documente, côté `pipeline_temps_reel`, les tables que
le CDC (étape 2) va capturer et que les KPI (étape 4) vont exploiter.

Convention commune à toutes les tables : `DATE_CREATION` (défaut `now()`, non nul) et
`DATE_MODIFICATION` (nullable, renseigné à la mise à jour) — ce sont ces deux colonnes qui permettent
de suivre l'activité de la base et qui serviront de repère pour les mesures de fraîcheur (chapitre 7).
`UTILISATEUR_ID_CREATION` / `UTILISATEUR_ID_MODIFICATION` tracent l'auteur du changement.

## Tables de référence

Peu ou pas de CDC nécessaire sur ces tables (faible volume, changent rarement) — utiles surtout pour
enrichir les événements de facturation par jointure côté Flink.

| Table | Clé primaire | Rôle |
|---|---|---|
| `TB_REF_ASSURES` | `PERSONNE_UUID` | Identité de l'assuré (nom, numéro sécu, numéro de récépissé). |
| `TB_REF_CENTRES_SANTE` | `CENTRE_SANTE_CODE` | Centres de santé (dénomination, type d'établissement). |
| `TB_REF_AGENTS` | `AGENT_CODE` | Agents CNAM (`accueil`, `medecin_conseil`, `autre`) — répondants des ententes préalables. |
| `TB_REF_PROFESSIONNELS_SANTE` | `PROFESSIONNEL_SANTE_CODE` | Praticiens (nom, type, statut, numéro d'ordre). |
| `TB_REF_SPECIALITES_MEDICALES` | `SPECIALITE_MEDICALE_CODE` | Référentiel des spécialités. |
| `TB_REF_PATHOLOGIES` | `PATHOLOGIE_CODE` + `PATHOLOGIE_DATE_DEBUT` | Référentiel des pathologies (versionné dans le temps). |
| `TB_REF_MEDICAMENTS` | `MEDICAMENT_CODE` + `MEDICAMENT_DATE_DEBUT` | Référentiel médicaments (tarif par défaut, âge min/max, quantité max). |
| `TB_REF_ACTES_MEDICAUX` | `ACTE_MEDICAL_CODE` + `ACTE_MEDICAL_DATE_DEBUT` | Référentiel des actes médicaux. |
| `TB_TV_TYPES_FACTURES` | `TYPE_FACTURE_CODE` + `TYPE_FACTURE_DATE_DEBUT` | Table de valeurs : types de facture. |
| `TB_CENTRES_SANTE_AGENTS`, `TB_REF_PROFESSIONNELS_SANTE_SPECIALITES_MEDICALES`, `TB_PROFESSIONNELS_SANTE_CENTRES_SANTE` | composites | Tables d'association (affectations, périodes de validité). |

## Tables métier — prestations globales (KPI hebdomadaire)

### `TB_FACTURES` — une facture par passage de soins
Clé primaire : `FACTURE_NUMERO`.

| Colonne | Type | Utilité pour les KPI |
|---|---|---|
| `PERSONNE_UUID` | UUID (FK assuré) | Dimension assuré. |
| `TYPE_FACTURE_CODE` | VARCHAR(30) | Type de prestation (consultation, hospitalisation, biologie, radiologie…) — dimension principale du KPI « prestations par type ». |
| `FACTURE_DATE_SOINS` | DATE | Date de réalisation des soins — base du fenêtrage temporel (hebdomadaire/mensuel), potentiellement différente de `DATE_CREATION`. |
| `CENTRE_SANTE_CODE` | VARCHAR(30) (FK) | Dimension centre de santé. |
| `ENTENTE_PREALABLE_ID` | INTEGER (FK nullable) | Rattache la facture à une entente préalable si applicable. |
| `DOSSIER_NUMERO` | VARCHAR(50) | Regroupe plusieurs factures d'un même dossier. |

### `TB_FACTURES_PRESTATIONS` — le détail facturé, ligne par ligne
Clé primaire : `FACTURE_NUMERO` + `PRESTATION_CODE`. **Table la plus dense pour les KPI financiers.**

| Colonne | Type | Utilité |
|---|---|---|
| `PROFESSIONNEL_SANTE_CODE` | VARCHAR(30) (FK) | Praticien ayant réalisé la prestation. |
| `STATUT_REMBOURSEMENT` | VARCHAR(30) | Remboursé / non remboursé — base du **taux de rejet**. |
| `MOTIF_NON_REMBOURSEMENT` | TEXT | Motif texte libre si non remboursé. |
| `STATUT_CODE` | VARCHAR(30) | Statut de la ligne (à confirmer : liste de valeurs à observer en base — hypothèse à valider). |
| `MOTIF_REJET_CODE` | VARCHAR(30) | Code de rejet structuré — probablement la colonne à privilégier pour le **taux de rejet** plutôt que `STATUT_REMBOURSEMENT` (texte libre), à confirmer par un `SELECT DISTINCT` en base. |
| `PRESTATION_MONTANT_DEPENSE` | NUMERIC(15,2) | Montant facturé — base du KPI « montants facturés ». |
| `PRESTATION_MONTANT_RQ` | NUMERIC(15,2) | Montant pris en charge CMU (à confirmer : RQ = « régime qualifiant » ? hypothèse à valider). |
| `PRESTATION_MONTANT_COMPLEMENTAIRE` | NUMERIC(15,2) | Montant pris en charge par un régime complémentaire. |
| `PRESTATION_MONTANT_ASSURE` | NUMERIC(15,2) | Reste à charge de l'assuré. |
| `PRESTATION_QUANTITE_PRESCRITE` / `PRESTATION_QUANTITE_SERVIE` | NUMERIC(10,2) | Écart prescrit/servi — pertinent pour les contrôles qualité (étape 3). |
| `PRESTATION_DATE_DEBUT` / `PRESTATION_DATE_FIN` | DATE | Période de la prestation (peut différer de `FACTURE_DATE_SOINS`). |

### `TB_FACTURES_REJETS` et `TB_FACTURES_STATUTS`
Historisent respectivement les rejets (`REJET_CODE`, période `REJET_DATE_DEBUT`/`FIN`) et les statuts
(`STATUT_CODE`, période) **au niveau de la facture entière** — à distinguer de `MOTIF_REJET_CODE` et
`STATUT_CODE` de `TB_FACTURES_PRESTATIONS`, qui sont au niveau de la ligne. Point à clarifier avec
Mathieu avant de figer la définition du « taux de rejet » : rejet de facture vs rejet de prestation ne
sont pas forcément le même KPI.

### `TB_FACTURES_PATHOLOGIES`, `TB_FACTURES_PRESCRIPTIONS` — détails cliniques (famille C)
Détails cliniques rattachés à une facture. **Intégrés au périmètre CDC le 2026-09-21** (KPI 25, 27, 30 à
33 de `docs/kpi.md`), avec leurs référentiels `TB_REF_MEDICAMENTS`, `TB_REF_PATHOLOGIES` et `TB_REF_DCI`.
**Données de santé sensibles** (loi n°2013-450) : voir les hypothèses H7 à H10 de `docs/kpi.md`.

| Table | Clé primaire | Colonnes utiles | Remarques constatées le 2026-09-21 |
|---|---|---|---|
| `TB_FACTURES_PRESCRIPTIONS` | `FACTURE_NUMERO` + `PRESCRIPTION_CODE` + `DATE_DEBUT` | `PRESCRIPTION_CODE` = code médicament (référentiel `TB_REF_MEDICAMENTS`, sans clé étrangère déclarée mais 100 % de correspondance) ; `PRESCRIPTION_QUANTITE` | Au plus une prescription par facture ; `PRESCRIPTION_QUANTITE` toujours égal à 1 (H8) ; 68 271 lignes (125 325 avant la réduction du volume du 2026-09-21). `DATE_DEBUT` est un `DATE` pur (entier de jours côté Debezium). |
| `TB_FACTURES_PATHOLOGIES` | `FACTURE_NUMERO` + `PATHOLOGIE_CODE` + `PATHOLOGIE_DATE_DEBUT` | `PATHOLOGIE_CODE` (3 caractères) | En moyenne 2 pathologies par facture ; 200 430 lignes, 100 codes (367 003 avant la réduction du volume du 2026-09-21). Clé étrangère vers `TB_REF_PATHOLOGIES` (code + date de début). |
| `TB_REF_MEDICAMENTS` | `MEDICAMENT_CODE` + `MEDICAMENT_DATE_DEBUT` | `MEDICAMENT_DENOMINATION`, `DCI_CODE` | Versionné par date, mais aucun code n'a plusieurs versions (918 / 918) : clé réduite au code côté Flink. |
| `TB_REF_PATHOLOGIES` | `PATHOLOGIE_CODE` + `PATHOLOGIE_DATE_DEBUT` | `PATHOLOGIE_DENOMINATION` | Idem : 100 codes, une seule version chacun. |
| `TB_REF_DCI` | `DCI_CODE` | `DCI_DENOMINATION` | 149 codes. Répliquée, pas encore utilisée par un KPI (disponible pour une ventilation par principe actif). |

`TB_ENTENTES_PREALABLES.FACTURE_NUMERO` relie chaque entente à une facture, donc à ses prescriptions et
pathologies (H7) : c'est le seul lien entre la famille B et la famille C.

**Côté analytique** (`sql/analytics/010_kpi_clinique.sql`) : `dim_medicaments`, `dim_dci`, `dim_pathologies`
(dénominations) ; `kpi_prescriptions_medicament_jour` et `kpi_pathologies_jour` (agrégats journaliers
Flink) ; `fait_prescriptions`, `fait_pathologies`, `fait_entente_facture`, `fait_entente_statut` (copies
1:1, **sans assuré**, réservées au SGD) ; vues masquées `v_kpi_prescriptions_jour`, `v_top10_medicaments`, `v_top10_pathologies`,
`v_kpi_ep_prescriptions`, `v_kpi_ep_medicaments`, `v_kpi_ep_pathologies` (seules lues par la DPREST,
regroupements < 5 supprimés, H10) et vue interne `v_ep_clinique_detail`.

## Tables métier — ententes préalables (KPI mensuel)

### `TB_ENTENTES_PREALABLES` — une demande d'entente préalable
Clé primaire : `ENTENTE_PREALABLE_ID` (SERIAL). Unique : `ENTENTE_PREALABLE_NUMERO`.

| Colonne | Utilité |
|---|---|
| `TYPE_DEMANDE_CODE` | Type de demande (dimension). |
| `TYPE_HOSPITALISATION_CODE` | Sous-type si hospitalisation. |
| `ENTENTE_PREALABLE_DATE_DEBUT` / `_FIN` | Période — `DATE_DEBUT` = date de génération probable pour le KPI « nombre générées/mois », à confirmer. |
| `FACTURE_NUMERO` | Facture liée une fois validée (FK nullable vers `TB_FACTURES`). |
| `CENTRE_SANTE_CODE`, `PERSONNE_UUID` | Dimensions centre de santé / assuré. |

### `TB_ENTENTES_PREALABLES_STATUTS` — historique des statuts, avec l'agent répondant
Clé primaire : `ENTENTE_PREALABLE_ID` + `STATUT_CODE` + `STATUT_DATE_DEBUT`.

**Table clé pour le KPI ententes préalables** : `AGENT_CODE` (FK vers `TB_REF_AGENTS`, filtrable par
`AGENT_TYPE_CODE = 'medecin_conseil'`) donne le **praticien-conseil répondant**. La succession de
lignes par `ENTENTE_PREALABLE_ID` (ordonnée par `STATUT_DATE_DEBUT`) donne l'historique complet :
- **Délai moyen de traitement** = `STATUT_DATE_DEBUT` du premier statut « répondu » (validé/rejeté) −
  `ENTENTE_PREALABLE_DATE_DEBUT` de la demande initiale (hypothèse à valider : quel statut marque le
  début exact de la demande, quel(s) statut(s) marquent une réponse).
- **Taux de réponse** = ententes ayant atteint un statut terminal (validé/rejeté) / total des ententes
  de la période.
- **Ventilation par statut** (validées, rejetées, sans réponse) = agrégation sur le dernier
  `STATUT_CODE` connu par `ENTENTE_PREALABLE_ID`.
- **Valeurs exactes de `STATUT_CODE`** non contraintes par un `CHECK` en base (contrairement à
  `AGENT_TYPE_CODE`) : à lister par un `SELECT DISTINCT STATUT_CODE FROM TB_ENTENTES_PREALABLES_STATUTS;`
  avant d'écrire la définition finale dans `docs/kpi.md`.

### `TB_ENTENTES_PREALABLES_ACTES_MEDICAUX` et `TB_ENTENTES_PREALABLES_PRESTATIONS`
Détail de ce qui est demandé dans l'entente préalable (actes médicaux ou prestations, avec montants
CMU/assuré et statut individuel `ACTE_MEDICAL_STATUT` / `PRESTATION_STATUT`) — utile si un KPI doit
descendre au niveau de la ligne plutôt que de la demande entière.

## Colonnes à valeur non contrainte (à observer avant de figer les KPI)

Contrairement à `TB_REF_AGENTS.AGENT_TYPE_CODE` (contrainte `CHECK` explicite en base :
`'accueil' | 'medecin_conseil' | 'autre'`), les colonnes suivantes n'ont **aucune contrainte `CHECK`**
dans le schéma — leurs valeurs possibles ne sont connues qu'en observant les données réellement
produites par le moteur de simulation (`simulateur_V5/simulation/`) :
- `TB_FACTURES_PRESTATIONS.STATUT_REMBOURSEMENT`, `.STATUT_CODE`, `.MOTIF_REJET_CODE`
- `TB_FACTURES_STATUTS.STATUT_CODE`, `TB_FACTURES_REJETS.REJET_CODE`
- `TB_ENTENTES_PREALABLES_STATUTS.STATUT_CODE`
- `TB_ENTENTES_PREALABLES.TYPE_DEMANDE_CODE`, `.TYPE_HOSPITALISATION_CODE`

**À faire avant l'étape 4** (schéma analytique + KPI) : interroger la base (`SELECT DISTINCT ...`) pour
lister les valeurs réellement produites, plutôt que de les supposer.
