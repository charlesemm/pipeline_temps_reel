# Définitions des KPI — périmètre DPREST

Document de référence : chaque KPI est défini par sa **formule**, son **périmètre**, sa
**granularité** et sa **source**, conformément aux règles d'analyse de `CLAUDE.md`.

Toutes les valeurs « constatées » de ce document ont été mesurées sur la base du simulateur le
2026-09-10 (61 391 factures, 61 381 prestations, 12 340 ententes préalables, sur 4 jours de données —
du 7 au 10 septembre 2026). Elles servent à prouver que chaque KPI est réellement alimenté, pas à
constituer un résultat métier.

**Périmètre de ce document (révisé le 2026-09-17)** : seuls les KPI effectivement exposés par un
chart dans un dashboard Superset (`DASHBOARD PRESTATIONS`, `DASHBOARD ENTENTE PREALBLES`) sont
documentés ici. Les KPI 5, 6, 8, 14, 23 et 24 ont été retirés du présent document : leur table ou vue
cible peut encore exister côté pipeline, mais aucun chart Superset ne les affiche actuellement.

## Principes de calcul retenus

**Granularité de base : le jour.** Flink maintient un agrégat **continu** au grain journalier : la
ligne du jour en cours est réécrite (UPSERT) à chaque nouvelle prestation reçue, avec une latence de
l'ordre de la seconde. Les lignes des jours passés ne bougent plus.

**Toutes les autres périodes en découlent par simple regroupement SQL** (`DATE_TRUNC('week', jour)`,
`DATE_TRUNC('month', jour)`, ou une plage libre `WHERE jour BETWEEN ... AND ...`). Conséquence : la
semaine et le mois en cours sont eux aussi **temps réel**, et n'importe quelle période est
consultable instantanément dans Superset sans relire Kafka ni recalculer quoi que ce soit.

**Exception : les KPI réglementaires mensuels des ententes préalables** sont produits *en plus* par
une fenêtre Flink fermée, qui ne publie qu'à la clôture du mois. Le chiffre est alors définitif et
complet — c'est ce qui répond à l'exigence « avant le 5 du mois » du périmètre DPREST.

**Arbitrage sur le grain** : le jour est retenu comme grain le plus fin. Plus fin (l'heure) rendrait
le tableau de bord plus vivant mais multiplierait le volume stocké par 24 sans bénéfice métier pour
la DPREST, dont les décisions se prennent au jour ou à la semaine.

## Hypothèses assumées (à signaler en soutenance)

| # | Hypothèse | Justification |
|---|---|---|
| H1 | **Aucun rejet technique de facture ni de prestation.** Les KPI de type « taux de rejet » sont hors périmètre. | Décision validée avec le SGD. Confirmé par les données : `STATUT_REMBOURSEMENT` = `couvert` et `STATUT_CODE` = `servie` sur 100 % des lignes, `MOTIF_REJET_CODE` toujours nul, `TB_FACTURES_REJETS` vide. |
| H2 | La **date de référence** des KPI d'activité est `FACTURE_DATE_SOINS` (date de réalisation des soins), pas la date de saisie. | C'est la date métier : « combien de soins ont été réalisés », et non « combien de lignes ont été saisies ». |
| H3 | Le **délai de traitement d'une entente préalable** se mesure en heures pleines. | Révisé le 2026-09-17 : les colonnes source portent en réalité un horodatage complet (`TIMESTAMP`, pas `DATE`), mais le simulateur traite chaque EP en quelques minutes/heures — mesurer en jours pleins donnait 0 pour la quasi-totalité des cas (13 339 sur 13 341 constatées), pas assez discriminant pour un pipeline temps réel. L'heure reste une approximation (pas la seconde) : suffisant pour distinguer un traitement instantané d'un traitement qui traîne, sans viser une précision inutile. |
| H4 | Une entente préalable est **« sans réponse »** si aucune ligne n'existe pour elle dans `TB_ENTENTES_PREALABLES_STATUTS`. | Aucun statut explicite « en attente » n'existe dans les données. |
| H5 | Les statuts `acceptee`, `refusee` et `validee_office` sont tous **terminaux** (une EP qui en porte un est considérée traitée). | Aucune EP ne porte plusieurs statuts successifs dans les données observées. |
| H6 | **Un passage = une facture.** Un assuré qui vient plusieurs fois est compté à chaque venue (décision de Mathieu, 2026-09-11). | Le schéma ne porte pas de notion de « venue » distincte de la facture. Conséquence : KPI 2 et KPI 4 sont fusionnés. |

---

# Famille A — Prestations et facturation

**Source principale** : `TB_FACTURES_PRESTATIONS` jointe à `TB_FACTURES` (clé `FACTURE_NUMERO`).
**Granularité de stockage** : jour × type de prestation × type de facture × régime × type de centre.
**Table cible** : `kpi_prestations_jour`.

## Bloc 1 — Compteurs d'ensemble

| # | KPI | Formule | Valeur constatée |
|---|---|---|---|
| 1 | Nombre de prestations | `COUNT(*)` sur `TB_FACTURES_PRESTATIONS` | 61 381 |
| 2/4 | Nombre de passages (factures) | `COUNT(*)` sur `TB_FACTURES` — un assuré revenu est compté à chaque venue (H6). Table `kpi_factures_jour` | 61 391 |
| 3 | Montant total facturé | `SUM(PRESTATION_MONTANT_DEPENSE)` | 614 070 000 F |

> **KPI 2 et 4 fusionnés (2026-09-11)** : l'ancien KPI 4 comptait les assurés *distincts*, décompte
> non additif (un assuré venu lundi et mardi compte 1 sur la semaine, pas 2). Mathieu a retenu la
> définition « un assuré qui revient est compté à chaque venue » : le KPI 4 devient alors le nombre de
> passages, c'est-à-dire le KPI 2 (H6). Un seul indicateur est conservé.
>
> **Particularité du simulateur** : chaque facture contient exactement une prestation (constaté sur
> 63 574 factures). Les KPI 1 et 2/4 donnent donc le même chiffre ici — à la dizaine de factures
> près dont la prestation n'est pas encore écrite. Dans MIRKA, une facture peut regrouper plusieurs
> actes : les deux indicateurs divergeraient, d'où l'intérêt de les garder distincts.

## Bloc 2 — Répartition de l'activité

| # | KPI | Formule | Modalités constatées |
|---|---|---|---|
| 7 | Prestations par type de facture | `COUNT(*) GROUP BY TYPE_FACTURE_CODE` | AMB 43 371 (71 %) · DEN 18 032 (29 %) |
| 9 | Prestations par type de centre | `COUNT(*) GROUP BY CENTRE_SANTE_TYPE_LIBELLE` | 6 catégories, ~10 200 chacune |
| 10 | Top centres de santé | `COUNT(*)` et `SUM(montant) GROUP BY CENTRE_SANTE_CODE`, classés | 30 centres |
| 11 | Top praticiens | `COUNT(*)` et `SUM(montant) GROUP BY PROFESSIONNEL_SANTE_CODE`, classés | 150 praticiens |

## Bloc 3 — Prise en charge financière

| # | KPI | Formule | Valeur constatée |
|---|---|---|---|
| 12 | Montant pris en charge CMU | `SUM(PRESTATION_MONTANT_RQ)` | 504 249 000 F (82 %) |
| 13 | Reste à charge assuré | `SUM(PRESTATION_MONTANT_ASSURE)` | 109 821 000 F (18 %) |
| 15 | Montant moyen par prestation | `SUM(MONTANT_DEPENSE) / COUNT(*)` | 10 000 F |

> Les KPI 12 et 13 expriment la mission même de la CMU en un chiffre — à mettre en avant sur le
> tableau de bord DPREST.

---

# Famille B — Ententes préalables

**Source principale** : `TB_ENTENTES_PREALABLES` jointe à `TB_ENTENTES_PREALABLES_STATUTS`
(clé `ENTENTE_PREALABLE_ID`) et `TB_ENTENTES_PREALABLES_ACTES_MEDICAUX`.
**Granularité de stockage** : jour × statut × type de demande.
**Tables cibles** : `kpi_ententes_prealables_jour` et `kpi_ententes_prealables_agent_jour` (continu,
alimentées par Flink), `v_kpi_ententes_prealables_mois` (vue SQL certifiée, mois clos uniquement —
voir `docs/decisions.md` pour le choix vue plutôt que fenêtre Flink).

**Statut (2026-09-11)** : famille alimentée et vérifiée contre la source (357 EP acceptées, 72
refusées, 38 validées d'office ; montant engagé 6 818 035 F / 0 F / 684 900 F ; 20 agents actifs,
429 lignes ; 388 demandes sur acte, 79 sur hospitalisation — chiffres identiques source/pipeline).

| # | KPI | Formule | Valeur constatée |
|---|---|---|---|
| 16 | Nombre d'EP générées | `COUNT(*)` sur `TB_ENTENTES_PREALABLES`, daté par `ENTENTE_PREALABLE_DATE_DEBUT` | 12 340 |
| 17 | Taux de réponse | `EP ayant au moins un statut / EP totales × 100` (voir H4) | 99,98 % (3 sans réponse) |
| 18 | Ventilation par statut | `COUNT(*) GROUP BY STATUT_CODE` | Acceptées 9 804 (79,5 %) · Refusées 1 678 (13,6 %) · Validées d'office 855 (6,9 %) |
| 19 | Délai moyen de traitement | `AVG(STATUT_DATE_DEBUT − ENTENTE_PREALABLE_DATE_DEBUT)` en heures (voir H3, révisée le 2026-09-17 — anciennement en jours) | à mesurer après redéploiement (était 0,00 jour avant la révision) |
| 20 | Activité par praticien-conseil | `COUNT(*) GROUP BY AGENT_CODE`, restreint aux agents de type `medecin_conseil` | 20 agents répondants (mesure du 2026-09-17, voir note ci-dessous) |
| 21 | EP par type de demande | `COUNT(*) GROUP BY TYPE_DEMANDE_CODE` | Acte 9 923 (80 %) · Hospitalisation 2 148 (20 %) |
| 22 | Montant engagé via EP | `SUM(ACTE_MEDICAL_MONTANT_CMU)` sur `TB_ENTENTES_PREALABLES_ACTES_MEDICAUX` | 17 511 F en moyenne par acte accepté ; 0 F pour les actes refusés |

> **KPI 20, particularité constatée** : les 855 EP `validee_office` ne portent aucun praticien-conseil
> identifiable (validation automatique, sans intervention humaine). Elles doivent être exclues du
> décompte par praticien mais comptées dans le total des EP traitées — sinon les pourcentages ne
> tombent pas juste.
>
> **KPI 22** : les actes refusés portent un montant CMU de 0 F, ce qui est cohérent (aucun engagement
> financier sur un refus). Le montant engagé ne doit donc porter que sur les actes acceptés et validés
> d'office.
>
> **KPI 20, correction du 2026-09-17** : le « 10 agents répondants » ci-dessus datait du 2026-09-11 et
> était en réalité inatteignable tel quel — `TB_REF_AGENTS` (qui porte `AGENT_TYPE_CODE`) n'était pas
> répliquée par CDC, le filtre `medecin_conseil` n'a donc jamais pu être appliqué avant cette date. Une
> fois la réplication ajoutée (`sql/analytics/006_dim_agents.sql`) et le filtre appliqué via la vue
> `v_kpi_ep_agent_medecin_conseil`, mesure faite sur les données actuelles : **20 agents**, identique
> au décompte non filtré — sur ce jeu de données, tous les agents ayant répondu à une EP sont déjà de
> type `medecin_conseil` (aucun agent `accueil` ne répond jamais). Le filtre reste appliqué par
> prudence (défensif si le générateur évolue), mais ne change rien à la valeur actuelle. Voir
> `docs/decisions.md`, entrée du 2026-09-17.

---

# Tables cibles (schéma analytique)

| Table | Alimentation | Grain | Comportement |
|---|---|---|---|
| `kpi_prestations_jour` | Flink, agrégat continu | jour × type prestation × type facture × régime × type centre | Temps réel : la ligne du jour est réécrite à chaque prestation |
| `kpi_prestations_centre_jour` | Flink, agrégat continu | jour × centre | Temps réel (KPI 10) |
| `kpi_prestations_praticien_jour` | Flink, agrégat continu | jour × praticien | Temps réel (KPI 11) |
| `kpi_factures_jour` | Flink, agrégat continu | jour × type facture × régime × type centre | Temps réel (KPI 2/4) |
| `kpi_ententes_prealables_jour` | Flink, agrégat continu | jour × statut × type de demande | Temps réel |
| `kpi_ententes_prealables_agent_jour` | Flink, agrégat continu | jour × agent × statut | Temps réel (KPI 20), hors `validee_office` |
| `v_kpi_ententes_prealables_mois` | Vue SQL, dérivée du grain jour | mois × statut | **Changé le 2026-09-11** : vue plutôt que fenêtre Flink (voir `docs/decisions.md`) ; ne porte que sur les mois entièrement clos |

**Idempotence** : écriture en `UPSERT` sur la clé primaire (grain + dimensions). Relancer un job,
rejouer le topic depuis le début ou redémarrer le pipeline ne duplique ni ne fausse aucune ligne —
exigence non négociable de `CLAUDE.md`.

**Contrôle de justesse** : chaque KPI doit être vérifiable par une requête SQL directe sur la base
source pour une période donnée, et donner exactement le même résultat. La méthode est décrite dans
`docs/guides/etape3_flink.md`, section 6 ; elle a déjà permis de valider le premier agrégat au
centime près (56 481 prestations / 564 810 000 F).

---

# Limites connues, à mentionner dans le mémoire

1. **Profondeur d'historique** : 4 jours de données au moment de la rédaction. Les KPI hebdomadaires
   et mensuels sont structurellement corrects mais peu démonstratifs tant que le simulateur n'a pas
   tourné plus longtemps.
2. **Absence de rejets** (H1) : le simulateur ne produit aucun rejet, ce qui prive le tableau de bord
   d'un indicateur de contrôle attendu en production réelle.
3. **Granularité temporelle des ententes préalables** (H3, révisée le 2026-09-17) : délai de
   traitement mesurable à l'heure près (pas la seconde) — suffisant pour ce pipeline, mais un
   traitement de quelques secondes reste indiscernable d'un traitement de 59 minutes.
4. **Montants uniformes** : toutes les prestations sont facturées 10 000 F. Les KPI de dispersion
   (montant médian, écart-type, valeurs aberrantes) n'auraient aucun sens sur ces données.
5. **Régime complémentaire non simulé** : colonne `PRESTATION_MONTANT_COMPLEMENTAIRE` toujours nulle.
