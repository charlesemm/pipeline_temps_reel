# Guide — Étape 4 : KPI dans la base analytique PostgreSQL

Ce que fait cette étape : Flink ne se contente plus d'écrire dans Kafka, il alimente une **base
PostgreSQL analytique** que Grafana et Superset pourront interroger — y compris sur une période
choisie librement, ce que Kafka ne permet pas.

Définitions des KPI : `docs/kpi.md`. Schéma des tables : `sql/analytics/001_schema.sql`.

Prérequis : étapes 0 à 3 terminées, stack démarrée (`docs/guides/demarrage_arret.md`).

---

## Ce qui a été mis en place

| Élément | Rôle |
|---|---|
| Service `postgres-analytics` | Base cible, **distincte** de la base source. Port **15433** côté Windows (voir `docs/guides/consulter_les_donnees.md`). |
| `sql/analytics/001_schema.sql` | 6 tables KPI, appliquées automatiquement à la création du volume. |
| Connecteur JDBC + pilote PostgreSQL | Ajoutés à l'image Flink (`flink/Containerfile`). |
| `flink/sql/kpi_prestations.sql` | Le job : lit les prestations **et** les factures, les joint, agrège par jour, écrit en base. |

---

## 1. Démarrer la base analytique

```powershell
podman compose up -d postgres-analytics
podman exec pipeline_temps_reel-postgres-analytics-1 psql -U dprest -d dprest_analytics -c "\dt"
```

→ Doit lister les 6 tables `kpi_*`. Si la liste est vide, c'est que le volume existait déjà avant que
le schéma ne soit écrit : les scripts d'initialisation de PostgreSQL ne s'exécutent qu'à la **création**
du volume. Dans ce cas, appliquer le schéma à la main :

```powershell
podman cp sql/analytics/001_schema.sql pipeline_temps_reel-postgres-analytics-1:/tmp/001.sql
podman exec pipeline_temps_reel-postgres-analytics-1 psql -v ON_ERROR_STOP=1 -U dprest -d dprest_analytics -f /tmp/001.sql
```

### 1b. Appliquer les ajouts de schéma (fichiers `002_` et suivants)

Chaque évolution du schéma est un nouveau fichier numéroté dans `sql/analytics/`. Sur une base déjà
créée, ils ne s'appliquent **pas** tout seuls. Pour le fichier `002_kpi_passages_assures.sql` (passages
et classements d'assurés, ajoutés le 2026-09-11) :

```powershell
podman cp sql/analytics/002_kpi_passages_assures.sql pipeline_temps_reel-postgres-analytics-1:/tmp/002.sql
podman exec pipeline_temps_reel-postgres-analytics-1 psql -v ON_ERROR_STOP=1 -U dprest -d dprest_analytics -f /tmp/002.sql
```

Rejouable sans risque : il ne fait que des `CREATE ... IF NOT EXISTS` et `CREATE OR REPLACE VIEW`.

> Passer par `podman cp` + `-f`, et non par `Get-Content ... | podman exec -i` : PowerShell 5.1
> ré-encode le texte envoyé dans un tube et abîme les accents des commentaires SQL.

**À faire AVANT de soumettre le job** : si le job écrit dans une table qui n'existe pas encore, il
échoue.

## 2. Soumettre le job

```powershell
podman cp flink/sql/kpi_prestations.sql pipeline_temps_reel-flink-jobmanager-1:/tmp/kpi_prestations.sql
podman exec pipeline_temps_reel-flink-jobmanager-1 ./bin/sql-client.sh -f /tmp/kpi_prestations.sql
```

Puis vérifier qu'il **tient dans la durée** (au-delà de 2 minutes) :

```powershell
podman exec pipeline_temps_reel-flink-jobmanager-1 curl -s http://localhost:8081/jobs
```

## 3. Vérifier que les lignes arrivent

```powershell
podman exec pipeline_temps_reel-postgres-analytics-1 psql -U dprest -d dprest_analytics -c "SELECT COUNT(*) FROM kpi_prestations_jour;"
```

Cardinalités attendues sur les données actuelles : environ 96 lignes par jour couvert pour
`kpi_prestations_jour` (4 types d'acte × 2 types de facture × 2 régimes × 6 types de centre),
30 lignes par jour pour les centres, 150 pour les praticiens.

---

## 4. LA vérification qui compte : comparer avec la source

```powershell
# Cible — calculé par Flink
podman exec pipeline_temps_reel-postgres-analytics-1 psql -U dprest -d dprest_analytics -c "SELECT SUM(nombre_prestations), SUM(montant_depense), SUM(montant_pris_en_charge) FROM kpi_prestations_jour;"

# Source — calcul de contrôle indépendant
podman exec simulateur_v5-postgres-1 psql -U echo -d echo_db -c "SELECT COUNT(*), SUM(p.\"PRESTATION_MONTANT_DEPENSE\"), SUM(p.\"PRESTATION_MONTANT_RQ\") FROM \"TB_FACTURES_PRESTATIONS\" p JOIN \"TB_FACTURES\" f ON f.\"FACTURE_NUMERO\"=p.\"FACTURE_NUMERO\";"
```

**Comment interpréter un écart** : si le simulateur tourne pendant la mesure, un petit écart est
normal — c'est la latence du pipeline. Pour le distinguer d'une erreur de calcul, refaire les deux
mesures 30 secondes plus tard : si l'écart **se réduit** pendant que les volumes augmentent, c'est du
décalage temps réel. S'il **reste constant ou grandit**, c'est un bug.

Résultat obtenu lors de la validation : écart passé de 11 à **1 ligne** pendant que le volume
augmentait de 59 lignes → le pipeline suit la source en quasi-temps réel.

---

## 5. Consulter les KPI sur une période

C'est tout l'intérêt de la base analytique — du SQL standard :

```sql
-- Activité sur une plage libre
SELECT prestation_code, SUM(nombre_prestations), SUM(montant_depense)
FROM kpi_prestations_jour
WHERE jour BETWEEN '2026-09-01' AND '2026-09-30'
GROUP BY prestation_code
ORDER BY 2 DESC;

-- Vue hebdomadaire, dérivée du grain journalier
SELECT DATE_TRUNC('week', jour) AS semaine,
       SUM(nombre_prestations)  AS prestations,
       SUM(montant_depense)     AS facture,
       ROUND(100.0 * SUM(montant_pris_en_charge) / NULLIF(SUM(montant_depense), 0), 1) AS taux_couverture
FROM kpi_prestations_jour
GROUP BY 1 ORDER BY 1;

-- Top 10 centres
SELECT centre_sante_code, SUM(nombre_prestations) AS n, SUM(montant_depense) AS montant
FROM kpi_prestations_centre_jour
GROUP BY 1 ORDER BY 3 DESC LIMIT 10;
```

Aucune de ces requêtes ne relit Kafka ni ne relance un calcul : la semaine, le mois et les plages
libres se dérivent du grain journalier. C'est ce que fera Superset à l'étape 6 quand tu déplaceras le
sélecteur de dates.

---

## Pièges rencontrés (et corrigés) — utiles si tu rejoues cette étape

1. **Reconstruire les DEUX images Flink.** Le JobManager compile le job, mais c'est le TaskManager qui
   l'exécute. Ne reconstruire que le JobManager donne un job qui se soumet correctement puis échoue à
   l'exécution (`ClassNotFoundException` sur le connecteur JDBC).
   → `podman compose build flink-jobmanager flink-taskmanager`

2. **Mémoire du TaskManager.** Une jointure entre deux flux CDC garde **les deux côtés en état** ;
   avec 1024 Mo de mémoire totale, il ne restait que 25 Mo de tas applicatif → `OutOfMemoryError`.
   Corrigé en portant la mémoire à 1536 Mo **et** en réduisant la fraction de mémoire « managée »
   (réservée à RocksDB, non utilisé ici) de 40 % à 10 % : 614 Mo de tas applicatif pour seulement
   512 Mo de RAM supplémentaire.

3. **Dates encodées en entier.** Debezium sérialise les colonnes `DATE` en nombre de jours depuis
   1970 (`FACTURE_DATE_SOINS: 20703`). Conversion nécessaire côté Flink — vérifier la formule sur une
   valeur connue avant de l'intégrer au job.

---

## Critère de validation de l'étape 4

- [x] Base `postgres-analytics` démarrée, 6 tables créées.
- [x] Job `kpi-prestations-continu` `RUNNING` de façon stable.
- [x] Les trois tables de la famille « prestations » se remplissent.
- [x] Les totaux correspondent à la source, à la latence temps réel près.
- [x] Dédoublonnage des événements Debezium réémis (2026-09-11, voir `docs/decisions.md`).
- [x] Passages (KPI 2/4) et top 10 des assurés, global et par centre (KPI 23, 24) — identiques à la
      source, empreintes comprises (sections 11 à 13 des scripts de contrôle).
- [ ] Famille « ententes préalables » (KPI 16 à 22) — à implémenter.
- [ ] Test d'idempotence explicite : relancer le job depuis le début ne doit pas dupliquer de ligne.
