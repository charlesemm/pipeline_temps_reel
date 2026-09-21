# Guide — Étape 3 : Flink (agrégations, fenêtres, contrôle qualité)

Fichiers déjà écrits : `docker-compose.yml` (services `flink-jobmanager`, `flink-taskmanager`),
`flink/Containerfile`, `flink/sql/pipeline_kpi_hebdo.sql`.

Rappel important (voir `docs/decisions.md`) : la formule de KPI dans ce premier job est une
**hypothèse provisoire**, juste là pour prouver que le mécanisme fonctionne. La vraie définition sera
figée à l'étape 4, avec toi.

Prérequis : étape 2 terminée et stack démarrée (`docs/guides/demarrage_arret.md`).

---

## 1. Construire et démarrer Flink

```powershell
podman compose build flink-jobmanager flink-taskmanager
podman compose up -d flink-jobmanager flink-taskmanager
podman compose ps
```

Le téléchargement des deux connecteurs (~27 Mo au total) se fait pendant le `build`, une seule fois.

Interface web Flink : **http://localhost:8082** (port 8082 côté hôte, pas 8081 — déjà pris par Schema
Registry). Si `localhost` ne répond pas, même souci que d'habitude, voir
`docs/guides/demarrage_arret.md` (IP interne de la VM Podman).

---

## 2. Soumettre le job SQL

Le script est déjà dans le dépôt ; il faut juste le copier dans le conteneur et l'exécuter avec le
client SQL de Flink (depuis PowerShell — `MSYS_NO_PATHCONV` n'est utile que dans Git Bash, pas ici) :

```powershell
podman cp flink/sql/pipeline_kpi_hebdo.sql pipeline_temps_reel-flink-jobmanager-1:/tmp/pipeline_kpi_hebdo.sql
podman exec pipeline_temps_reel-flink-jobmanager-1 ./bin/sql-client.sh -f /tmp/pipeline_kpi_hebdo.sql
```

Si tu es en Git Bash plutôt que PowerShell, préfixe les deux commandes avec `MSYS_NO_PATHCONV=1`
(sinon `/tmp/...` sera traduit en chemin Windows, comme rencontré à l'étape 2).

Le client SQL doit se terminer en affichant que le job (le `STATEMENT SET`, donc les deux `INSERT`
d'un coup) a été soumis, avec un Job ID. C'est normal qu'il rende la main : le job continue de
tourner sur le cluster Flink en arrière-plan.

---

## 3. Vérifier que le job tourne

Dans l'interface web (**http://localhost:8082**), onglet **Running Jobs** : le job doit apparaître
avec un statut `RUNNING`, et rester `RUNNING` (pas de redémarrage en boucle — signe d'une erreur de
config si ça boucle).

En ligne de commande :
```powershell
podman exec pipeline_temps_reel-flink-jobmanager-1 ./bin/flink list
```

---

## 4. Vérifier les résultats dans Kafka

Deux nouveaux topics doivent apparaître (via AKHQ — http://172.31.104.114:8085 ou l'IP courante de la
VM — ou en ligne de commande) :

- `kpi.prestations_hebdo_brut` — l'agrégat hebdomadaire (nombre de prestations + montant total, par
  statut de remboursement, par semaine).
- `qualite.prestations_rejetees` — les lignes isolées par le contrôle qualité (montant manquant ou
  négatif).

En ligne de commande (PowerShell, `MSYS_NO_PATHCONV=1` si Git Bash) :
```powershell
podman exec pipeline_temps_reel-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kpi.prestations_hebdo_brut --from-beginning --max-messages 5
```
→ Ces messages sont en JSON lisible directement (pas Avro), tu dois voir des lignes du type
`{"semaine_debut":"...","statut_remboursement":"...","nombre_prestations":...,"montant_total":...}`.

---

## 5. Test de bout en bout

Lance une simulation côté `simulateur_V5`, attends quelques secondes, puis reconsulte
`kpi.prestations_hebdo_brut` (sans `--from-beginning`, avec `--max-messages 1`) pour voir l'agrégat de
la semaine en cours se mettre à jour (nouveau message avec la même clé semaine+statut — c'est le
comportement upsert voulu, pas un doublon).

---

## 6. LA vérification qui compte : comparer avec la base source

Un pipeline peut être « tout vert » (connecteurs `RUNNING`, job `RUNNING`, messages qui sortent) et
produire quand même un résultat entièrement faux — c'est arrivé ici (voir `docs/decisions.md`, piège
de nommage JSON). Le seul contrôle qui prouve quelque chose, c'est de recalculer le KPI
indépendamment sur la base source et de comparer.

Prendre la fenêtre affichée dans un message du topic KPI (champ `semaine_debut`), puis :

```powershell
podman exec simulateur_v5-postgres-1 psql -U echo -d echo_db -c "SELECT \"STATUT_REMBOURSEMENT\", COUNT(*), SUM(\"PRESTATION_MONTANT_DEPENSE\") FROM \"TB_FACTURES_PRESTATIONS\" WHERE \"DATE_CREATION\" >= TIMESTAMPTZ '2026-09-03 00:00:00+00' AND \"DATE_CREATION\" < TIMESTAMPTZ '2026-09-10 00:00:00+00' GROUP BY 1;"
```

(adapter les deux dates : début de fenêtre, et début + 7 jours)

→ Le nombre et le montant doivent correspondre **exactement** à ceux du message Kafka.

Deuxième contrôle de cohérence utile : le nombre de messages du topic qualité doit être **très
inférieur** au nombre de lignes du topic source. S'ils sont égaux, c'est le signe d'un bug de
lecture (colonne mal nommée, mapping raté), pas d'une base de mauvaise qualité.

---

## Critère de validation de l'étape 3

- [x] `flink-jobmanager` et `flink-taskmanager` `healthy`.
- [x] Job `RUNNING` dans l'UI Flink, pas de redémarrage en boucle.
- [x] Topic `kpi.prestations_hebdo_brut` contient un agrégat.
- [x] **L'agrégat correspond exactement à la requête de contrôle sur la base source**
      (56 481 prestations / 564 810 000 pour la semaine du 2026-09-03).
- [x] Topic `qualite.prestations_rejetees` vide — cohérent : aucune anomalie de montant dans les
      données produites par `simulateur_V5` (vérifié en base, pas supposé).

**Étape 3 validée.** Suite : étape 4 — figer les définitions de KPI dans `docs/kpi.md` et créer le
schéma PostgreSQL analytique. Point ouvert à traiter en priorité : le simulateur ne génère aucun
rejet de prestation (100 % `couvert`/`servie`), donc le KPI « taux de rejet » n'est pas calculable en
l'état.
