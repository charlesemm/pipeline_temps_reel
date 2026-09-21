# Guide — Étape 2 : Kafka, Schema Registry, CDC Debezium

Tous les fichiers de config sont déjà écrits dans le dépôt : `docker-compose.yml` (services `kafka`,
`schema-registry`, `kafka-connect`), `kafka-connect/Containerfile`, `connectors/debezium-postgres.json`.
Ce guide, c'est juste la suite de commandes pour construire, démarrer, enregistrer le connecteur et
vérifier que ça capture bien les changements de `simulateur_V5`.

Prérequis : étape 1 terminée (réseau `simulateur_v5_net` créé, `wal_level=logical` actif côté
`simulateur_V5`, `simulateur_V5` démarré).

---

## 1. Construire et démarrer la stack

Depuis `pipeline_temps_reel/` :

```powershell
podman compose build kafka-connect
podman compose up -d
podman compose ps
```

Le premier `build` télécharge `cp-kafka-connect-base` (image assez lourde, ~1 Go) puis installe deux
plugins avec `confluent-hub` — patiente, ça peut prendre plusieurs minutes la première fois.

Attends que les trois services soient `healthy` (`podman compose ps`) avant de continuer — `kafka` en
premier, puis `schema-registry`, puis `kafka-connect` (dépendances explicites dans le compose).
`kafka-connect` peut prendre 30-60s de plus après son démarrage (`start_period` du healthcheck).

Si un service reste en échec, regarde ses logs :
```powershell
podman compose logs kafka-connect --tail 100
```

---

## 2. Enregistrer le connecteur Debezium

Une fois `kafka-connect` `healthy` :

```powershell
podman exec -i pipeline_temps_reel-kafka-connect-1 curl -s -X POST -H "Content-Type: application/json" --data-binary "@/dev/stdin" http://localhost:8083/connectors < connectors/debezium-postgres.json
```

Si cette forme pose souci avec l'échappement PowerShell (comme pour psql à l'étape 1), passe plutôt
par le fichier monté ou copie-le dans le conteneur :

```powershell
podman cp connectors/debezium-postgres.json pipeline_temps_reel-kafka-connect-1:/tmp/debezium-postgres.json
podman exec pipeline_temps_reel-kafka-connect-1 curl -s -X POST -H "Content-Type: application/json" -d "@/tmp/debezium-postgres.json" http://localhost:8083/connectors
```

Vérifier l'enregistrement :
```powershell
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/dprest-postgres-source/status
```
→ `"connector": {"state": "RUNNING"}` et chaque tâche (`tasks`) aussi `RUNNING`.

Si le statut est `FAILED`, la cause est presque toujours dans `trace` (visible dans la réponse JSON) —
copie-la-moi si tu bloques, je diagnostique.

---

## 3. Vérifier que les topics Kafka se remplissent

Lister les topics créés par Debezium (préfixe `dprest.` selon `topic.prefix` du connecteur) :

```powershell
podman exec pipeline_temps_reel-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

Tu dois voir des topics comme `dprest.public.TB_FACTURES`, `dprest.public.TB_FACTURES_PRESTATIONS`,
etc., plus les topics internes (`_connect-configs`, `_connect-offsets`, `_connect-status`).

Consommer quelques messages du topic des factures (le connecteur fait un snapshot initial des données
déjà présentes — tu dois voir des messages tout de suite, pas besoin d'attendre une nouvelle
simulation) :

```powershell
podman exec pipeline_temps_reel-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dprest.public.TB_FACTURES --from-beginning --max-messages 3
```

→ Doit afficher 3 messages (contenu binaire Avro illisible tel quel dans la console, c'est normal —
l'important est qu'il y ait des messages, pas de les lire ici).

---

## 4. Vérifier le contrat de schéma dans Schema Registry

```powershell
curl http://localhost:8081/subjects
```
→ Doit lister des sujets comme `dprest.public.TB_FACTURES-value`.

```powershell
curl http://localhost:8081/subjects/dprest.public.TB_FACTURES-value/versions/latest
```
→ Retourne le schéma Avro enregistré (champs de la table, types).

---

## 5. Test de bout en bout : une nouvelle simulation doit apparaître dans Kafka

1. Lance une petite simulation côté `simulateur_V5` (voir son README, ou
   `python run_simulation.py --nombre-passages 5 --vitesse 60`).
2. Reconsomme le topic sans `--from-beginning`, en attendant les nouveaux messages :
   ```powershell
   podman exec pipeline_temps_reel-kafka-1 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dprest.public.TB_FACTURES --max-messages 1
   ```
   (la commande attend qu'un nouveau message arrive puis s'arrête — patiente le temps de la
   simulation, quelques secondes à quelques minutes selon `--vitesse`.)

---

## Critère de validation de l'étape 2

- [ ] `podman compose ps` : `kafka`, `schema-registry`, `kafka-connect` tous `healthy`.
- [ ] Connecteur `dprest-postgres-source` en état `RUNNING` (connecteur + toutes ses tâches).
- [ ] Topics `dprest.public.*` créés et contiennent des messages (snapshot initial).
- [ ] Schema Registry contient les schémas Avro correspondants.
- [ ] Une simulation lancée côté `simulateur_V5` fait apparaître un nouveau message dans Kafka.

Dis-moi où tu en es (ou colle-moi une erreur si ça bloque) — je mets à jour `docs/PLAN.md` et je
prépare le guide de l'étape 3 (Flink) une fois ces points validés.
