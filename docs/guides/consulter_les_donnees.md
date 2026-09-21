# Guide — Consulter les données : bases et interfaces web

Trois façons de regarder ce que produit le pipeline, de la plus simple à la plus visuelle.

## Règle à connaître avant tout : utiliser l'IP de la VM, pas `localhost`

Sur ce poste, la machine Podman tourne en mode *rootful* : **`localhost` ne fonctionne pas** depuis
Windows pour joindre les conteneurs. Il faut passer par l'IP interne de la VM :

```powershell
podman machine ssh "ip -4 -o addr show eth0"
```

→ Actuellement `172.31.104.114`. **Cette IP change à chaque `podman machine start`** : la relire si
un accès cesse de fonctionner.

Dans tout ce guide, remplacer `<IP_VM>` par cette adresse.

---

# 1. La base analytique (les KPI)

C'est là que se trouvent les résultats du pipeline.

| Paramètre | Valeur |
|---|---|
| Hôte | `<IP_VM>` (ex. `172.31.104.114`) |
| Port | **15433** |
| Base | `dprest_analytics` |
| Utilisateur | `dprest` |
| Mot de passe | `dprest_dev_2026` |

> Le port est 15433 et non 5433 : ce dernier n'était pas redirigé par Podman sur ce poste, très
> probablement à cause de l'installation PostgreSQL native de Windows qui occupe cette plage.

## 1a. Avec pgAdmin (interface graphique)

pgAdmin 4 est déjà installé avec ton PostgreSQL 17 :
`C:\Program Files\PostgreSQL\17\pgAdmin 4`

1. Lancer pgAdmin depuis le menu Démarrer
2. Clic droit sur **Servers** → **Register** → **Server…**
3. Onglet *General* → **Name** : `DPREST analytique`
4. Onglet *Connection* → renseigner les paramètres du tableau ci-dessus
5. **Save**

Les 6 tables se trouvent ensuite sous
`DPREST analytique → Databases → dprest_analytics → Schemas → public → Tables`.
Clic droit sur une table → **View/Edit Data** → **All Rows** pour voir son contenu.

## 1b. Avec psql (ligne de commande)

`psql` est aussi installé nativement. Depuis PowerShell :

```powershell
$env:PGPASSWORD="dprest_dev_2026"
& "C:\Program Files\PostgreSQL\17\bin\psql.exe" -h 172.31.104.114 -p 15433 -U dprest -d dprest_analytics
```

Une fois dans `psql` : `\dt` liste les tables, `\d kpi_prestations_jour` décrit une table, `\q` quitte.

Ou directement, sans session interactive :

```powershell
& "C:\Program Files\PostgreSQL\17\bin\psql.exe" -h 172.31.104.114 -p 15433 -U dprest -d dprest_analytics -f evaluation/controle_analytics.sql
```

## 1c. Sans rien installer, depuis le conteneur

```powershell
podman exec -it pipeline_temps_reel-postgres-analytics-1 psql -U dprest -d dprest_analytics
```

## Quelques requêtes utiles

```sql
-- Activité par jour
SELECT jour, SUM(nombre_prestations), SUM(montant_depense)
FROM kpi_prestations_jour GROUP BY 1 ORDER BY 1;

-- Vue hebdomadaire (dérivée du grain journalier)
SELECT DATE_TRUNC('week', jour)::date AS semaine,
       SUM(nombre_prestations) AS prestations,
       SUM(montant_depense)    AS montant
FROM kpi_prestations_jour GROUP BY 1 ORDER BY 1;

-- Taux de couverture CMU (KPI 12-14)
SELECT ROUND(100.0 * SUM(montant_pris_en_charge) / SUM(montant_depense), 1) AS taux_couverture_pct
FROM kpi_prestations_jour;

-- Top 10 centres de santé
SELECT centre_sante_code, SUM(nombre_prestations) AS n, SUM(montant_depense) AS montant
FROM kpi_prestations_centre_jour GROUP BY 1 ORDER BY 3 DESC LIMIT 10;

-- Passages (factures) par semaine (KPI 2/4)
SELECT DATE_TRUNC('week', jour)::date AS semaine, SUM(nombre_factures) AS passages
FROM kpi_factures_jour GROUP BY 1 ORDER BY 1;

-- Top 10 des assurés sur tout l'historique (KPI 23) — ex-aequo inclus
SELECT * FROM v_top10_assures ORDER BY rang;

-- Top 10 des assurés d'un centre (KPI 24)
SELECT * FROM v_top10_assures_par_centre WHERE centre_sante_code = 'CS001' ORDER BY rang;
```

### Top 10 des assurés sur une période choisie

Les vues ci-dessus portent sur tout l'historique. Pour une période précise, on refait le classement
sur la plage voulue — c'est pour ça que la base stocke le détail par jour et non un top tout fait :

```sql
WITH totaux AS (
    SELECT personne_uuid, SUM(nombre_prestations) AS nombre_prestations
    FROM kpi_prestations_assure_jour
    WHERE jour BETWEEN '2026-09-08' AND '2026-09-10'
    GROUP BY personne_uuid
),
classement AS (
    SELECT RANK() OVER (ORDER BY nombre_prestations DESC) AS rang, personne_uuid, nombre_prestations
    FROM totaux
)
SELECT rang, personne_uuid, nombre_prestations
FROM classement
WHERE rang <= 10
ORDER BY rang;
```

> Le « top 10 » peut compter **plus de 10 lignes** : tous les ex-aequo sont affichés (hypothèse H7 de
> `docs/kpi.md`). Les assurés n'apparaissent que par leur identifiant opaque `PERSONNE_UUID`.

---

# 2. La base source (les données brutes)

Elle n'a **aucun port publié**, volontairement : c'est le principe de l'architecture — les outils de
restitution n'interrogent jamais la base métier. Pour l'inspecter malgré tout :

```powershell
podman exec -it simulateur_v5-postgres-1 psql -U echo -d echo_db
```

Attention aux noms de tables en majuscules, qui exigent des guillemets doubles :

```sql
SELECT COUNT(*) FROM "TB_FACTURES_PRESTATIONS";
```

Pour éviter les problèmes d'échappement dans PowerShell, préférer le script fourni :

```powershell
podman cp evaluation/controle_source.sql simulateur_v5-postgres-1:/tmp/controle_source.sql
podman exec simulateur_v5-postgres-1 psql -U echo -d echo_db -f /tmp/controle_source.sql
```

---

# 3. Les interfaces web

| Interface | URL | Ce qu'on y voit |
|---|---|---|
| **AKHQ** | `http://<IP_VM>:8085` | Les topics Kafka, le contenu des messages, les schémas Avro, l'état des connecteurs Debezium |
| **Flink** | `http://<IP_VM>:8082` | Les jobs en cours, leur état, les métriques de traitement |
| **simulateur_V5** | `http://<IP_VM>:8000` | Le dashboard du simulateur, pour lancer des simulations |

> AKHQ n'est pas démarré par défaut (il consomme ~400 Mo) :
> `podman compose up -d akhq` pour le lancer, `podman compose stop akhq` pour le libérer.

### Ce qu'on voit dans AKHQ

- **Topics** → `dprest.public.*` (flux Avro) et `dprest-json.public.*` (flux JSON lu par Flink)
- Cliquer sur un topic → **Data** pour lire les messages un par un
- **Connects** → l'état des deux connecteurs Debezium
- **Schema Registry** → les contrats de schéma Avro

### Ce qu'on voit dans l'interface Flink

- **Running Jobs** → le job doit être `RUNNING`. S'il n'apparaît pas, il faut le resoumettre
  (voir `docs/guides/demarrage_arret.md`).
- Cliquer sur le job → le graphe des opérateurs, le nombre d'enregistrements traités, les watermarks.

---

# À venir : Superset (étape 6)

Les tables de la base analytique sont conçues pour être branchées directement à **Superset**, qui
donnera à la DPREST un vrai tableau de bord : sélecteur de période, graphiques, exports. Les requêtes
SQL ci-dessus sont exactement celles que Superset générera — la base est déjà prête à le recevoir.
