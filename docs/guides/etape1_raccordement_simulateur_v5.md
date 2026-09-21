# Guide — Étape 1 : raccordement à la base source (simulateur_V5)

Objectif : préparer `simulateur_V5` pour le CDC (réplication logique + réseau Podman partagé), sans
rien créer côté source — on lit une base qui existe déjà.

Deux dépôts distincts vont être impliqués :
- `C:\Users\charles.nguessan\OneDrive - IPSCNAM\Documents\simulateur_V5` (à modifier légèrement)
- `C:\Users\charles.nguessan\Documents\pipeline_temps_reel` (ce dépôt)

---

## 1. Créer le réseau Podman partagé

Un réseau nommé que les deux projets peuvent rejoindre, créé une fois pour toutes :

```powershell
podman network create simulateur_v5_net
```

Vérifier :
```powershell
podman network ls
```
→ `simulateur_v5_net` doit apparaître dans la liste.

---

## 2. Modifier `simulateur_V5/compose.yaml`

Ouvrir `simulateur_V5/compose.yaml`. Deux changements sur le service `postgres` :

### a) Activer la réplication logique

Ajouter une clé `command` au service `postgres` :

```yaml
  postgres:
    image: docker.io/library/postgres:16-alpine
    restart: unless-stopped
    command: ["postgres", "-c", "wal_level=logical"]
    environment:
      POSTGRES_DB: echo_db
      POSTGRES_USER: echo
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-echo_dev_2026}
    ...
```

(Insérer la ligne `command:` juste après `restart:`, le reste du service ne change pas.)

### b) Rejoindre le réseau partagé

En bas du fichier, section `networks:` (à créer si elle n'existe pas encore) :

```yaml
networks:
  default:
    name: simulateur_v5_net
```

Ça fait passer le réseau interne du projet (jusqu'ici anonyme) sur le nom `simulateur_v5_net` — le
même que celui créé à l'étape 1. Pas besoin de toucher aux autres services (`api`, `migrations`) : ils
héritent automatiquement du réseau `default`.

> Si un réseau `simulateur_v5_net` existe déjà (créé à l'étape 1 avec `podman network create`), Podman
> Compose le réutilise au lieu d'en recréer un — c'est le comportement voulu.

---

## 3. Redémarrer la base avec la nouvelle config

Depuis `simulateur_V5/` :

```powershell
podman compose down
podman compose up -d
podman compose ps
```

Le `down`/`up` est nécessaire : `wal_level` ne peut pas être changé à chaud, PostgreSQL doit
redémarrer avec le nouveau paramètre.

⚠️ **Sauvegarde avant de continuer si la base contient déjà des données que tu ne veux pas perdre** —
en principe `podman compose down` (sans `-v`) ne touche pas au volume `pgdata`, mais vérifie qu'aucune
option `-v`/`--volumes` ne traîne dans tes habitudes de commande.

---

## 4. Vérifier `wal_level`

```powershell
podman exec -it simulateur_v5-postgres-1 psql -U echo -d echo_db -c "SHOW wal_level;"
```

→ Doit retourner `logical`.

(Si le nom du conteneur diffère, vérifie avec `podman ps` — il suit le motif
`<nom_projet>-<service>-<n>`.)

---

## 5. Rattacher `pipeline_temps_reel` au réseau

Rien à faire ici : le `docker-compose.yml` de ce dépôt déclare déjà
```yaml
networks:
  simulateur_v5_net:
    external: true
```
Il rejoindra `simulateur_v5_net` dès qu'un service l'utilisera explicitement (à partir de l'étape 2,
pour Kafka Connect/Debezium). Pour l'instant, vérifie juste que le réseau existe bien (étape 1) avant
de passer à la suite.

---

## 6. Test de connectivité (facultatif mais recommandé)

Pour confirmer que le réseau fonctionne, avant même d'installer Debezium :

```powershell
podman run --rm --network simulateur_v5_net docker.io/library/postgres:16-alpine `
  psql "postgresql://echo:echo_dev_2026@postgres:5432/echo_db" -c "SELECT count(*) FROM \"TB_FACTURES\";"
```
(Adapter le mot de passe si tu l'as changé dans le `.env` de `simulateur_V5`.)

→ Doit retourner un nombre (0 ou plus), sans erreur de connexion. Ça prouve qu'un conteneur externe
au projet `simulateur_V5` peut atteindre `postgres` par son nom de service, ce qui est exactement ce
que fera Debezium à l'étape 2.

---

## 7. Faire tourner une simulation pour avoir des données

Si `TB_FACTURES` est vide, lance une simulation courte depuis `simulateur_V5/` (voir son
[README.md](../../../simulateur_V5/README.md) pour le détail complet — API, dashboard, ou ligne de
commande) :

```powershell
.\.venv\Scripts\Activate.ps1
python run_simulation.py --nombre-passages 50 --vitesse 60
```

Puis relance la requête de l'étape 6 pour confirmer que le compte a augmenté.

---

## Critère de validation de l'étape 1

- [ ] `podman network ls` liste `simulateur_v5_net`.
- [ ] `SHOW wal_level;` retourne `logical` sur le PostgreSQL de `simulateur_V5`.
- [ ] Un conteneur externe (test de l'étape 6) peut lire `TB_FACTURES` via le nom de service
      `postgres` sur `simulateur_v5_net`.
- [ ] Le `COUNT(*)` de `TB_FACTURES` augmente après une simulation.

Une fois ces quatre points validés, dis-le-moi (ou continue directement) : je passerai
`docs/PLAN.md` à jour et je préparerai le guide de l'étape 2 (Debezium/Kafka).

---

## Ce que je peux préparer pendant que tu fais ça

Le `docs/dictionnaire_donnees.md` (schéma des tables pertinentes, retranscrit depuis
`simulateur_V5/schema_initial.sql`) ne demande aucune manipulation de ta part — dis-moi si tu veux que
je le rédige maintenant en parallèle, ou que j'attende que l'étape 1 soit validée.
