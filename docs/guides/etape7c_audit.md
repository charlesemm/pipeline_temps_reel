# Étape 7c — Journal d'audit des accès

## Ce qui est tracé

| Source | Ce qui est journalisé | Comment |
|---|---|---|
| `postgres-analytics` | connexions et déconnexions (utilisateur, base, application, IP), instructions DDL, **instructions refusées** (`permission denied`) | `log_connections`, `log_disconnections`, `log_statement=ddl`, `log_line_prefix` avec `user=` (voir `docker-compose.yml`) |
| Superset | chaque action de l'interface (utilisateur, action, tableau de bord, durée) | table `logs` de `superset-db`, jointe à `ab_user` |

## Intégrité : archivage quotidien avec chaîne de hachage

```powershell
.\scripts\audit-archive.ps1                    # archive la veille (ou -Day 2026-09-21)
.\scripts\audit-verify.ps1                     # code 0 = archives intactes
```

Chaque fichier archivé (`audit/postgres-analytics_<jour>.log`, `audit/superset_<jour>.csv`) est inscrit dans
`audit/manifest.log` avec `chaîne = SHA256(chaîne_précédente + sha256_fichier + nom)`. Modifier un fichier,
supprimer ou réordonner une ligne du manifeste est détecté par `audit-verify.ps1` (testé le 2026-09-21 :
fichier altéré → `ALTERE` ; première ligne supprimée → `CHAINE ROMPUE`). Les fichiers sont ensuite en lecture
seule ; rejouer un jour déjà archivé ne fait rien.

## Limites assumées

- **Les `SELECT` réussis ne sont pas tracés** : `pgaudit` n'existe pas dans l'image `postgres:16-alpine`.
  Sont tracés : connexions, DDL, refus. Passer à une image avec `pgaudit` couvrirait aussi les lectures.
- **Connexions locales en `trust`** : dans le conteneur, la boucle locale (`127.0.0.1`, socket) est configurée en
  `trust` par l'image officielle ; le mot de passe n'y est pas vérifié. Les accès réseau (DBeaver, Superset,
  Grafana, Flink) passent par `scram-sha-256` et sont tracés avec leur identité.
- **Immuabilité relative** : la chaîne de hachage rend l'altération *détectable*, pas *impossible* : un
  administrateur du poste peut recréer tout le manifeste. En production, copier `audit/` vers un stockage
  en écriture seule (WORM) ou un SIEM.
- **Fuseau** : les journaux PostgreSQL sont en UTC ; la fenêtre du jour est en heure locale de la machine.
- **Planification non activée** : lancer `audit-archive.ps1` à la main, ou l'ajouter au Planificateur de tâches
  comme `register-backup-task.ps1` le fait pour la sauvegarde.
- Les archives contiennent des noms d'utilisateurs et des requêtes : `audit/` est exclu de Git.
