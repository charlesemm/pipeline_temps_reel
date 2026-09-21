# Guide — Étape 7 : sécurité, chiffrement, rôles, traçabilité

Ce que fait cette étape : répondre à l'exigence légale de `CLAUDE.md` (loi n°2013-450, contrôle
ARTCI) — chiffrement des échanges, authentification par composant, contrôle d'accès par rôle,
traçabilité. Détail des choix et des pièges rencontrés : `docs/decisions.md`.

Prérequis : étapes 0 à 6 terminées, stack démarrée (`docs/guides/demarrage_arret.md`).

---

## Ce qui a changé pour toi, au quotidien

### Les adresses ont changé

Grafana et Superset sont désormais en **HTTPS**, sur des ports différents :

| Avant | Maintenant |
|---|---|
| `http://<IP_VM>:3000` (Grafana) | `https://<IP_VM>:3443` |
| `http://<IP_VM>:8088` (Superset) | `https://<IP_VM>:8443` |

Le certificat est **auto-signé** : ton navigateur va afficher un avertissement de sécurité
(« Ce site n'est pas sûr », « Continuer quand même »). **C'est normal** — c'est le comportement
attendu d'un certificat auto-signé, pas une erreur du pipeline. Clique sur « Avancé » puis
« Continuer vers le site » (le libellé exact dépend du navigateur).

### Les mots de passe

Tout est dans `.env`, à la racine du projet — fichier non versionné. Si tu ne l'as plus, il faut
régénérer les secrets (voir `docs/decisions.md`, étape 7, section 1) et relancer les services
concernés.

### Un nouveau compte dans Superset

En plus du compte admin, il existe maintenant un compte **`dprest`**, en lecture seule (rôle Gamma) —
c'est celui à donner à la DPREST pour consulter les tableaux de bord, sans pouvoir rien modifier.
Mot de passe dans `.env` (ou à régénérer via `superset fab create-user` si perdu).

---

## Ce qui a été mis en place

| Élément | Rôle |
|---|---|
| `.env` étendu | Secrets pour tous les services, plus seulement Superset |
| `connectors/secrets/db.properties` | Mot de passe du connecteur Debezium, hors du fichier versionné |
| `reverse-proxy/` (nginx) | Terminaison HTTPS pour Grafana et Superset |
| `dprest_lecture` (PostgreSQL analytique) | Compte lecture seule, utilisé par Grafana et Superset |
| `monitoring_ro` (base source) | Compte de supervision minimal, utilisé par Grafana |
| Compte `dprest` (Superset) | Lecture seule, pour un usage DPREST réel |

---

## Vérifier que ça tient

### Les comptes en lecture seule ne peuvent pas écrire

```powershell
$env:PGPASSWORD = "<ANALYTICS_RO_PASSWORD depuis .env>"
& "C:\Program Files\PostgreSQL\17\bin\psql.exe" -h <IP_VM> -p 15433 -U dprest_lecture -d dprest_analytics -c "DELETE FROM kpi_prestations_jour WHERE jour = '2026-01-01';"
```

Attendu : `ERROR: permission denied for table kpi_prestations_jour`.

### Le secret du connecteur n'est jamais exposé

```powershell
podman exec pipeline_temps_reel-kafka-connect-1 curl -s http://localhost:8083/connectors/dprest-postgres-source-json/config
```

Attendu : le champ `database.password` affiche `${file:/run/secrets/db.properties:...}`, jamais la
valeur réelle — même pour un administrateur qui consulte cette configuration.

### La traçabilité fonctionne

```powershell
podman exec pipeline_temps_reel-superset-db-1 psql -U superset -d superset -c "SELECT action, user_id, dttm FROM logs ORDER BY dttm DESC LIMIT 10;"
```

Chaque action (consultation d'un dashboard, requête SQL...) doit apparaître avec l'identifiant de
l'utilisateur qui l'a faite.

---

## Pièges rencontrés (et corrigés) — utiles si tu rejoues cette étape

1. **`docs/` était dans `.gitignore` depuis le début du projet.** Toute la documentation du mémoire
   n'avait jamais été commitée. Découvert et corrigé en cours d'étape — sans rapport avec la
   sécurité, mais trop important pour attendre.

2. **Le reverse proxy tombe en `502 Bad Gateway` après avoir recréé Grafana ou Superset.** nginx
   résout les noms de service une seule fois au démarrage. Corrigé avec un résolveur DNS dynamique —
   mais attention, l'adresse `127.0.0.11` (habituelle sous Docker) **ne fonctionne pas sous Podman** :
   le DNS interne répond sur une adresse propre à chaque réseau, à relire dans la configuration
   réseau du conteneur plutôt que supposée.

3. **Les tableaux de bord Superset restaient invisibles pour le compte `dprest`** malgré les
   permissions accordées. Cause : ils étaient encore en brouillon (`published: false`). Un dashboard
   non publié reste invisible à tout le monde sauf son propriétaire, indépendamment des droits sur
   les données.

4. **Grafana perd son mot de passe admin à chaque redémarrage complet** (pas de volume persistant,
   limite déjà connue depuis l'étape 5) — le script `start-stack.ps1` le réinitialise automatiquement
   à chaque lancement, rien à faire de ton côté.

---

## Limites assumées, à mentionner en soutenance

- **Pas de compte Debezium séparé du compte applicatif** de `simulateur_V5` — changer le propriétaire
  de la publication de réplication sur un connecteur déjà actif a été jugé trop risqué pour être
  tenté sans fenêtre de test dédiée.
- **Pas de TLS entre les services internes** (Kafka, Flink, PostgreSQL, Kafka Connect) — tout tourne
  sur une seule machine, dans un réseau Podman qui ne sort jamais à l'extérieur. Une vraie mise en
  production distribuée sur plusieurs machines exigerait du TLS inter-services.
- **Pas d'agrégation centralisée des journaux** (ELK, Loki) — chaque service garde les siens.

---

## Critère de validation de l'étape 7

- [x] Aucun mot de passe en clair dans les fichiers versionnés (vérifiable : `git grep` sur les
      mots de passe connus ne doit rien trouver dans les fichiers suivis par Git).
- [x] Comptes en lecture seule créés et testés (lecture OK, écriture refusée).
- [x] Compte DPREST distinct de l'admin, restreint aux 2 tableaux de bord.
- [x] Traçabilité active et vérifiée (Superset, PostgreSQL analytique).
- [x] Grafana et Superset accessibles en HTTPS, ports en clair retirés de la publication vers l'hôte.
- [x] Pipeline entier revérifié après tous ces changements : chiffres source/cible toujours
      identiques, job Flink stable.
