"""Configuration Superset — étape 6 (restitution métier, public DPREST).

Architecture proche production, choisie explicitement (voir docs/decisions.md) :
base de métadonnées PostgreSQL dédiée (superset-db) + cache Redis (superset-redis),
plutôt que le SQLite embarqué par défaut.

Toutes les valeurs sensibles viennent de variables d'environnement (docker-compose.yml) —
aucune n'est codée en dur ici, contrairement à d'autres services de ce dépôt où le
mot de passe apparaît en clair (services internes, sans exposition réseau au-delà de
cette machine) : Superset gère en plus des comptes utilisateurs, donc on applique ici
la règle stricte de CLAUDE.md.
"""

import os

# ── Base de métadonnées (dashboards, utilisateurs, connexions) ──────────
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{os.environ['SUPERSET_DB_USER']}:"
    f"{os.environ['SUPERSET_DB_PASSWORD']}@{os.environ['SUPERSET_DB_HOST']}:5432/"
    f"{os.environ['SUPERSET_DB_NAME']}"
)

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]

# ── Cache (résultats de requêtes, filtres) ───────────────────────────────
_REDIS_HOST = os.environ.get("SUPERSET_REDIS_HOST", "superset-redis")
_REDIS_PORT = int(os.environ.get("SUPERSET_REDIS_PORT", "6379"))

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_cache_",
    "CACHE_REDIS_HOST": _REDIS_HOST,
    "CACHE_REDIS_PORT": _REDIS_PORT,
    "CACHE_REDIS_DB": 1,
}
DATA_CACHE_CONFIG = CACHE_CONFIG
FILTER_STATE_CACHE_CONFIG = {**CACHE_CONFIG, "CACHE_REDIS_DB": 2, "CACHE_KEY_PREFIX": "superset_filter_"}
EXPLORE_FORM_DATA_CACHE_CONFIG = {**CACHE_CONFIG, "CACHE_REDIS_DB": 3, "CACHE_KEY_PREFIX": "superset_explore_"}

# Pas de worker Celery asynchrone sur ce simulateur (économie de RAM,
# volumétrie faible) : les requêtes restent synchrones. Le cache Redis
# sert uniquement à accélérer l'affichage répété d'un même graphique,
# pas à faire des exports planifiés en tâche de fond.

# ── Langue et fuseau horaire ──────────────────────────────────────────
BABEL_DEFAULT_LOCALE = "fr"
DEFAULT_LOCALE = "fr"

# Dev/démo uniquement : pas de HTTPS forcé, cohérent avec le reste du
# simulateur sur cette machine. À revoir à l'étape 7 (sécurité).
ENABLE_PROXY_FIX = True
