"""Supprime la connexion Superset du SGD (compte sgd_qualite), ses jeux de donnees et graphiques (2026-10-02).

Constat : connexion "qualite SGD (nominatif)" sans usage (23 jeux de donnees,
1 graphique hors de tout tableau de bord, aucune requete SQL Lab sur 30 jours).
Les donnees nominatives se lisent avec le compte humain sgd_admin.
Sauvegarde prealable : backups/avant_comptes_20261002/superset_meta.dump.gpg.

Fichier volontairement en ASCII : PowerShell (Get-Content | ...) altere les
accents, et la connexion est retrouvee par son compte, pas par son nom.
Idempotent (ne fait rien si la connexion n'existe plus). Execution :
  cmd /c "docker compose exec -T superset superset shell < superset\\supprimer_connexion_sgd.py"
"""
from sqlalchemy import text

from superset import db
from superset.connectors.sqla.models import SqlaTable
from superset.models.core import Database
from superset.models.slice import Slice

bases = [d for d in db.session.query(Database).all() if "//sgd_qualite:" in d.sqlalchemy_uri]
ids_bases = [d.id for d in bases]
jeux = db.session.query(SqlaTable).filter(SqlaTable.database_id.in_(ids_bases)).all() if ids_bases else []
ids = [j.id for j in jeux]
graphiques = db.session.query(Slice).filter(Slice.datasource_type == "table", Slice.datasource_id.in_(ids)).all() if ids else []
print(f"RESULTAT a supprimer : {len(bases)} connexion(s), {len(jeux)} jeu(x), graphiques {[g.slice_name for g in graphiques]}")

# Lignes vides apres chaque bloc : superset shell est une console interactive.
for g in graphiques:
    g.dashboards = []
    db.session.delete(g)

db.session.flush()

# Proprietaires en double dans sqlatable_user (20 lignes constatees le 2026-10-02) :
# l'ORM attend une ligne par couple et annule tout -> on vide ces liens en SQL d'abord.
if ids:
    db.session.execute(text("DELETE FROM sqlatable_user WHERE table_id = ANY(:ids)"), {"ids": ids})
    db.session.expire_all()

for j in jeux:
    db.session.delete(j)

db.session.flush()

for d in bases:
    db.session.delete(d)

db.session.commit()
print(f"RESULTAT connexions restantes : {[d.id for d in db.session.query(Database).all()]} (attendu : [1])")
