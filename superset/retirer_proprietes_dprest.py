"""Retire au compte en lecture dprest_lecteur toute propriété d'objet Superset (correctif du 2026-09-24).

Constat : dprest_lecteur figurait parmi les propriétaires de jeux de données,
graphiques et tableaux de bord. Dans Superset, un propriétaire accède à l'objet
et peut le modifier, quels que soient ses rôles : le compte DPREST lisait ainsi
les jeux de données de la connexion nominative du SGD. La propriété est
transférée à sgd_admin. Idempotent. À exécuter dans le contexte de l'application :
voir rattacher_connexion_dprest.py pour la commande.
"""
from superset import db, security_manager
from superset.connectors.sqla.models import SqlaTable
from superset.models.dashboard import Dashboard
from superset.models.slice import Slice
from superset.utils.core import override_user

lecteur = security_manager.find_user(username="dprest_lecteur")
admin = security_manager.find_user(username="sgd_admin")
for modele in (SqlaTable, Slice, Dashboard):
    n = 0
    for objet in db.session.query(modele).all():
        if lecteur in objet.owners:
            objet.owners.remove(lecteur)
            if admin not in objet.owners:
                objet.owners.append(admin)
            n += 1
    print(f"{modele.__name__} : propriété retirée sur {n} objet(s)")

# Lignes vides après chaque bloc : `superset shell` est une console interactive,
# un bloc non terminé par une ligne vide provoque une SyntaxError.
db.session.commit()

with override_user(lecteur):
    ouverts = [j.id for j in db.session.query(SqlaTable).all() if security_manager.can_access_datasource(j)]
    sgd = [j.id for j in db.session.query(SqlaTable).all() if j.database.database_name.startswith("PostgreSQL analytique - qualité") and j.id in ouverts]

print(f"jeux accessibles à dprest_lecteur : {sorted(ouverts)}")
print(f"{'ÉCHEC' if sgd else 'OK'} : jeux de la connexion SGD accessibles : {sgd or 'aucun'}")
