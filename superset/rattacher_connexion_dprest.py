"""Rattache les tableaux de bord DPREST à la connexion DPREST (étape 7, correctif du 2026-09-24).

Constat du 22 septembre : 24 des graphiques DPREST interrogeaient la base par
la connexion du SGD (sgd_qualite, qui lit les tables nominatives), et le rôle
Superset du compte dprest_lecteur (Gamma) n'avait aucun droit sur les jeux de
données. Ce script :
  1. bascule les jeux de données des tableaux « prestation » et « entente
     préalable » sur la connexion « PostgreSQL analytique (dprest_analytics) »
     (compte PostgreSQL dprest_lecture, rôle role_kpi_lecture) ;
  2. crée le rôle Superset DPREST_Lecture, limité à ces jeux de données, et
     l'attribue à dprest_lecteur ;
  3. vérifie que chaque jeu de données s'interroge bien avec la connexion DPREST
     et que dprest_lecteur y a accès, mais pas au tableau de bord des anomalies.

Idempotent. Exécution :
  podman exec -i pipeline_temps_reel-superset-1 superset shell < superset/rattacher_connexion_dprest.py
"""
from sqlalchemy import text

from superset import db, security_manager
from superset.connectors.sqla.models import SqlaTable
from superset.models.core import Database
from superset.models.dashboard import Dashboard
from superset.utils.core import override_user

CONNEXION_DPREST = "PostgreSQL analytique (dprest_analytics)"
TABLEAUX_DPREST = ["[ dashboard prestation]", "[dashboard entente prealable ]"]
ROLE = "DPREST_Lecture"
COMPTE = "dprest_lecteur"

base_dprest = db.session.query(Database).filter_by(database_name=CONNEXION_DPREST).one()
tableaux = db.session.query(Dashboard).filter(Dashboard.dashboard_title.in_(TABLEAUX_DPREST)).all()
jeux = {s.datasource_id: s.datasource for d in tableaux for s in d.slices if s.datasource_type == "table"}

bascules = 0
for jeu in jeux.values():
    if jeu.database_id != base_dprest.id:
        jeu.database = base_dprest
        bascules += 1
    jeu.perm = jeu.get_perm()
    jeu.schema_perm = jeu.get_schema_perm()
    for graphique in jeu.slices:
        graphique.perm = jeu.perm
        graphique.schema_perm = jeu.schema_perm
db.session.commit()
print(f"jeux de données des tableaux DPREST : {len(jeux)}, basculés : {bascules}")

role = security_manager.find_role(ROLE) or security_manager.add_role(ROLE)
for jeu in jeux.values():
    pv = security_manager.add_permission_view_menu("datasource_access", jeu.perm)
    security_manager.add_permission_role(role, pv)
utilisateur = security_manager.find_user(username=COMPTE)
if role not in utilisateur.roles:
    utilisateur.roles.append(role)
db.session.commit()
print(f"rôle {ROLE} : {len(jeux)} droits datasource_access ; rôles de {COMPTE} : {[r.name for r in utilisateur.roles]}")

# Vérifications.
erreurs = 0
with base_dprest.get_sqla_engine() as moteur, moteur.connect() as cx:
    compte_pg = cx.execute(text("SELECT current_user")).scalar()
    for jeu in sorted(jeux.values(), key=lambda j: j.id):
        source = f"({jeu.sql}) AS q" if jeu.sql else f'"{jeu.schema or "public"}"."{jeu.table_name}"'
        try:
            n = cx.execute(text(f"SELECT COUNT(*) FROM {source}")).scalar()
            print(f"  OK  jeu {jeu.id:>2} {jeu.table_name[:40]:<40} {n} lignes (compte {compte_pg})")
        except Exception as e:  # noqa: BLE001
            erreurs += 1
            print(f"  ÉCHEC jeu {jeu.id} {jeu.table_name} : {str(e).splitlines()[0]}")
    try:
        cx.execute(text("SELECT donnee_brute FROM qualite_anomalies LIMIT 1"))
        erreurs += 1
        print("  ÉCHEC : la connexion DPREST lit donnee_brute")
    except Exception:  # noqa: BLE001
        print("  OK  connexion DPREST : lecture de donnee_brute refusée")

with override_user(utilisateur):
    for jeu in jeux.values():
        if not security_manager.can_access_datasource(jeu):
            erreurs += 1
            print(f"  ÉCHEC : {COMPTE} n'accède pas au jeu {jeu.id}")
    autres = db.session.query(SqlaTable).filter(SqlaTable.database_id != base_dprest.id).all()
    ouverts = [j.id for j in autres if security_manager.can_access_datasource(j)]
    print(f"  {'ÉCHEC' if ouverts else 'OK '} {COMPTE} : jeux de la connexion SGD accessibles : {ouverts or 'aucun'}")
    erreurs += bool(ouverts)

print(f"vérifications terminées : {erreurs} échec(s)")
