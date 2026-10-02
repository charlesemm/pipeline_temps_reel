"""Ouvre le tableau de bord « anomalies » à la DPREST, sans la ligne source (2026-09-24).

Décision du 2026-09-24 : la DPREST doit voir la quarantaine qualité. Elle la
voit par la vue v_qualite_anomalies (sql/analytics/011_v_qualite_anomalies.sql),
qui exclut donnee_brute : la ligne source nominative reste réservée au SGD.
Ce script :
  1. crée le jeu de données v_qualite_anomalies sur la connexion DPREST
     (compte PostgreSQL dprest_lecture) ;
  2. y rebranche les graphiques du tableau et son filtre natif par domaine
     (jusque-là sur qualite_anomalies, par la connexion nominative du SGD) ;
  3. donne au rôle DPREST_Lecture l'accès à ce seul jeu de données ;
  4. rend la propriété du tableau à sgd_admin : un propriétaire accède à
     l'objet quels que soient ses rôles (tests/test_securite.py).
Le jeu qualite_anomalies de la connexion SGD n'est pas supprimé.

Idempotent. Exécution (pas via `superset shell` : le mode interactif lit le
fichier ligne à ligne et casse les blocs `for` suivis d'une instruction) :
  podman cp superset/ouvrir_tableau_anomalies_dprest.py pipeline_temps_reel-superset-1:/tmp/s.py
  podman exec pipeline_temps_reel-superset-1 python -c "from superset.app import create_app; app = create_app(); ctx = app.app_context(); ctx.push(); exec(open('/tmp/s.py', encoding='utf-8').read())"
"""
import json

from superset import db, security_manager
from superset.connectors.sqla.models import SqlaTable
from superset.models.core import Database
from superset.models.dashboard import Dashboard

CONNEXION_DPREST = "PostgreSQL analytique (dprest_analytics)"
TITRE = "[ dashboard anomalies]"
VUE = "v_qualite_anomalies"
ROLE = "DPREST_Lecture"

base = db.session.query(Database).filter_by(database_name=CONNEXION_DPREST).one()
admin = security_manager.find_user(username="sgd_admin")

jeu = db.session.query(SqlaTable).filter_by(table_name=VUE, database_id=base.id).one_or_none()
if jeu is None:
    jeu = SqlaTable(table_name=VUE, schema="public", database=base)
    db.session.add(jeu)
    db.session.flush()
    jeu.fetch_metadata()
jeu.main_dttm_col = "detecte_le"
for col in jeu.columns:
    col.is_dttm = col.column_name == "detecte_le"
jeu.owners = [admin]
jeu.perm = jeu.get_perm()
jeu.schema_perm = jeu.get_schema_perm()
db.session.flush()

tableau = db.session.query(Dashboard).filter_by(dashboard_title=TITRE).one()
for graphique in tableau.slices:
    params = json.loads(graphique.params or "{}")
    params["datasource"] = f"{jeu.id}__table"
    graphique.params = json.dumps(params)
    graphique.datasource_type = "table"
    graphique.datasource_id = jeu.id
    graphique.perm = jeu.perm
    graphique.schema_perm = jeu.schema_perm
    graphique.owners = [admin]

meta = json.loads(tableau.json_metadata or "{}")
for filtre in meta.get("native_filter_configuration", []):
    for cible in filtre.get("targets", []):
        if cible.get("datasetId") is not None:
            cible["datasetId"] = jeu.id
tableau.json_metadata = json.dumps(meta)
tableau.owners = [admin]

role = security_manager.find_role(ROLE)
pv = security_manager.add_permission_view_menu("datasource_access", jeu.perm)
security_manager.add_permission_role(role, pv)
db.session.commit()

print(f"tableau « {TITRE} » : {len(tableau.slices)} graphiques sur le jeu {jeu.id} ({CONNEXION_DPREST}) ; "
      f"propriétaires {[u.username for u in tableau.owners]} ; publié={tableau.published} ; accès {ROLE} accordé")
