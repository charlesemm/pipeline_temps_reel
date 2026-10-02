"""Construit le tableau de bord « anomalies » du SGD (étape 6c, réalisé le 2026-09-24).

Source : table de quarantaine qualite_anomalies, par la connexion SGD (compte
sgd_qualite). Les graphiques n'exposent jamais la colonne donnee_brute (ligne
source nominative) : seuls domaine, motif, clé métier et date de détection.
Le tableau reste non publié : il est réservé au SGD (rôle Admin), pas à la DPREST.

Idempotent : les graphiques portant le même nom sont remplacés.
Exécution : voir rattacher_connexion_dprest.py (contexte de l'application).
"""
import json

from superset import db
from superset.connectors.sqla.models import SqlaTable
from superset.models.core import Database
from superset.models.dashboard import Dashboard
from superset.models.slice import Slice
from superset import security_manager

CONNEXION_SGD = "PostgreSQL analytique - qualité SGD (nominatif)"
TITRE = "[ dashboard anomalies]"
ANOMALIES = {"expressionType": "SQL", "sqlExpression": "COUNT(*)", "label": "Anomalies"}

base = db.session.query(Database).filter_by(database_name=CONNEXION_SGD).one()
jeu = db.session.query(SqlaTable).filter_by(table_name="qualite_anomalies", database_id=base.id).one_or_none()
if jeu is None:
    jeu = SqlaTable(table_name="qualite_anomalies", schema="public", database=base)
    db.session.add(jeu)
    db.session.flush()
    jeu.fetch_metadata()
jeu.main_dttm_col = "detecte_le"
for col in jeu.columns:
    col.is_dttm = col.column_name == "detecte_le"
admin = security_manager.find_user(username="sgd_admin")
jeu.owners = [admin]
db.session.commit()

commun = {"datasource": f"{jeu.id}__table", "adhoc_filters": [], "time_range": "No filter"}
graphiques = [
    ("Anomalies en quarantaine (total)", "big_number_total",
     {"metric": ANOMALIES, "subheader": "lignes isolées par le contrôle qualité"}),
    ("Anomalies par motif", "echarts_timeseries_bar",
     {"x_axis": "motif_anomalie", "metrics": [ANOMALIES], "groupby": [], "orientation": "horizontal",
      "row_limit": 50, "show_legend": False, "x_axis_sort_asc": False, "show_value": True}),
    ("Anomalies par domaine", "pie",
     {"groupby": ["domaine"], "metric": ANOMALIES, "show_labels": True, "label_type": "key_value",
      "row_limit": 20}),
    ("Anomalies détectées par jour", "echarts_timeseries_bar",
     {"x_axis": "detecte_le", "time_grain_sqla": "P1D", "metrics": [ANOMALIES], "groupby": ["domaine"],
      "row_limit": 10000, "show_legend": True}),
    ("Dernières anomalies (sans donnée nominative)", "table",
     {"query_mode": "raw", "all_columns": ["detecte_le", "domaine", "motif_anomalie", "cle_metier"],
      "order_by_cols": [json.dumps(["detecte_le", False])], "row_limit": 200,
      "server_pagination": False}),
]

tranches = []
for nom, viz, params in graphiques:
    s = db.session.query(Slice).filter_by(slice_name=nom).one_or_none() or Slice(slice_name=nom)
    s.viz_type = viz
    s.datasource_type = "table"
    s.datasource_id = jeu.id
    s.params = json.dumps({**commun, "viz_type": viz, **params})
    s.owners = [admin]
    db.session.add(s)
    tranches.append(s)
db.session.flush()

tableau = db.session.query(Dashboard).filter_by(dashboard_title=TITRE).one()
tableau.slices = tranches
tableau.owners = [admin]
tableau.published = False
# Disposition : logo CNAM, 3 indicateurs, la chronique au milieu, le détail en bas.
# Bloc logo identique aux dashboards prestation et entente préalable (même id,
# même code) ; /assets/ est servi par le reverse-proxy (reverse-proxy/nginx.conf).
positions = {"DASHBOARD_VERSION_KEY": "v2",
             "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
             "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": ["ROW-logo-cnam"], "parents": ["ROOT_ID"]},
             "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": TITRE}},
             "ROW-logo-cnam": {"type": "ROW", "id": "ROW-logo-cnam", "children": ["MARKDOWN-logo-cnam"],
                               "parents": ["ROOT_ID", "GRID_ID"], "meta": {"background": "BACKGROUND_TRANSPARENT"}},
             "MARKDOWN-logo-cnam": {"type": "MARKDOWN", "id": "MARKDOWN-logo-cnam", "children": [],
                                    "parents": ["ROOT_ID", "GRID_ID", "ROW-logo-cnam"],
                                    "meta": {"code": '<img src="/assets/logo-cnam.png" alt="Logo CNAM" height="80">',
                                             "width": 12, "height": 12}}}
rangees = [[(0, 3), (1, 5), (2, 4)], [(3, 12)], [(4, 12)]]
for i, rangee in enumerate(rangees):
    rid = f"ROW-anomalies-{i}"
    positions["GRID_ID"]["children"].append(rid)
    positions[rid] = {"type": "ROW", "id": rid, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                      "meta": {"background": "BACKGROUND_TRANSPARENT"}}
    for k, largeur in rangee:
        cid = f"CHART-anomalies-{k}"
        positions[rid]["children"].append(cid)
        positions[cid] = {"type": "CHART", "id": cid, "children": [], "parents": ["ROOT_ID", "GRID_ID", rid],
                          "meta": {"chartId": tranches[k].id, "sliceName": tranches[k].slice_name,
                                   "width": largeur, "height": 60 if i < 2 else 80}}
tableau.position_json = json.dumps(positions)
db.session.commit()
print(f"tableau « {TITRE} » : {len(tranches)} graphiques sur le jeu {jeu.id} ({CONNEXION_SGD})")
