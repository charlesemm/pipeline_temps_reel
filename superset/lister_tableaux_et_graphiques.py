"""Diagnostic temporaire : liste les dashboards Superset et leurs graphiques (noms exacts).
Non idempotent au sens où il n'écrit rien — script de lecture seule.
"""
from superset import db
from superset.models.dashboard import Dashboard
from superset.models.slice import Slice

for d in db.session.query(Dashboard).all():
    print(f"DASHBOARD id={d.id} titre={d.dashboard_title!r} publie={d.published}")
    for s in d.slices:
        print(f"    - slice id={s.id} nom={s.slice_name!r} viz={s.viz_type}")

print("\nToutes les slices (y compris hors dashboard) :")
for s in db.session.query(Slice).order_by(Slice.id).all():
    print(f"  id={s.id} nom={s.slice_name!r} viz={s.viz_type}")
