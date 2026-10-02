"""Fusionne les dashboards DPREST « Prestations » et « Ententes préalables » en un seul,
réduit à 10 graphiques (demande du 2026-09-30) : trop de KPI dilués pour un tableau de bord de
synthèse destiné à la DPREST.

**Révision du 2026-09-30, après inspection de l'état réel de Superset** : la sélection ci-dessous
ne suit pas les noms de `docs/guides/etape6a_superset_prestations.md`/`etape6b_superset_ententes.md`
— ces guides décrivent une construction qui n'a pas été suivie telle quelle. Les graphiques
réellement présents ont des noms différents, et deux KPI prévus par les guides n'ont jamais été
construits (pas de jauge « taux de couverture CMU », pas de « délai moyen de traitement EP »).
En repli, la mission CMU est représentée par `REAPARTITION MONTANT PRIS EN CHARGE PAR TYPE
FACTURE`, seul graphique existant sur ce sujet. Le tableau réglementaire mensuel
`ENTENTES PREALALES MENSUELLES` (slice id 23) existe mais n'était rattaché à **aucun** dashboard
avant ce script — il répond pourtant à l'obligation « avant le 5 du mois » du périmètre DPREST
(CLAUDE.md) : rattaché ici en priorité.

Ne recrée aucun graphique : les 10 slices existent déjà. Ce script les rassemble dans un seul
dashboard et renomme le dashboard « Prestations » (id 1) en dashboard de synthèse. Le dashboard
« Ententes préalables » (id 2) est **dépublié**, pas supprimé : ses 8 graphiques restent
consultables individuellement si besoin (menu Charts), de même que les graphiques de l'ancien
dashboard Prestations non repris ici.

Propriétaire : `admin`, seul compte Superset restant depuis la suppression de `sgd_admin` et
`dprest_lecteur` le 2026-09-29 (voir docs/decisions.md). Aucun nouveau jeu de données n'est
introduit ici : rien à ajouter au rôle `DPREST_Lecture` si les comptes de lecture sont un jour
recréés.

Idempotent. Exécution (pas via `superset shell` : le mode interactif lit le fichier ligne à
ligne et casse les blocs `for` suivis d'une instruction) :
  podman cp superset/fusionner_tableaux_dprest.py pipeline_temps_reel-superset-1:/tmp/f.py
  podman exec pipeline_temps_reel-superset-1 python -c "from superset.app import create_app; app = create_app(); ctx = app.app_context(); ctx.push(); exec(open('/tmp/f.py', encoding='utf-8').read())"
"""
import json

from superset import db, security_manager
from superset.models.dashboard import Dashboard
from superset.models.slice import Slice

TITRE_PRESTATION = "[ dashboard prestation]"
TITRE_ENTENTE = "[dashboard entente prealable ]"
TITRE_SYNTHESE = "[ dashboard synthese dprest ]"

# (nom exact du graphique, largeur en douzièmes de grille) ; une sous-liste = une ligne.
LIGNES = [
    [("NOMBRES DE PRESTATIONS", 4), ("NOMBRES DE FACTURES", 4), ("MONTANT TOTAL FACTURE", 4)],
    [("NOMBRES D'ENTENPTES PREALABLES", 4), ("TAUX DE REPONSES", 4), ("MONTANT ENGAGE PAR ENTENTE PREALABLE", 4)],
    [("NOMBRES DE PRESTATIONS PAR TYPE D'ACTES", 6), ("ENTENTE PREALABLE PAR STATUT", 6)],
    [("REAPARTITION MONTANT PRIS EN CHARGE PAR TYPE FACTURE", 12)],
    [("ENTENTES PREALALES MENSUELLES", 12)],
]

admin = security_manager.find_user(username="admin")
if admin is None:
    raise SystemExit("compte 'admin' introuvable — vérifier le nom du compte Superset restant")

noms = [nom for ligne in LIGNES for nom, _ in ligne]
graphiques = {s.slice_name: s for s in db.session.query(Slice).filter(Slice.slice_name.in_(noms)).all()}
manquants = [n for n in noms if n not in graphiques]
if manquants:
    raise SystemExit(f"graphiques introuvables, à créer d'abord : {manquants}")

tableau = db.session.query(Dashboard).filter_by(dashboard_title=TITRE_PRESTATION).one()
tableau.dashboard_title = TITRE_SYNTHESE
tableau.slices = [graphiques[n] for n in noms]
tableau.owners = [admin]
tableau.published = False  # republié à la main après vérification visuelle des 10 chiffres

# Disposition : logo CNAM (même bloc que les autres dashboards DPREST), puis une ligne par
# groupe de LIGNES, et le tableau réglementaire mensuel isolé en pleine largeur en dernière ligne.
positions = {
    "DASHBOARD_VERSION_KEY": "v2",
    "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
    "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": ["ROW-logo-cnam"], "parents": ["ROOT_ID"]},
    "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": TITRE_SYNTHESE}},
    "ROW-logo-cnam": {"type": "ROW", "id": "ROW-logo-cnam", "children": ["MARKDOWN-logo-cnam"],
                      "parents": ["ROOT_ID", "GRID_ID"], "meta": {"background": "BACKGROUND_TRANSPARENT"}},
    "MARKDOWN-logo-cnam": {"type": "MARKDOWN", "id": "MARKDOWN-logo-cnam", "children": [],
                           "parents": ["ROOT_ID", "GRID_ID", "ROW-logo-cnam"],
                           "meta": {"code": '<img src="/assets/logo-cnam.png" alt="Logo CNAM" height="80">',
                                    "width": 12, "height": 12}},
}
for i, ligne in enumerate(LIGNES):
    rid = f"ROW-synthese-{i}"
    positions["GRID_ID"]["children"].append(rid)
    positions[rid] = {"type": "ROW", "id": rid, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                      "meta": {"background": "BACKGROUND_TRANSPARENT"}}
    for nom, largeur in ligne:
        s = graphiques[nom]
        cid = f"CHART-synthese-{s.id}"
        positions[rid]["children"].append(cid)
        positions[cid] = {"type": "CHART", "id": cid, "children": [], "parents": ["ROOT_ID", "GRID_ID", rid],
                          "meta": {"chartId": s.id, "sliceName": s.slice_name, "width": largeur,
                                   "height": 50 if largeur < 12 else 70}}
tableau.position_json = json.dumps(positions)

ancien_ep = db.session.query(Dashboard).filter_by(dashboard_title=TITRE_ENTENTE).one_or_none()
if ancien_ep is not None:
    ancien_ep.published = False

db.session.commit()
print(f"tableau « {TITRE_SYNTHESE} » : {len(tableau.slices)} graphiques, publié={tableau.published}")
if ancien_ep is not None:
    print(f"tableau « {TITRE_ENTENTE} » dépublié (conservé, non supprimé)")
else:
    print(f"tableau « {TITRE_ENTENTE} » introuvable (déjà fusionné ou renommé ?)")
