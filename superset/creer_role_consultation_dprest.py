"""Remplace le rôle Gamma du compte dprest_lecteur par un rôle de consultation (correctif du 2026-10-01).

Constat : avec Gamma, dprest_lecteur voyait le menu « Jeux de données », la liste
des bases, la requête SQL des graphiques et le détail des lignes (drill to
detail). La DPREST ne doit voir que les tableaux de bord et les graphiques.

Gamma n'est pas modifié : Superset le réécrit à chaque `superset init`. Ce script :
  1. crée le rôle DPREST_Consultation = liste blanche des droits d'interface
     nécessaires pour lire tableaux de bord et graphiques (rien sur Dataset,
     Database, SQL Lab, aucune écriture) ;
  2. remplace Gamma par DPREST_Consultation sur dprest_lecteur ; l'accès aux
     données reste porté par DPREST_Lecture (datasource_access) ;
  3. vérifie les droits accordés et refusés, et l'accès aux tableaux de bord.

Idempotent : le rôle est réaligné exactement sur la liste blanche à chaque
exécution. Exécution :
  podman cp superset/creer_role_consultation_dprest.py pipeline_temps_reel-superset-1:/tmp/
  podman exec pipeline_temps_reel-superset-1 python /tmp/creer_role_consultation_dprest.py
Retour arrière : réattribuer Gamma à dprest_lecteur et lui retirer DPREST_Consultation.
"""
from superset.app import create_app

ROLE = "DPREST_Consultation"
COMPTE = "dprest_lecteur"

# (droit, vue) autorisés. Tout ce qui n'est pas listé est refusé.
LISTE_BLANCHE = {
    # Navigation et page d'accueil
    ("menu_access", "Home"),
    ("menu_access", "Dashboards"),
    ("menu_access", "Charts"),
    ("can_get", "MenuApi"),
    ("can_recent_activity", "Log"),
    ("can_log", "Superset"),
    ("can_read", "SecurityRestApi"),
    ("can_read", "AvailableDomains"),
    ("can_list", "AsyncEventsRestApi"),
    ("can_list", "DynamicPlugin"),
    ("can_show", "DynamicPlugin"),
    ("can_read", "AdvancedDataType"),
    # Tableaux de bord (lecture, filtres, lien de partage)
    ("can_read", "Dashboard"),
    ("can_dashboard", "Superset"),
    ("can_dashboard_permalink", "Superset"),
    ("can_share_dashboard", "Superset"),
    ("can_view_chart_as_table", "Dashboard"),
    ("can_cache_dashboard_screenshot", "Dashboard"),
    ("can_read", "DashboardFilterStateRestApi"),
    ("can_write", "DashboardFilterStateRestApi"),
    ("can_read", "DashboardPermalinkRestApi"),
    ("can_write", "DashboardPermalinkRestApi"),
    # Graphiques (lecture, affichage seul, export CSV des résultats)
    ("can_read", "Chart"),
    ("can_slice", "Superset"),
    ("can_explore", "Superset"),
    ("can_share_chart", "Superset"),
    ("can_csv", "Superset"),
    ("can_read", "Explore"),
    ("can_read", "ExploreFormDataRestApi"),
    ("can_write", "ExploreFormDataRestApi"),
    ("can_read", "ExplorePermalinkRestApi"),
    ("can_write", "ExplorePermalinkRestApi"),
    ("can_query", "Api"),
    ("can_query_form_data", "Api"),
    ("can_time_range", "Api"),
    ("can_get_value", "KV"),
    ("can_store", "KV"),
    # Compte personnel
    ("can_userinfo", "UserDBModelView"),
    ("resetmypassword", "UserDBModelView"),
    ("can_this_form_get", "ResetMyPasswordView"),
    ("can_this_form_post", "ResetMyPasswordView"),
}

# Doivent être refusés (contrôle après application).
INTERDITS = [
    ("menu_access", "Datasets"),
    ("menu_access", "Data"),
    ("menu_access", "Databases"),
    ("can_read", "Dataset"),
    ("can_read", "Database"),
    ("can_get", "Datasource"),
    ("can_drill", "Dashboard"),
    ("can_view_query", "Dashboard"),
    ("can_write", "Dashboard"),
    ("can_write", "Chart"),
    ("can_export", "Dashboard"),
    ("can_export", "Chart"),
    ("menu_access", "SQL Lab"),
]


def main() -> None:
    """Aligne le rôle sur la liste blanche, l'attribue à dprest_lecteur et vérifie les accès."""
    from superset import db, security_manager
    from superset.models.dashboard import Dashboard
    from superset.utils.core import override_user

    role = security_manager.find_role(ROLE) or security_manager.add_role(ROLE)
    voulus = {security_manager.add_permission_view_menu(droit, vue) for droit, vue in LISTE_BLANCHE}
    retires = [pv for pv in role.permissions if pv not in voulus]
    for pv in retires:
        role.permissions.remove(pv)
    for pv in voulus - set(role.permissions):
        role.permissions.append(pv)

    utilisateur = security_manager.find_user(username=COMPTE)
    gamma = security_manager.find_role("Gamma")
    if gamma in utilisateur.roles:
        utilisateur.roles.remove(gamma)
    if role not in utilisateur.roles:
        utilisateur.roles.append(role)
    db.session.commit()
    print(f"rôle {ROLE} : {len(role.permissions)} droits ({len(retires)} retirés) ; "
          f"rôles de {COMPTE} : {sorted(r.name for r in utilisateur.roles)}")

    # Vérifications, en tant que dprest_lecteur.
    erreurs = 0
    with override_user(utilisateur):
        for droit, vue in INTERDITS:
            if security_manager.can_access(droit, vue):
                print(f"ERREUR : {droit} sur {vue} encore accordé")
                erreurs += 1
        for droit, vue in [("can_read", "Dashboard"), ("can_read", "Chart"), ("menu_access", "Dashboards")]:
            if not security_manager.can_access(droit, vue):
                print(f"ERREUR : {droit} sur {vue} refusé")
                erreurs += 1
        for tableau in db.session.query(Dashboard).order_by(Dashboard.id):
            lisibles = sum(
                1 for g in tableau.slices if g.datasource and security_manager.can_access_datasource(g.datasource)
            )
            print(f"tableau « {tableau.dashboard_title} » : {lisibles}/{len(tableau.slices)} graphiques lisibles")
    print("vérifications OK" if erreurs == 0 else f"{erreurs} erreur(s)")


if __name__ == "__main__":
    with create_app().app_context():
        main()
