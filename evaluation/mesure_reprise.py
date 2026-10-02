"""Mesure du temps de reprise après une panne du traitement (annexe 6 du mémoire).

Scénario : on arrête brutalement le TaskManager Flink (panne du moteur de
calcul), on le laisse arrêté `--panne` secondes, puis on applique la procédure
d'exploitation documentée : scripts/start-stack.ps1, qui redémarre le conteneur
et resoumet le job. Sans point de contrôle, le job relit les topics depuis le
début ; l'écriture idempotente remet chaque ligne à sa valeur exacte.

Instants relevés (secondes depuis la panne) :
  - relance    : lancement de la procédure de redémarrage
  - job actif  : job kpi-continu de nouveau RUNNING
  - concordance: factures ET prestations du pipeline égales à la source
                 (source à l'arrêt pendant la mesure, pour une cible fixe)
  - fin de relecture : le nouveau job a relu autant d'événements que le job
                 d'avant la panne (métrique Flink numRecordsOut des sources) ; le
                 nombre d'événements relus rapporté à cette durée donne le
                 débit de traitement du moteur

Usage : python evaluation/mesure_reprise.py --panne 30
"""
import argparse
import json
import subprocess
import time
import urllib.request
from datetime import datetime
from pathlib import Path

import psycopg2

RACINE = Path(__file__).resolve().parent.parent
TASKMANAGER = "pipeline_temps_reel-flink-taskmanager-1"


def lire_env():
    env = {}
    for ligne in (RACINE / ".env").read_text(encoding="utf-8-sig").splitlines():
        if "=" in ligne and not ligne.lstrip().startswith("#"):
            cle, val = ligne.split("=", 1)
            env[cle.strip()] = val.strip()
    return env


FLINK = "http://localhost:8082"


def flink(chemin):
    with urllib.request.urlopen(FLINK + chemin, timeout=20) as r:
        return json.load(r)


def job_actif(exclu=None):
    """Identifiant du job kpi-continu RUNNING (autre que `exclu`), ou None."""
    try:
        jobs = flink("/jobs/overview")["jobs"]
    except Exception:  # noqa: BLE001
        return None
    return next((j["jid"] for j in jobs
                 if j["name"] == "kpi-continu" and j["state"] == "RUNNING" and j["jid"] != exclu), None)


def arriere_sources(jid, cache={}):
    """(événements restant à lire, événements lus) cumulés sur les sources Kafka du job."""
    if jid not in cache:
        noms = {}
        for v in flink(f"/jobs/{jid}")["vertices"]:
            if v["name"].startswith("Source:"):
                ids = [m["id"] for m in flink(f"/jobs/{jid}/vertices/{v['id']}/metrics")]
                noms[v["id"]] = [i for i in ids if i.endswith(".pendingRecords")
                                 or (i.startswith("0.Source__") and i.endswith(".numRecordsOut"))]
        # Les métriques n'apparaissent qu'une fois les tâches déployées : ne rien
        # mémoriser tant qu'aucune n'est publiée, sinon on lirait 0 indéfiniment.
        if not any(noms.values()):
            return 0, 0
        cache[jid] = noms
    restant = lus = 0
    for vid, ids in cache[jid].items():
        if not ids:
            continue
        for m in flink(f"/jobs/{jid}/vertices/{vid}/metrics?get={','.join(ids)}"):
            valeur = float(m["value"])
            if m["id"].endswith(".pendingRecords"):
                restant += valeur
            else:
                lus += valeur
    return int(restant), int(lus)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--panne", type=int, default=30, help="durée de la panne (s)")
    ap.add_argument("--limite", type=int, default=1800, help="abandon après (s)")
    args = ap.parse_args()

    pw = lire_env()["SGD_ADMIN_DB_PASSWORD"]
    src = psycopg2.connect(host="localhost", port=5433, dbname="echo_db", user="sgd_admin", password=pw)
    src.autocommit = True
    with src.cursor() as c:
        c.execute('SELECT (SELECT COUNT(*) FROM "TB_FACTURES"), (SELECT COUNT(*) FROM "TB_FACTURES_PRESTATIONS")')
        cible = c.fetchone()
    print(f"cible (source) : {cible[0]} factures, {cible[1]} prestations")

    def pipeline():
        cx = psycopg2.connect(host="localhost", port=15433, dbname="dprest_analytics",
                              user="sgd_admin", password=pw, connect_timeout=3)
        try:
            with cx.cursor() as c:
                c.execute("SELECT (SELECT COALESCE(SUM(nombre_factures),0) FROM kpi_factures_jour),"
                          " (SELECT COALESCE(SUM(nombre_prestations),0) FROM kpi_prestations_jour)")
                return tuple(int(x) for x in c.fetchone())
        finally:
            cx.close()

    assert pipeline() == cible, "pipeline non concordant avant la panne : attendre le rattrapage"

    ancien = job_actif()
    # Volume à relire : ce que le job en cours a lu depuis son démarrage (source figée).
    attendu = arriere_sources(ancien)[1]
    print(f"événements à relire après la panne : {attendu}")
    t0 = time.monotonic()
    debut = datetime.now()
    subprocess.run(["podman", "kill", TASKMANAGER], capture_output=True, check=True)
    print(f"{debut:%H:%M:%S} panne : TaskManager arrêté brutalement")
    time.sleep(args.panne)

    t_relance = time.monotonic() - t0
    procedure = subprocess.Popen(
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", str(RACINE / "scripts" / "start-stack.ps1")],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    print(f"+{t_relance:.0f} s relance : scripts/start-stack.ps1")

    t_job = t_conc = t_fin = None
    jid = None
    lus = 0
    minimum = cible
    releves = []
    while time.monotonic() - t0 < args.limite:
        t = time.monotonic() - t0
        if jid is None:
            jid = job_actif(exclu=ancien)
            if jid:
                t_job = t
                print(f"+{t:.0f} s job RUNNING", flush=True)
        restant = None
        if jid:
            try:
                restant, lus = arriere_sources(jid)
            except Exception as e:  # noqa: BLE001
                print(f"+{t:.0f} s métriques Flink indisponibles : {e}", flush=True)
        try:
            etat = pipeline()
        except Exception:  # noqa: BLE001
            etat = None
        if etat:
            releves.append((round(t, 1), *etat, restant, lus))
            minimum = min(minimum, etat)
            if t_job is not None and etat == cible and t_conc is None:
                t_conc = t
                print(f"+{t:.0f} s concordance des totaux : {etat}")
        # Relecture terminée : le nouveau job a relu tout l'historique.
        if jid and lus >= attendu and etat == cible:
            t_fin = t
            print(f"+{t:.0f} s relecture terminée : {lus} événements relus")
            break
        if int(t) % 30 == 0:
            print(f"+{t:.0f} s relus : {lus}/{attendu}", flush=True)
        time.sleep(1)
    procedure.wait(timeout=600)
    if t_fin is None:
        print("relecture non terminée dans la limite")

    lignes = [
        f"mesure de reprise ({debut:%Y-%m-%d %H:%M:%S})",
        f"volume : {cible[0]} factures, {cible[1]} prestations dans la source",
        f"panne du TaskManager : {args.panne} s",
        f"relance de la procédure : +{t_relance:.0f} s",
        f"job de nouveau RUNNING : +{t_job:.0f} s" if t_job else "job jamais RUNNING dans la limite",
        f"concordance des totaux : +{t_conc:.0f} s" if t_conc else "concordance non retrouvée dans la limite",
        f"relecture terminée (plus aucun événement en attente) : +{t_fin:.0f} s" if t_fin else "relecture non terminée dans la limite",
        f"temps de reprise (relance -> fin de relecture) : {t_fin - t_relance:.0f} s" if t_fin else "",
        f"événements relus : {lus} en {t_fin - t_job:.0f} s, soit un débit de traitement de "
        f"{lus / max(t_fin - t_job, 1):.0f} événements/s" if t_fin and t_job else "",
        f"plus bas relevé pendant la relecture : {minimum[0]} factures, {minimum[1]} prestations",
    ]
    texte = "\n".join(x for x in lignes if x)
    dossier = RACINE / "evaluation" / "resultats"
    dossier.mkdir(exist_ok=True)
    horodatage = f"{debut:%Y-%m-%d_%H%M%S}"
    (dossier / f"reprise_{horodatage}.txt").write_text(texte + "\n", encoding="utf-8")
    with (dossier / f"reprise_{horodatage}.csv").open("w", encoding="utf-8") as f:
        f.write("t_s;factures;prestations;evenements_en_attente;evenements_lus\n")
        f.writelines(";".join("" if x is None else str(x) for x in r) + "\n" for r in releves)
    print(texte)


if __name__ == "__main__":
    main()
