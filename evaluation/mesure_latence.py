"""Mesure du délai de bout en bout source -> base analytique (annexe 6 du mémoire).

Principe : la sonde interroge toutes les `--pas` secondes le nombre de factures
dans la source (TB_FACTURES) et dans la base analytique (somme de
kpi_factures_jour). La k-ième facture créée dans la source est réputée prise en
compte par le pipeline au premier relevé où le compteur analytique atteint k.
Délai(k) = instant de ce relevé - DATE_CREATION de la facture k.
Les deux bases tournent sur la même machine virtuelle : même horloge.

Rien n'est écrit dans les bases. Résultat : un CSV par facture et un résumé.

Usage : python evaluation/mesure_latence.py --duree 300 --libelle charge_normale
"""
import argparse
import csv
import statistics
import time
from datetime import datetime
from pathlib import Path

import psycopg2

RACINE = Path(__file__).resolve().parent.parent


def lire_env():
    env = {}
    for ligne in (RACINE / ".env").read_text(encoding="utf-8-sig").splitlines():
        if "=" in ligne and not ligne.lstrip().startswith("#"):
            cle, val = ligne.split("=", 1)
            env[cle.strip()] = val.strip()
    return env


def percentile(valeurs, p):
    v = sorted(valeurs)
    k = (len(v) - 1) * p / 100
    b = int(k)
    return v[b] + (v[min(b + 1, len(v) - 1)] - v[b]) * (k - b)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--duree", type=int, default=300, help="durée d'observation (s)")
    ap.add_argument("--pas", type=float, default=0.2, help="intervalle entre relevés (s)")
    ap.add_argument("--libelle", default="mesure")
    args = ap.parse_args()

    env = lire_env()
    pw = env["SGD_ADMIN_DB_PASSWORD"]
    src = psycopg2.connect(host="localhost", port=5433, dbname="echo_db", user="sgd_admin", password=pw)
    ana = psycopg2.connect(host="localhost", port=15433, dbname="dprest_analytics", user="sgd_admin", password=pw)
    src.autocommit = ana.autocommit = True
    cs, ca = src.cursor(), ana.cursor()

    def compte_source():
        cs.execute('SELECT COUNT(*) FROM "TB_FACTURES"')
        return cs.fetchone()[0]

    def compte_pipeline():
        ca.execute("SELECT COALESCE(SUM(nombre_factures), 0), clock_timestamp() FROM kpi_factures_jour")
        return ca.fetchone()

    # Point de départ : on attend un instant où les deux compteurs sont égaux,
    # pour que la k-ième facture source corresponde à la k-ième unité analytique.
    for _ in range(600):
        n_src = compte_source()
        n_pipe, _t = compte_pipeline()
        if n_src == n_pipe:
            break
        time.sleep(0.05)
    else:
        raise SystemExit(f"compteurs jamais égaux (source {n_src}, pipeline {n_pipe}) : pipeline en rattrapage ?")
    n0 = n_src
    print(f"départ : {n0} factures dans les deux bases")

    releves = []  # (instant analytique, compteur pipeline)
    fin = time.monotonic() + args.duree
    while time.monotonic() < fin:
        n_pipe, t = compte_pipeline()
        releves.append((t, n_pipe))
        time.sleep(args.pas)

    # Laisser le pipeline rattraper les dernières factures créées.
    n_final_src = compte_source()
    limite = time.monotonic() + 120
    while time.monotonic() < limite:
        n_pipe, t = compte_pipeline()
        releves.append((t, n_pipe))
        if n_pipe >= n_final_src:
            break
        time.sleep(args.pas)

    cs.execute(
        'SELECT "FACTURE_NUMERO", "DATE_CREATION" FROM "TB_FACTURES" '
        'ORDER BY "DATE_CREATION", "FACTURE_NUMERO" OFFSET %s LIMIT %s',
        (n0, n_final_src - n0),
    )
    factures = cs.fetchall()

    lignes, delais = [], []
    i = 0
    for k, (numero, cree) in enumerate(factures, start=1):
        while i < len(releves) and releves[i][1] < n0 + k:
            i += 1
        if i == len(releves):
            break  # pas encore vue : hors mesure
        vu = releves[i][0]
        d = (vu - cree).total_seconds()
        delais.append(d)
        lignes.append((numero, cree.isoformat(), vu.isoformat(), round(d, 3)))

    horodatage = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    dossier = RACINE / "evaluation" / "resultats"
    dossier.mkdir(exist_ok=True)
    fichier = dossier / f"latence_{args.libelle}_{horodatage}.csv"
    with fichier.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["facture", "creee_source", "vue_analytique", "delai_s"])
        w.writerows(lignes)

    duree_reelle = (releves[-1][0] - releves[0][0]).total_seconds()
    resume = [
        f"mesure : {args.libelle} ({horodatage})",
        f"volume source au départ : {n0} factures",
        f"factures mesurées : {len(delais)} sur {len(factures)} créées pendant {duree_reelle:.0f} s "
        f"(débit source moyen {len(factures) / max(duree_reelle, 1):.2f} factures/s)",
        f"pas de relevé : {args.pas} s ({len(releves)} relevés)",
    ]
    if delais:
        resume += [
            f"délai médian : {statistics.median(delais):.2f} s",
            f"délai moyen : {statistics.mean(delais):.2f} s",
            f"95e percentile : {percentile(delais, 95):.2f} s",
            f"minimum / maximum : {min(delais):.2f} s / {max(delais):.2f} s",
        ]
    texte = "\n".join(resume)
    (dossier / f"latence_{args.libelle}_{horodatage}.txt").write_text(texte + "\n", encoding="utf-8")
    print(texte)
    print(f"détail : {fichier}")


if __name__ == "__main__":
    main()
