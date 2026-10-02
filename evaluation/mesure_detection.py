"""Taux de détection des anomalies : quarantaine du pipeline contre journal d'injection du simulateur.

Le simulateur consigne chaque anomalie qu'il injecte (TB_ANOMALIES_INJECTIONS : code, clé de la ligne
visée). Le pipeline n'a pas accès à ce journal (il n'est pas répliqué) : on le lit ici uniquement pour
évaluer, ligne par ligne, ce que la quarantaine (qualite_anomalies) a retrouvé.

Correspondance : même code d'anomalie et même clé (clé métier du pipeline avant le « / »).
Rien n'est écrit dans les bases.

Usage : python evaluation/mesure_detection.py
"""
from collections import Counter
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


def main():
    pw = lire_env()["SGD_ADMIN_DB_PASSWORD"]
    with psycopg2.connect(host="localhost", port=5433, dbname="echo_db", user="sgd_admin", password=pw) as src, \
         psycopg2.connect(host="localhost", port=15433, dbname="dprest_analytics", user="sgd_admin", password=pw) as ana:
        cs, ca = src.cursor(), ana.cursor()
        cs.execute('SELECT "ANOMALIE_CODE", "CIBLE_CLE" FROM "TB_ANOMALIES_INJECTIONS"')
        injectees = {(c, str(k)) for c, k in cs.fetchall()}
        ca.execute("SELECT motif_anomalie, split_part(cle_metier, '/', 1) FROM qualite_anomalies")
        detectees = {(c, k) for c, k in ca.fetchall()}

    codes_injectes = Counter(c for c, _ in injectees)
    trouvees = injectees & detectees
    codes_trouves = Counter(c for c, _ in trouvees)
    # Détections portant un code du catalogue mais sans injection correspondante.
    en_trop = Counter(c for c, k in detectees - injectees if c in codes_injectes)
    controles = Counter(c for c, _ in detectees if c not in codes_injectes)

    lignes = [f"taux de détection des anomalies ({datetime.now():%Y-%m-%d %H:%M:%S})",
              f"{'code':<22}{'injectées':>10}{'détectées':>10}{'taux':>8}{'en trop':>9}"]
    for code, n in codes_injectes.most_common():
        t = codes_trouves[code]
        lignes.append(f"{code:<22}{n:>10}{t:>10}{100 * t / n:>7.0f}%{en_trop[code]:>9}")
    total, total_t = sum(codes_injectes.values()), sum(codes_trouves.values())
    lignes.append(f"{'TOTAL':<22}{total:>10}{total_t:>10}{100 * total_t / total:>7.0f}%{sum(en_trop.values()):>9}")
    lignes.append("contrôles de complétude hors catalogue : "
                  + (", ".join(f"{c} {n}" for c, n in controles.most_common()) or "aucun"))
    texte = "\n".join(lignes)
    dossier = RACINE / "evaluation" / "resultats"
    dossier.mkdir(exist_ok=True)
    (dossier / f"detection_{datetime.now():%Y-%m-%d_%H%M%S}.txt").write_text(texte + "\n", encoding="utf-8")
    print(texte)


if __name__ == "__main__":
    main()
