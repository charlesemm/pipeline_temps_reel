"""Rappel de la détection d'anomalies Flink, mesuré contre la vérité terrain.

Compare, code par code, les anomalies réellement posées par l'injecteur du
simulateur (TB_ANOMALIES_INJECTIONS, base source) à celles isolées par Flink
dans qualite_anomalies (base analytique). La vérité terrain n'est lue qu'ici,
pour l'évaluation (chapitre 7) : le job Flink n'y a jamais accès.

Appariement : même code (ANOMALIE_CODE = motif_anomalie) et même ligne
métier. CIBLE_CLE vaut le numéro de facture (prestations, factures),
le personne_uuid (assurés) ou l'agent_code (agents) ; côté Flink,
cle_metier vaut "facture/prestation_code" pour une prestation, d'où la
comparaison sur la partie avant le "/".

Une injection trouvée sur la bonne ligne mais sous un autre motif est
comptée à part : le CASE de Flink ne retient qu'un motif par ligne.

Usage : python evaluation/mesure_detection_anomalies.py --depuis "2026-09-24 15:00"
Aucune donnée individuelle n'est affichée : uniquement des comptages.
"""
import argparse
import os
import subprocess
from collections import Counter, defaultdict

SOURCE = ("simulateur_v5-postgres-1", "echo", "echo_db")
ANALYTICS = ("pipeline_temps_reel-postgres-analytics-1", "dprest", "dprest_analytics")

# Codes du catalogue volontairement non couverts par le job Flink
# (voir flink/sql/kpi_prestations.sql, en-tête de la détection).
NON_COUVERTS = {"DOUBLON_EXACT", "DOUBLON_APPROCHANT", "DATE_HORS_DROITS", "FORMAT_DATE_INCOHERENT"}


def psql(cible: tuple[str, str, str], requete: str, *params: str) -> list[list[str]]:
    """Exécute une requête en lecture dans un conteneur PostgreSQL, renvoie les lignes."""
    conteneur, utilisateur, base = cible
    variables = [a for i, p in enumerate(params) for a in ("-v", f"p{i}={p}")]
    env = {**os.environ, "MSYS_NO_PATHCONV": "1"}
    sortie = subprocess.run(
        ["podman", "exec", "-i", conteneur, "psql", "-U", utilisateur, "-d", base,
         "-At", "-F", "\t", "-v", "ON_ERROR_STOP=1", *variables],
        input=requete, capture_output=True, text=True, encoding="utf-8", env=env, check=True,
    ).stdout
    return [ligne.split("\t") for ligne in sortie.splitlines() if ligne]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--depuis", required=True, help="début de la campagne d'injection (horodatage)")
    args = ap.parse_args()

    injections = psql(SOURCE, """
        SELECT "ANOMALIE_CODE", COALESCE("CIBLE_CLE", '')
        FROM "TB_ANOMALIES_INJECTIONS"
        WHERE "DATE_CREATION" >= :'p0'::timestamptz;
    """, args.depuis)
    detections = psql(ANALYTICS, """
        SELECT motif_anomalie, split_part(cle_metier, '/', 1)
        FROM qualite_anomalies
        WHERE detecte_le >= :'p0'::timestamptz;
    """, args.depuis)

    detecte = {(code, cle) for code, cle in detections}
    motifs_par_cle = defaultdict(set)
    for code, cle in detections:
        motifs_par_cle[cle].add(code)

    injectees, trouvees, autre_motif, sans_cle = Counter(), Counter(), Counter(), Counter()
    for code, cle in injections:
        injectees[code] += 1
        if not cle:
            sans_cle[code] += 1
        elif (code, cle) in detecte:
            trouvees[code] += 1
        elif motifs_par_cle.get(cle):
            autre_motif[code] += 1

    print(f"Campagne depuis {args.depuis} : {len(injections)} injections, {len(detections)} détections")
    print(f"{'code':28} {'injectées':>9} {'détectées':>9} {'rappel':>7} {'autre motif':>11}  couverture")
    for code in sorted(injectees):
        n = injectees[code]
        couverture = "non couvert (attendu 0 %)" if code in NON_COUVERTS else "couvert"
        if sans_cle[code]:
            couverture += f", {sans_cle[code]} sans CIBLE_CLE"
        print(f"{code:28} {n:9} {trouvees[code]:9} {trouvees[code] / n:7.0%} {autre_motif[code]:11}  {couverture}")

    couverts = [c for c in injectees if c not in NON_COUVERTS]
    total = sum(injectees[c] for c in couverts)
    if total:
        print(f"\nRappel global sur les codes couverts : {sum(trouvees[c] for c in couverts) / total:.1%} ({total} injections)")

    codes_injectes = set(injectees)
    hors_journal = Counter(code for code, _ in detections if code not in codes_injectes)
    if hors_journal:
        print("\nDétections sans injection du même code (contrôles de complétude ou faux positifs) :")
        for code, n in hors_journal.most_common():
            print(f"  {code:28} {n}")


if __name__ == "__main__":
    main()
