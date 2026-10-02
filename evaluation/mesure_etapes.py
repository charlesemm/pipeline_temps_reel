"""Décomposition du délai par étape, à partir des événements Debezium dans Kafka.

Pour les N derniers événements d'insertion du topic des factures :
  - création -> validation en source : source.ts_ms - DATE_CREATION
  - validation -> lecture par Debezium : ts_ms - source.ts_ms
  - Debezium -> écriture dans Kafka : horodatage du message - ts_ms
Le reste du délai de bout en bout (mesure_latence.py) revient à Flink et à
l'écriture dans PostgreSQL.

Usage : python evaluation/mesure_etapes.py --n 400
"""
import argparse
import json
import os
import statistics
import subprocess
from datetime import datetime
from pathlib import Path

KAFKA = "pipeline_temps_reel-kafka-1"
TOPIC = "dprest-json.public.TB_FACTURES"
BIN = "/opt/kafka/bin"
RACINE = Path(__file__).resolve().parent.parent


def podman(*args):
    env = {**os.environ, "MSYS_NO_PATHCONV": "1"}
    return subprocess.run(["podman", "exec", KAFKA, *args], capture_output=True, text=True,
                          encoding="utf-8", env=env).stdout


def resume(nom, v):
    v = sorted(v)
    return (f"{nom} : n={len(v)}, médiane {statistics.median(v):.0f} ms, "
            f"95e percentile {v[int(0.95 * (len(v) - 1))]:.0f} ms, max {v[-1]:.0f} ms")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=400)
    args = ap.parse_args()

    fin = int(podman(f"{BIN}/kafka-get-offsets.sh", "--bootstrap-server", "localhost:9094",
                     "--topic", TOPIC).strip().split(":")[-1])
    sortie = podman(f"{BIN}/kafka-console-consumer.sh", "--bootstrap-server", "localhost:9094",
                    "--topic", TOPIC, "--partition", "0", "--offset", str(max(fin - args.n, 0)),
                    "--max-messages", str(args.n), "--property", "print.timestamp=true",
                    "--timeout-ms", "20000")

    transaction, capture, publication = [], [], []
    for ligne in sortie.splitlines():
        if "\t" not in ligne:
            continue
        ts, js = ligne.split("\t", 1)
        m = json.loads(js)
        if m.get("op") != "c":
            continue
        cree = datetime.fromisoformat(m["after"]["DATE_CREATION"].replace("Z", "+00:00")).timestamp() * 1000
        commit, lu, ecrit = m["source"]["ts_ms"], m["ts_ms"], int(ts.split(":")[1])
        transaction.append(commit - cree)
        capture.append(lu - commit)
        publication.append(ecrit - lu)

    lignes = [
        f"décomposition par étape ({datetime.now():%Y-%m-%d %H:%M:%S}), topic {TOPIC}",
        resume("création -> validation en source", transaction),
        resume("validation -> lecture Debezium", capture),
        resume("Debezium -> Kafka", publication),
        resume("création -> Kafka (total amont)", [a + b + c for a, b, c in zip(transaction, capture, publication)]),
    ]
    texte = "\n".join(lignes)
    dossier = RACINE / "evaluation" / "resultats"
    dossier.mkdir(exist_ok=True)
    (dossier / f"etapes_{datetime.now():%Y-%m-%d_%H%M%S}.txt").write_text(texte + "\n", encoding="utf-8")
    print(texte)


if __name__ == "__main__":
    main()
