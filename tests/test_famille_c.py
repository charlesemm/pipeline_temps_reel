"""Cohérence des fichiers de la famille C (prescriptions et pathologies).

Tests purs sur les fichiers du dépôt : aucun conteneur requis. Ils protègent
contre les oublis qui ont déjà coûté des incidents (voir docs/decisions.md) :
table suivie par CDC sans REPLICA IDENTITY FULL, sink Flink sans droits
d'écriture, groupe de consommation refusé par les ACL Kafka, vue clinique
exposée sans masquage.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
CONNECTOR = ROOT / "connectors" / "debezium-postgres-json.json"
FLINK_SQL = ROOT / "flink" / "sql" / "kpi_prestations.sql"
ANALYTICS_SQL = ROOT / "sql" / "analytics" / "010_kpi_clinique.sql"
REPLICA_SQL = ROOT / "sql" / "source" / "001_replica_identity_cdc.sql"

FAMILLE_C_SOURCE_TABLES = {
    "TB_FACTURES_PRESCRIPTIONS",
    "TB_FACTURES_PATHOLOGIES",
    "TB_REF_MEDICAMENTS",
    "TB_REF_DCI",
    "TB_REF_PATHOLOGIES",
}
FAMILLE_C_SINKS = {
    "kpi_prescriptions_medicament_jour",
    "kpi_pathologies_jour",
    "fait_prescriptions",
    "fait_pathologies",
    "fait_entente_facture",
    "fait_entente_statut",
    "dim_medicaments",
    "dim_dci",
    "dim_pathologies",
}
# Objets lus par la DPREST : uniquement les vues masquées et les dimensions.
VUES_MASQUEES = {
    "v_kpi_prescriptions_jour",
    "v_top10_medicaments",
    "v_top10_pathologies",
    "v_kpi_ep_prescriptions",
    "v_kpi_ep_medicaments",
    "v_kpi_ep_pathologies",
}
TABLES_SGD = {
    "kpi_prescriptions_medicament_jour",
    "kpi_pathologies_jour",
    "fait_prescriptions",
    "fait_pathologies",
    "fait_entente_facture",
    "fait_entente_statut",
    "v_ep_clinique_detail",
}


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _connector_tables() -> set[str]:
    config = json.loads(_read(CONNECTOR))["config"]
    return {t.split(".", 1)[1] for t in config["table.include.list"].split(",")}


def _grant_block(sql: str, role: str) -> str:
    """Retourne le texte des GRANT ... TO <role> (tables listées incluses)."""
    blocs = re.findall(r"GRANT[^;]*?TO\s+" + role + r"\s*;", sql, flags=re.DOTALL)
    return "\n".join(blocs)


def test_connector_tracks_famille_c_tables() -> None:
    assert FAMILLE_C_SOURCE_TABLES <= _connector_tables()


def test_replica_identity_covers_every_tracked_table() -> None:
    """Sans REPLICA IDENTITY FULL, un UPDATE fait tomber le job Flink (2026-09-11)."""
    couvertes = set(re.findall(r"'(TB_[A-Z_]+)'", _read(REPLICA_SQL)))
    manquantes = _connector_tables() - couvertes
    assert not manquantes, f"REPLICA IDENTITY FULL absent pour : {sorted(manquantes)}"


def test_flink_kafka_sources_read_tracked_topics() -> None:
    sql = _read(FLINK_SQL)
    topics = set(re.findall(r"'topic' = 'dprest-json\.public\.([A-Z_]+)'", sql))
    assert FAMILLE_C_SOURCE_TABLES <= topics
    assert topics <= _connector_tables(), "un topic Flink n'est pas produit par le connecteur"


def test_flink_consumer_groups_match_kafka_acl() -> None:
    """Les ACL n'autorisent que les groupes préfixés « flink-kpi- » (kafka-secure-setup.ps1)."""
    groupes = re.findall(r"'properties\.group\.id' = '([^']+)'", _read(FLINK_SQL))
    assert groupes, "aucun group.id trouvé"
    hors_acl = [g for g in groupes if not g.startswith("flink-kpi-")]
    assert not hors_acl, f"groupes refusés par les ACL Kafka : {hors_acl}"
    assert len(groupes) == len(set(groupes)), "deux sources partagent un group.id"


def test_flink_sinks_exist_in_analytics_schema() -> None:
    flink = _read(FLINK_SQL)
    analytics = _read(ANALYTICS_SQL)
    for table in FAMILLE_C_SINKS:
        assert re.search(rf"CREATE TABLE {table}\b", flink), f"sink Flink absent : {table}"
        assert re.search(
            rf"CREATE TABLE IF NOT EXISTS {table}\b", analytics
        ), f"table analytique absente de 010 : {table}"
        assert f"'table-name' = '{table}'" in flink, f"table-name incorrect : {table}"


def test_flink_writer_can_write_every_famille_c_sink() -> None:
    grants = _grant_block(_read(ANALYTICS_SQL), "role_flink_ecriture")
    manquants = [t for t in FAMILLE_C_SINKS if not re.search(rf"\b{t}\b", grants)]
    assert not manquants, f"role_flink_ecriture sans droit sur : {manquants}"


def test_masked_views_apply_confidentiality_threshold() -> None:
    """H10 : chaque vue exposée à la DPREST supprime les regroupements < 5."""
    sql = _read(ANALYTICS_SQL)
    for vue in VUES_MASQUEES:
        m = re.search(rf"CREATE OR REPLACE VIEW {vue} AS(.*?);", sql, flags=re.DOTALL)
        assert m, f"vue absente : {vue}"
        assert re.search(r"HAVING[^;]*>=\s*5", m.group(1)), f"{vue} sans seuil de masquage >= 5"


def test_dprest_role_only_reads_masked_views_and_dimensions() -> None:
    """H10 : la DPREST ne lit ni les faits, ni les agrégats bruts, ni la vue interne."""
    grants = _grant_block(_read(ANALYTICS_SQL), "role_kpi_lecture")
    for objet in TABLES_SGD:
        assert not re.search(rf"\b{objet}\b", grants), f"{objet} ne doit pas être lisible par la DPREST"
    for vue in VUES_MASQUEES:
        assert re.search(rf"\b{vue}\b", grants), f"{vue} doit être lisible par la DPREST"


def test_no_default_privilege_leak_to_dprest_lecture() -> None:
    """Le droit par défaut posé le 2026-09-21 donnait SELECT sur toute nouvelle table."""
    sql = _read(ANALYTICS_SQL)
    assert "REVOKE SELECT ON TABLES FROM dprest_lecture" in sql.replace("\n", " ").replace("  ", " ") or (
        re.search(r"ALTER DEFAULT PRIVILEGES[^;]*REVOKE SELECT ON TABLES FROM dprest_lecture", sql, flags=re.DOTALL)
    )


@pytest.mark.parametrize("path", [FLINK_SQL, ANALYTICS_SQL, REPLICA_SQL])
def test_no_secret_in_versioned_files(path: Path) -> None:
    """Seuls les jetons __XXX__ sont autorisés : jamais de mot de passe en clair."""
    texte = _read(path)
    for match in re.finditer(r"password\s*=\s*['\"]([^'\"]*)['\"]", texte, flags=re.IGNORECASE):
        valeur = match.group(1)
        assert re.fullmatch(r"__[A-Z_]+__", valeur), f"mot de passe en clair dans {path.name}"
    assert not re.search(r"PASSWORD\s+'[^']+'\s*;", texte), f"CREATE/ALTER ROLE avec mot de passe littéral dans {path.name}"
