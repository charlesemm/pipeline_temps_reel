"""Tests d'integration de la securite (etapes 7b a 7e) contre la stack en marche.

Necessite la stack demarree (scripts/start-stack.ps1) ; sinon les tests sont
ignores. Les identifiants sont lus dans .env (jamais dans le code).
Lancer : pytest tests/test_securite.py -v
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
ANALYTICS = "pipeline_temps_reel-postgres-analytics-1"
KAFKA = "pipeline_temps_reel-kafka-1"


def _env(name: str) -> str:
    """Lit une variable de .env."""
    for line in (ROOT / ".env").read_text(encoding="utf-8-sig").splitlines():
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].strip()
    raise KeyError(name)


def _run(*args: str, stdin: str | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["podman", *args],
        capture_output=True,
        text=True,
        input=stdin,
        timeout=120,
        check=False,
    )


def _container_up(name: str) -> bool:
    try:
        return name in _run("ps", "--format", "{{.Names}}").stdout
    except (OSError, subprocess.TimeoutExpired):
        return False


pytestmark = pytest.mark.skipif(
    not _container_up(ANALYTICS), reason="stack non demarree"
)


def _sql(user: str, password_var: str, query: str) -> subprocess.CompletedProcess[str]:
    """Execute une requete SQL sur la base analytique via TCP (authentifie)."""
    host = _run("exec", ANALYTICS, "hostname", "-i").stdout.split()[0]
    return _run(
        "exec", "-e", f"PGPASSWORD={_env(password_var)}", ANALYTICS,
        "psql", "-h", host, "-U", user, "-d", "dprest_analytics", "-At", "-c", query,
    )


def test_dprest_lecture_reads_kpi() -> None:
    result = _sql("dprest_lecture", "ANALYTICS_RO_PASSWORD", "SELECT count(*) FROM kpi_prestations_jour;")
    assert result.returncode == 0


def test_dprest_lecture_cannot_read_nominative_column() -> None:
    result = _sql("dprest_lecture", "ANALYTICS_RO_PASSWORD", "SELECT donnee_brute FROM qualite_anomalies;")
    assert result.returncode != 0
    assert "permission denied" in result.stderr


def test_dprest_lecture_cannot_delete() -> None:
    result = _sql(
        "dprest_lecture", "ANALYTICS_RO_PASSWORD",
        "DELETE FROM kpi_prestations_jour WHERE jour < '2000-01-01';",
    )
    assert "permission denied" in result.stderr


def test_sgd_qualite_reads_nominative_column() -> None:
    result = _sql("sgd_qualite", "SGD_QUALITE_PASSWORD", "SELECT count(donnee_brute) FROM qualite_anomalies;")
    assert result.returncode == 0


def test_flink_writer_cannot_alter_schema() -> None:
    result = _sql("flink_writer", "FLINK_WRITER_PASSWORD", "DROP TABLE dim_agents;")
    assert result.returncode != 0
    assert "must be owner" in result.stderr


def test_denied_access_is_audited() -> None:
    _sql("dprest_lecture", "ANALYTICS_RO_PASSWORD", "DELETE FROM kpi_prestations_jour WHERE jour < '2000-01-01';")
    logs = _run("logs", "--tail", "200", ANALYTICS)
    text = logs.stdout + logs.stderr
    assert "user=dprest_lecture" in text
    assert "permission denied for table kpi_prestations_jour" in text


def _kafka_props(user: str, password_var: str) -> None:
    jaas = (
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{user}" password="{_env(password_var)}";'
    )
    props = f"security.protocol=SASL_PLAINTEXT\nsasl.mechanism=SCRAM-SHA-512\nsasl.jaas.config={jaas}\n"
    _run("exec", "-i", KAFKA, "sh", "-c", "cat > /tmp/test.props", stdin=props)


@pytest.mark.skipif(not _container_up(KAFKA), reason="kafka non demarre")
def test_kafka_rejects_anonymous_client() -> None:
    result = _run(
        "exec", KAFKA, "timeout", "40", "/opt/kafka/bin/kafka-broker-api-versions.sh",
        "--bootstrap-server", "kafka:9092",
    )
    assert result.returncode != 0


@pytest.mark.skipif(not _container_up(KAFKA), reason="kafka non demarre")
def test_kafka_flink_user_is_read_only() -> None:
    _kafka_props("flink", "KAFKA_FLINK_PASSWORD")
    try:
        result = _run(
            "exec", "-i", KAFKA, "/opt/kafka/bin/kafka-console-producer.sh",
            "--bootstrap-server", "kafka:9092", "--producer.config", "/tmp/test.props",
            "--topic", "dprest-json.public.TB_FACTURES", stdin="x\n",
        )
        assert "not authorized" in (result.stdout + result.stderr).lower() or "authorization failed" in (
            result.stdout + result.stderr
        ).lower()
    finally:
        _run("exec", KAFKA, "rm", "-f", "/tmp/test.props")
