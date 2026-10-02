"""Tests d'integration de la securite (etapes 7b a 7e) contre la stack en marche.

Necessite la stack demarree (scripts/start-stack.ps1) ; sinon les tests sont
ignores. Les identifiants sont lus dans .env (jamais dans le code).
Lancer : pytest tests/test_securite.py -v
"""

from __future__ import annotations

import subprocess
import urllib.error
import urllib.request
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
ANALYTICS = "pipeline_temps_reel-postgres-analytics-1"
KAFKA = "pipeline_temps_reel-kafka-1"
FLINK_PROXY = "pipeline_temps_reel-flink-proxy-1"
AKHQ = "pipeline_temps_reel-akhq-1"


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


def test_sgd_admin_reads_nominative_column() -> None:
    result = _sql("sgd_admin", "SGD_ADMIN_DB_PASSWORD", "SELECT count(donnee_brute) FROM qualite_anomalies;")
    assert result.returncode == 0


@pytest.mark.parametrize("role", ["sgd_qualite", "dprest_lecteur"])
def test_removed_login_role_is_absent(role: str) -> None:
    """Comptes supprimés le 2026-10-02 : ils ne doivent pas réapparaître."""
    result = _sql("sgd_admin", "SGD_ADMIN_DB_PASSWORD", f"SELECT count(*) FROM pg_roles WHERE rolname = '{role}';")
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "0"


DETAIL_VIEWS = {
    "prestation": "v_anomalies_prestation",
    "facture": "v_anomalies_facture",
    "agent": "v_anomalies_agent",
    "assure": "v_anomalies_assure",
    "entente_prealable": "v_anomalies_entente_prealable",
}


@pytest.mark.parametrize("view", DETAIL_VIEWS.values())
def test_dprest_lecture_cannot_read_detail_view(view: str) -> None:
    result = _sql("dprest_lecture", "ANALYTICS_RO_PASSWORD", f"SELECT count(*) FROM {view};")
    assert result.returncode != 0
    assert "permission denied" in result.stderr


@pytest.mark.parametrize(("domaine", "view"), DETAIL_VIEWS.items())
def test_detail_view_matches_quarantine(domaine: str, view: str) -> None:
    """Chaque ligne de la quarantaine se retrouve dans sa vue, colonnes converties sans erreur."""
    query = (
        f"SELECT (SELECT count(row_to_json(v)) FROM {view} AS v)"
        f" = (SELECT count(*) FROM qualite_anomalies WHERE domaine = '{domaine}');"
    )
    result = _sql("sgd_admin", "SGD_ADMIN_DB_PASSWORD", query)
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "t"


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


SUPERSET_DB = "pipeline_temps_reel-superset-db-1"


def _superset_sql(query: str) -> str:
    """Interroge la base de metadonnees de Superset (lecture)."""
    return _run("exec", SUPERSET_DB, "psql", "-U", "superset", "-d", "superset", "-At", "-c", query).stdout.strip()


def test_dprest_dashboards_use_dprest_connection() -> None:
    """Correctif du 2026-09-24 : aucun graphique publie pour la DPREST ne passe par la connexion nominative du SGD."""
    nominatifs = _superset_sql(
        "SELECT COUNT(*) FROM dashboards d "
        "JOIN dashboard_slices ds ON ds.dashboard_id = d.id "
        "JOIN slices s ON s.id = ds.slice_id "
        "JOIN tables t ON t.id = s.datasource_id AND s.datasource_type = 'table' "
        "JOIN dbs b ON b.id = t.database_id "
        "WHERE d.published AND b.sqlalchemy_uri NOT LIKE '%//dprest_lecture:%'"
    )
    assert nominatifs == "0"


@pytest.mark.skipif(not _container_up(FLINK_PROXY), reason="flink-proxy non demarre")
def test_flink_dashboard_requires_auth() -> None:
    """Etape 7f : le Dashboard Flink n'est plus joignable sans mot de passe."""
    try:
        urllib.request.urlopen("http://localhost:8082/overview", timeout=10)
        pytest.fail("le Dashboard Flink a repondu sans authentification")
    except urllib.error.HTTPError as exc:
        assert exc.code == 401


@pytest.mark.skipif(not _container_up(AKHQ), reason="akhq non demarre")
def test_akhq_requires_auth() -> None:
    """Etape 7f : AKHQ (contenu brut des topics Kafka) exige une connexion."""
    try:
        urllib.request.urlopen("http://localhost:8085/api/cluster", timeout=10)
        pytest.fail("AKHQ a repondu sans authentification")
    except urllib.error.HTTPError as exc:
        assert exc.code == 401


def test_dprest_reader_owns_nothing_in_superset() -> None:
    """Un proprietaire accede a l'objet quels que soient ses roles : le compte DPREST ne doit rien posseder."""
    possessions = _superset_sql(
        "SELECT (SELECT COUNT(*) FROM sqlatable_user o JOIN ab_user u ON u.id = o.user_id WHERE u.username = 'dprest_lecteur')"
        " + (SELECT COUNT(*) FROM slice_user o JOIN ab_user u ON u.id = o.user_id WHERE u.username = 'dprest_lecteur')"
        " + (SELECT COUNT(*) FROM dashboard_user o JOIN ab_user u ON u.id = o.user_id WHERE u.username = 'dprest_lecteur')"
    )
    assert possessions == "0"
