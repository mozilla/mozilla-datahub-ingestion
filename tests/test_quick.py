from dataclasses import dataclass
from typing import Optional, Union
from unittest.mock import patch

import requests
from datahub.ingestion.api.common import PipelineContext

from sync.datahub.quick_source import QuickSource, QuickSourceConfig
from sync.quick import (
    QuickFetchError,
    find_tables,
    get_quick_dashboards,
    parse_description,
    parse_title,
)

DASHBOARD_HTML = """
<html><head>
<title>Fenix Retention</title>
<meta name="description" content="Retention for Firefox for Android.">
<script src="/app.js?v=2"></script>
<script src="/quick-nav.js"></script>
</head><body></body></html>
"""

DASHBOARD_APP_JS = """
const sql = 'SELECT * FROM `mozdata.fenix.retention`';
quick.query(sql);
"""

GAME_HTML = """
<html><head>
<title>Flappy Kit</title>
<meta content="A game." name="description">
</head><body><script>quick.bq('scores', rows)</script></body></html>
"""


@dataclass
class MockApiResponse:
    data: Optional[Union[dict, list]] = None
    text: str = ""
    status: int = 200

    @property
    def status_code(self):
        return self.status

    @property
    def ok(self):
        return self.status < 400

    def json(self):
        return self.data

    def raise_for_status(self):
        if not self.ok:
            raise requests.HTTPError(f"status {self.status}")


def fake_get(url, headers=None, timeout=None):
    if url.endswith("/api/sites?detail=1"):
        return MockApiResponse(
            data={
                "sites": ["fenix-retention", "flappy-kit", "tenant-drift"],
                "updated": {"fenix-retention": 1700000000000, "flappy-kit": 1},
                "deployers": {"fenix-retention": "someone@mozilla.com"},
            }
        )
    if url.startswith("https://fenix-retention."):
        if url.endswith("/app.js"):
            return MockApiResponse(text=DASHBOARD_APP_JS)
        return MockApiResponse(text=DASHBOARD_HTML)
    if url.startswith("https://flappy-kit."):
        return MockApiResponse(text=GAME_HTML)
    # tenant-drift has no index.html
    return MockApiResponse(status=404)


@patch("requests.get", side_effect=fake_get)
def test_get_quick_dashboards(mock_get):
    dashboards, errors = get_quick_dashboards(token="fake-token")

    # flappy-kit only writes app storage, tenant-drift has no index.html
    assert [d.name for d in dashboards] == ["fenix-retention"]
    assert errors == []

    dashboard = dashboards[0]
    assert dashboard.title == "Fenix Retention"
    assert dashboard.description == "Retention for Firefox for Android."
    assert dashboard.deployed_by == "someone@mozilla.com"
    assert dashboard.updated == 1700000000000
    assert dashboard.url == "https://fenix-retention.quick.mozilla.cloud/"
    assert dashboard.scripts == ["/app.js"]
    assert dashboard.bigquery_fully_qualified_names == ["mozdata.fenix.retention"]


def test_parse_title_and_description():
    assert parse_title(GAME_HTML) == "Flappy Kit"
    # name= after content= still resolves
    assert parse_description(GAME_HTML) == "A game."
    assert parse_description("<html><head></head></html>") is None


def test_find_tables_keeps_complete_names():
    complete, runtime = find_tables("FROM `mozdata.telemetry.clients_daily`")
    assert complete == ["mozdata.telemetry.clients_daily"]
    assert runtime == []


def test_find_tables_drops_runtime_names():
    text = """
    `moz-fx-data-shared-prod.monitoring_derived.airflow_${kind}_v1`
    'moz-fx-data-shared-prod.telemetry_stable.main_v' + n
    task_id LIKE 'moz-fx-data-shared-prod.telemetry_stable.main_v%'
    """
    complete, runtime = find_tables(text)
    assert complete == []
    assert runtime == [
        "moz-fx-data-shared-prod.monitoring_derived.airflow_",
        "moz-fx-data-shared-prod.telemetry_stable.main_v",
    ]


def test_find_tables_prefers_a_name_seen_complete():
    text = "`mozdata.monitoring.airflow_dag` and `mozdata.monitoring.airflow_${x}`"
    complete, runtime = find_tables(text)
    assert complete == ["mozdata.monitoring.airflow_dag"]
    assert runtime == ["mozdata.monitoring.airflow_"]


def test_a_site_that_times_out_is_an_error_not_a_deletion():
    def flaky_get(url, headers=None, timeout=None):
        if url.startswith("https://fenix-retention."):
            raise requests.ConnectTimeout("connection timed out")
        return fake_get(url, headers, timeout)

    with patch("requests.get", side_effect=flaky_get):
        dashboards, errors = get_quick_dashboards(token="fake-token")

    assert dashboards == []
    assert len(errors) == 1
    assert isinstance(errors[0], QuickFetchError)
    assert "fenix-retention" in str(errors[0])


def test_a_server_error_is_an_error_not_a_deletion():
    def broken_get(url, headers=None, timeout=None):
        if url.startswith("https://fenix-retention."):
            return MockApiResponse(status=502)
        return fake_get(url, headers, timeout)

    with patch("requests.get", side_effect=broken_get):
        dashboards, errors = get_quick_dashboards(token="fake-token")

    assert dashboards == []
    assert [("HTTP 502" in str(e)) for e in errors] == [True]


def test_a_missing_script_does_not_fail_the_run():
    def missing_script(url, headers=None, timeout=None):
        if url.endswith("/app.js"):
            return MockApiResponse(status=404)
        return fake_get(url, headers, timeout)

    with patch("requests.get", side_effect=missing_script):
        dashboards, errors = get_quick_dashboards(token="fake-token")

    # the page itself names no table, so without app.js it is not a dashboard
    assert dashboards == []
    assert errors == []


def test_a_fetch_error_blocks_stale_entity_removal():
    """report.failures is what StaleEntityRemovalHandler checks before soft-deleting."""
    ctx = PipelineContext(run_id="test", pipeline_name="quick_ingestion_pipeline")
    source = QuickSource(QuickSourceConfig(), ctx)
    error = QuickFetchError("https://fenix-retention.quick.mozilla.cloud/: timed out")

    with patch.object(QuickSource, "_token", return_value="fake-token"), patch(
        "sync.datahub.quick_source.get_quick_dashboards", return_value=([], [error])
    ):
        assert list(source.get_workunits_internal()) == []

    assert len(source.get_report().failures) == 1
