import json
from dataclasses import dataclass, field
from typing import Optional, Union
from unittest.mock import MagicMock, patch

import pytest
import requests
from datahub.ingestion.api.common import PipelineContext

from sync.datahub.deepnote_source import DeepnoteSource
from sync.deepnote import (
    DEFAULT_WORKSPACE_ID,
    DeepnoteApp,
    DeepnoteFetchError,
    DeepnoteWorkspaceError,
    get_deepnote_apps,
    parse_metadata,
)

# Shapes follow https://api.deepnote.com/v2/openapi.json
ME = {
    "apiKey": {"id": "k1"},
    "user": {"id": "u1"},
    "workspace": {"id": DEFAULT_WORKSPACE_ID, "name": "Mozilla"},
    "accessLevel": "viewer",
}

PROJECTS = {
    "projects": [
        {
            "id": "p-dau",
            "name": "Desktop DAU",
            "projectType": "notebook",
            "folder": {
                "id": "f1",
                "name": "efilho",
                "path": [
                    {"id": "f0", "name": "User Folders"},
                    {"id": "f1", "name": "efilho"},
                ],
                "isPathComplete": True,
            },
        },
        {
            "id": "p-notes",
            "name": "Scratch notes",
            "projectType": "notebook",
        },
    ],
    "pagination": {"pageSize": 100, "hasMore": False},
}

APPS = {
    "p-dau": {
        "streamlitApps": [
            {
                "id": "app-1",
                "projectId": "p-dau",
                "entrypoint": "app.py",
                "url": "https://app-1.deepnoteapp.com/",
                "createdAt": "2026-10-20T12:00:00.000Z",
            }
        ]
    },
    "p-notes": {"streamlitApps": []},
}

# Filled in the way the dashboard skill fills its metadata.yaml template.
SKILL_METADATA = """
tier: prod
generated_by: dashboard-skill
target: deepnote
title: "Desktop DAU by country"
owner: "ascholtz@mozilla.com"
created_at: "2026-10-20"
sensitivity: "public"
grounding:
  source: "metric_hub"
  metric_name: "daily_active_users_v2"
  datahub_urn: null
  platform: "firefox_desktop"
  notes: null
query:
  integration: "mozdata"
  sql: |
    SELECT submission_date, country, SUM(dau) AS dau
    FROM telemetry.active_users_aggregates
    WHERE app_name = 'Firefox Desktop'
    GROUP BY 1, 2
drift_check:
  status: "equivalent"
  last_checked: "2026-10-20"
promotion:
  eval_battery: passed
  promoted_at: "2026-10-22"
review_status: reviewed
"""


@dataclass
class MockResponse:
    data: Optional[Union[dict, list]] = None
    text: str = ""
    status: int = 200
    json_error: bool = field(default=False)

    @property
    def status_code(self):
        return self.status

    @property
    def ok(self):
        return self.status < 400

    def json(self):
        if self.json_error:
            raise ValueError("not json")
        return self.data


def api(files=None, me=ME, apps=APPS):
    files = {("p-dau", "metadata.yaml"): SKILL_METADATA} if files is None else files

    def get(url, params=None, timeout=None):
        params = params or {}
        if url.endswith("/me"):
            return MockResponse(data=me)
        if url.endswith("/projects"):
            return MockResponse(data=PROJECTS)
        if url.endswith("/streamlit-apps"):
            return MockResponse(data=apps[params["projectId"]])
        if url.endswith("/files/download"):
            text = files.get((params["projectId"], params["path"]))
            return MockResponse(text=text) if text is not None else MockResponse(404)
        return MockResponse(status=404)

    return get


def run(get=None):
    with patch.object(
        requests.Session, "get", side_effect=get or api(), autospec=False
    ):
        return get_deepnote_apps(api_key="fake-key")


def test_get_deepnote_apps():
    apps, errors = run()

    assert errors == []
    assert [a.key for a in apps] == ["p-dau/app.py"]
    assert apps[0].tier == "prod"
    assert apps[0].folder_path == "User Folders/efilho"


def test_metadata_is_read_from_the_entrypoint_folder():
    apps = {
        "p-dau": {
            "streamlitApps": [
                dict(APPS["p-dau"]["streamlitApps"][0], entrypoint="apps/app.py")
            ]
        },
        "p-notes": {"streamlitApps": []},
    }
    found, errors = run(
        api(files={("p-dau", "apps/metadata.yaml"): SKILL_METADATA}, apps=apps)
    )
    assert errors == []
    assert found[0].key == "p-dau/apps/app.py"
    assert found[0].title == "Desktop DAU by country"


@pytest.mark.parametrize(
    "me",
    [
        dict(ME, workspace={"id": "dc93065f-4228-46f9-a0de-aac91ea7d9f5"}),  # Ads
        dict(ME, workspace={"name": "Mozilla"}),  # no id: can't tell, so refuse
    ],
)
def test_keys_for_other_workspaces_are_refused(me):
    with pytest.raises(DeepnoteWorkspaceError):
        run(api(me=me))


@pytest.mark.parametrize(
    "failure", [requests.ConnectTimeout("timed out"), MockResponse(status=502)]
)
def test_a_failed_call_is_an_error_not_a_deletion(failure):
    base = api()

    def flaky(url, params=None, timeout=None):
        if url.endswith("/streamlit-apps"):
            if isinstance(failure, Exception):
                raise failure
            return failure
        return base(url, params, timeout)

    apps, errors = run(flaky)
    assert apps == []
    assert len(errors) == 2


def test_a_failed_metadata_read_leaves_the_app_out():
    base = api()

    def broken_files(url, params=None, timeout=None):
        if url.endswith("/files/download"):
            return MockResponse(status=500)
        return base(url, params, timeout)

    apps, errors = run(broken_files)
    assert apps == []
    assert len(errors) == 1


def test_parse_metadata():
    assert parse_metadata(SKILL_METADATA)[0]["tier"] == "prod"
    assert parse_metadata("name: my thing\n") == ({"name": "my thing"}, None)
    metadata, why = parse_metadata("tier: [unclosed")
    assert metadata is None and "YAML" in why
    assert parse_metadata("- a list") == (None, "not a YAML mapping")


def app_with(metadata=None, **overrides):
    app = DeepnoteApp(
        id="app-1",
        project_id="p1",
        project_name="Project",
        entrypoint="app.py",
        url="https://example.deepnoteapp.com/",
        created_at=None,
        metadata=metadata,
    )
    for name, value in overrides.items():
        setattr(app, name, value)
    return app


def test_metadata_values():
    app = app_with(
        {
            "title": "{{TITLE}}",
            "owner": "{{OWNER_EMAIL}}",
            "grounding": {
                "source": "metric_hub",
                "platform": "{{PLATFORM}}",
                "metric_name": "x",
            },
        }
    )
    assert app.title == "Project"
    assert app.owner_email is None
    assert app.metric_term is None
    assert app_with({"owner": "Data Science"}).owner_email is None
    assert app_with({"owner": ["a@mozilla.com"]}).owner_email is None
    assert app_with({"query": {"sql": "SELECT '{{x}}'"}}).sql == "SELECT '{{x}}'"
    grounding = {"source": "datahub", "platform": "fenix", "metric_name": "dau"}
    assert app_with({"grounding": grounding}).metric_term is None


def make_source(graph=None, **config):
    ctx = PipelineContext(run_id="test-deepnote")
    ctx.graph = graph
    return DeepnoteSource.create(config, ctx)


def emitted(source, apps, errors=()):
    with patch(
        "sync.datahub.deepnote_source.get_deepnote_apps",
        return_value=(apps, list(errors)),
    ):
        by_urn = {}
        for wu in source.get_workunits_internal():
            mcp = wu.metadata
            by_urn.setdefault(mcp.entityUrn, {})[mcp.aspectName] = (
                getattr(mcp, "aspect", None) or mcp
            )
        return by_urn


def skill_app(**overrides):
    metadata, _ = parse_metadata(SKILL_METADATA)
    metadata.update(overrides)
    return app_with(metadata, folder_path="Team dashboards/Growth")


URN = "urn:li:dashboard:(Deepnote,p1/app.py)"


def test_source_emits_a_prod_dashboard():
    aspects = emitted(make_source(), [skill_app()])[URN]
    info = aspects["dashboardInfo"]
    assert info.title == "Desktop DAU by country"
    assert info.dashboardUrl == "https://example.deepnoteapp.com/"
    assert [e.destinationUrn for e in info.datasetEdges] == [
        "urn:li:dataset:(urn:li:dataPlatform:bigquery,"
        "mozdata.telemetry.active_users_aggregates,PROD)"
    ]
    assert info.customProperties["grounding"] == "metric_hub"
    assert info.customProperties["promoted_at"] == "2026-10-22"
    assert info.customProperties["folder"] == "Team dashboards/Growth"
    assert aspects["browsePaths"].paths == ["/prod/deepnote/Team dashboards/Growth"]
    owners = json.loads(aspects["ownership"].value)["patch"]
    assert owners[0]["value"]["owner"] == "urn:li:corpuser:ascholtz@mozilla.com"
    terms = json.loads(aspects["glossaryTerms"].value)["patch"]
    assert terms[0]["value"]["urn"] == (
        "urn:li:glossaryTerm:Metric Hub.firefox_desktop.daily_active_users_v2"
    )


def test_source_reports_sql_it_cannot_parse():
    source = make_source()
    app = skill_app(query={"sql": "SELECT FROM WHERE((("})
    info = emitted(source, [app])[URN]["dashboardInfo"]
    assert info.datasetEdges == []
    assert source.report.without_lineage == ["p1/app.py"]


def test_source_skips_metric_terms_datahub_does_not_have():
    graph = MagicMock()
    graph.exists.return_value = False
    source = make_source(graph=graph)
    aspects = emitted(source, [skill_app()])[URN]
    assert "glossaryTerms" not in aspects
    assert len(source.report.unknown_metric_terms) == 1


def test_source_only_emits_prod_skill_dashboards():
    apps = [
        skill_app(),
        app_with(None, project_id="no-metadata"),
        app_with(parse_metadata(SKILL_METADATA)[0] | {"tier": "draft"}, project_id="d"),
        app_with(parse_metadata(SKILL_METADATA)[0] | {"tier": None}, project_id="t"),
        app_with(parse_metadata(SKILL_METADATA)[0] | {"tier": "Prod"}, project_id="p"),
        app_with(
            parse_metadata(SKILL_METADATA)[0] | {"sensitivity": "Restricted"},
            project_id="r",
        ),
    ]
    source = make_source()
    assert list(emitted(source, apps)) == [URN, "urn:li:dashboard:(Deepnote,p/app.py)"]
    assert source.report.skipped_without_metadata == 1
    assert source.report.dashboards_emitted == 2
    assert source.report.skipped_not_prod == 2
    assert source.report.skipped_restricted == ["r/app.py"]


def test_source_reports_api_and_metadata_errors_as_failures():
    source = make_source()
    unparseable = app_with(None, metadata_error="not valid YAML: ...")
    errors = [DeepnoteFetchError("/projects: HTTP 502")]
    assert emitted(source, [unparseable], errors) == {}
    assert len(source.report.failures) == 2
