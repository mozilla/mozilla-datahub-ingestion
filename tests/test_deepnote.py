from dataclasses import dataclass, field
from typing import Optional, Union
from unittest.mock import patch

import pytest
import requests

from sync.deepnote import (
    DeepnoteFetchError,
    find_tables,
    get_api_key,
    get_deepnote_apps,
)

# Shapes follow https://api.deepnote.com/v2/openapi.json
PROJECTS = {
    "projects": [
        {
            "id": "p-retention",
            "name": "Fenix Retention",
            "projectType": "notebook",
            "createdAt": "2026-01-05T10:00:00.000Z",
            "updatedAt": "2026-09-01T10:00:00.000Z",
            "folder": {"id": "f1", "name": "Data Science", "path": "/Data Science"},
        },
        {
            "id": "p-notes",
            "name": "Scratch notes",
            "projectType": "notebook",
            "createdAt": "2026-02-05T10:00:00.000Z",
        },
    ],
    "pagination": {"pageSize": 100, "hasMore": False},
}

APPS = {
    "p-retention": {
        "streamlitApps": [
            {
                "id": "app-1",
                "projectId": "p-retention",
                "entrypoint": "streamlit_app.py",
                "url": "https://app-1.deepnoteapp.com/",
                "createdAt": "2026-03-01T12:00:00.000Z",
            }
        ]
    },
    "p-notes": {"streamlitApps": []},
}

INTEGRATIONS = {
    "integrations": [
        {"id": "int-bq", "name": "BigQuery (Service Account)", "type": "big-query"},
        {"id": "int-slack", "name": "Slack", "type": "slack"},
    ]
}

BLOCK_USAGES = {
    "type": "block",
    "usages": [
        {
            "blockId": "b1",
            "blockType": "sql",
            "blockContent": "SELECT * FROM `mozdata.fenix.retention`",
            "notebookId": "n1",
            "notebookName": "Retention",
            "projectId": "p-retention",
            "projectName": "Fenix Retention",
        },
        {
            "blockId": "b2",
            "blockType": "sql",
            "blockContent": "SELECT 1",
            "notebookId": "n2",
            "notebookName": "Unrelated",
            "projectId": "p-other",
            "projectName": "Other",
        },
    ],
}

SERVED_FILE = """
import streamlit as st
df = client.query("SELECT * FROM `mozdata.telemetry.clients_daily`").to_dataframe()
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


def fake_get(url, params=None, timeout=None):
    params = params or {}
    if url.endswith("/projects"):
        return MockResponse(data=PROJECTS)
    if url.endswith("/streamlit-apps"):
        return MockResponse(data=APPS[params["projectId"]])
    if url.endswith("/integrations"):
        return MockResponse(data=INTEGRATIONS)
    if url.endswith("/integrations/int-bq/usages/block"):
        return MockResponse(data=BLOCK_USAGES)
    if url.endswith("/files/download"):
        return MockResponse(text=SERVED_FILE)
    if url.endswith("/status"):
        return MockResponse(data={"status": "unavailable"})
    return MockResponse(status=404)


def run(get=fake_get):
    with patch.object(requests.Session, "get", side_effect=get, autospec=False):
        return get_deepnote_apps(api_key="fake-key")


def test_get_deepnote_apps():
    apps, errors = run()

    assert errors == []
    assert [a.id for a in apps] == ["app-1"]

    app = apps[0]
    assert app.title == "Fenix Retention"
    assert app.project_id == "p-retention"
    assert app.url == "https://app-1.deepnoteapp.com/"
    assert app.folder_path == "/Data Science"
    assert app.status == "unavailable"
    # notebook SQL plus SQL inside the served file, and nothing from p-other
    assert app.bigquery_fully_qualified_names == [
        "mozdata.fenix.retention",
        "mozdata.telemetry.clients_daily",
    ]


def test_entrypoint_qualifies_the_title_when_it_is_not_the_default():
    apps = dict(APPS)
    apps["p-retention"] = {
        "streamlitApps": [
            dict(APPS["p-retention"]["streamlitApps"][0], entrypoint="cohorts.py")
        ]
    }

    def get(url, params=None, timeout=None):
        if url.endswith("/streamlit-apps"):
            return MockResponse(data=apps[params["projectId"]])
        return fake_get(url, params, timeout)

    found, errors = run(get)
    assert errors == []
    assert found[0].title == "Fenix Retention (cohorts.py)"


def test_a_failed_call_is_an_error_not_a_deletion():
    def flaky(url, params=None, timeout=None):
        if url.endswith("/streamlit-apps"):
            raise requests.ConnectTimeout("connection timed out")
        return fake_get(url, params, timeout)

    apps, errors = run(flaky)
    assert apps == []
    assert len(errors) == 2  # one per project
    assert all(isinstance(e, DeepnoteFetchError) for e in errors)


def test_a_server_error_is_an_error_not_a_deletion():
    def broken(url, params=None, timeout=None):
        if url.endswith("/streamlit-apps"):
            return MockResponse(status=502)
        return fake_get(url, params, timeout)

    apps, errors = run(broken)
    assert apps == []
    assert all("HTTP 502" in str(e) for e in errors)


def test_a_missing_entrypoint_file_does_not_fail_the_run():
    def no_file(url, params=None, timeout=None):
        if url.endswith("/files/download"):
            return MockResponse(status=404)
        return fake_get(url, params, timeout)

    apps, errors = run(no_file)
    assert errors == []
    # only the notebook SQL survives
    assert apps[0].bigquery_fully_qualified_names == ["mozdata.fenix.retention"]


def test_a_schema_change_fails_loudly():
    """The API is in preview. A non-JSON body must raise, not yield zero apps."""

    def html_body(url, params=None, timeout=None):
        if url.endswith("/projects"):
            return MockResponse(json_error=True)
        return fake_get(url, params, timeout)

    with pytest.raises(DeepnoteFetchError):
        run(html_body)


def test_get_api_key_requires_one(monkeypatch):
    monkeypatch.delenv("DEEPNOTE_API_KEY", raising=False)
    with pytest.raises(DeepnoteFetchError):
        get_api_key()
    monkeypatch.setenv("DEEPNOTE_API_KEY", "from-env")
    assert get_api_key() == "from-env"


def test_find_tables_drops_runtime_names():
    complete, runtime = find_tables(
        "`mozdata.telemetry.clients_daily` and `mozdata.telemetry.main_${x}`"
    )
    assert complete == ["mozdata.telemetry.clients_daily"]
    assert runtime == ["mozdata.telemetry.main_"]
