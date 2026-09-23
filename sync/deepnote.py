import os
import re
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Tuple

import requests

DEEPNOTE_API = "https://api.deepnote.com/v2"
BIGQUERY_INTEGRATION = "big-query"
DEFAULT_ENTRYPOINT = "streamlit_app.py"

REQUEST_TIMEOUT = 30
PROJECT_PAGE_SIZE = 100
FETCH_WORKERS = 8

# Same shape bigquery-etl uses. Duplicated from the Quick source on purpose;
# fold the two together once both are on main.
TABLE_RE = re.compile(
    r"(?<![A-Za-z0-9_.\-])"
    r"((?:mozdata|moz-fx-[a-z0-9\-]+)\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+)"
    r"(?![A-Za-z0-9_.])"
    r"(?=(.{0,4}))",
    re.DOTALL,
)
# A name cut short where interpolation, concatenation or a LIKE wildcard starts
# is a prefix, not a table. Those URNs do not resolve.
RUNTIME_REF_RE = re.compile(r"""^(?:\$\{|\{\{|%|["'`]\s*\+|\s*\+)""")


class DeepnoteFetchError(Exception):
    """A Deepnote API call failed.

    Distinct from a 404, which means the resource is absent. A run that hits one
    of these is incomplete and must not soft-delete anything.
    """


@dataclass
class DeepnoteApp:
    id: str
    project_id: str
    project_name: str
    entrypoint: str
    url: str
    created_at: Optional[str]
    folder_path: Optional[str] = None
    status: Optional[str] = None
    tables: Sequence[str] = field(default_factory=list)
    runtime_table_refs: Sequence[str] = field(default_factory=list)
    siblings: int = 1

    @property
    def title(self) -> str:
        """Project name, qualified by entrypoint only when it disambiguates."""
        if self.siblings > 1 or self.entrypoint != DEFAULT_ENTRYPOINT:
            return f"{self.project_name} ({self.entrypoint})"
        return self.project_name

    @property
    def bigquery_fully_qualified_names(self) -> Sequence[str]:
        return list(self.tables)


def get_api_key(api_key: Optional[str] = None) -> str:
    key = api_key or os.environ.get("DEEPNOTE_API_KEY")
    if not key:
        raise DeepnoteFetchError(
            "no Deepnote API key: set api_key in the recipe or DEEPNOTE_API_KEY"
        )
    return key


def _session(api_key: str) -> requests.Session:
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {api_key}"})
    return session


def _get(session: requests.Session, endpoint: str, **params) -> Optional[dict]:
    """Parsed body, or None on 404. Anything else raises.

    The endpoint is named `endpoint`, not `path`, because /files/download takes a
    query parameter called `path`.
    """
    url = f"{DEEPNOTE_API}{endpoint}"
    try:
        response = session.get(url, params=params or None, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as error:
        raise DeepnoteFetchError(f"{endpoint}: {error}")
    if response.status_code == 404:
        return None
    if not response.ok:
        raise DeepnoteFetchError(f"{endpoint}: HTTP {response.status_code}")
    try:
        return response.json()
    except ValueError as error:
        raise DeepnoteFetchError(f"{endpoint}: response was not JSON: {error}")


def _get_text(session: requests.Session, endpoint: str, **params) -> Optional[str]:
    url = f"{DEEPNOTE_API}{endpoint}"
    try:
        response = session.get(url, params=params or None, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as error:
        raise DeepnoteFetchError(f"{endpoint}: {error}")
    if response.status_code == 404:
        return None
    if not response.ok:
        raise DeepnoteFetchError(f"{endpoint}: HTTP {response.status_code}")
    return response.text


def list_projects(session: requests.Session) -> List[dict]:
    projects: List[dict] = []
    page_token = None
    while True:
        params = {"pageSize": PROJECT_PAGE_SIZE}
        if page_token:
            params["pageToken"] = page_token
        body = _get(session, "/projects", **params) or {}
        projects.extend(body.get("projects") or [])
        pagination = body.get("pagination") or {}
        page_token = pagination.get("nextPageToken")
        if not pagination.get("hasMore") or not page_token:
            return projects


def list_streamlit_apps(session: requests.Session, project_id: str) -> List[dict]:
    """The API has no workspace-wide list, so this is called once per project."""
    body = _get(session, "/streamlit-apps", projectId=project_id) or {}
    return body.get("streamlitApps") or []


def get_app_status(session: requests.Session, app_id: str) -> Optional[str]:
    body = _get(session, f"/streamlit-apps/{app_id}/status") or {}
    return body.get("status")


def bigquery_sql_by_project(session: requests.Session) -> Dict[str, List[str]]:
    """SQL block contents for every BigQuery integration, keyed by project.

    One call per integration covers the whole workspace, which is much cheaper
    than walking every project's notebooks.
    """
    body = _get(session, "/integrations") or {}
    integrations = [
        i
        for i in (body.get("integrations") or [])
        if i.get("type") == BIGQUERY_INTEGRATION
    ]
    sql: Dict[str, List[str]] = {}
    for integration in integrations:
        usages = (
            _get(session, f"/integrations/{integration['id']}/usages/block") or {}
        ).get("usages") or []
        for usage in usages:
            project_id = usage.get("projectId")
            content = usage.get("blockContent")
            if project_id and content:
                sql.setdefault(project_id, []).append(content)
    return sql


def find_tables(text: str) -> Tuple[List[str], List[str]]:
    """Return (complete, runtime) table names. Seen complete once wins."""
    complete, runtime = set(), set()
    for match in TABLE_RE.finditer(text):
        name, tail = match.group(1), match.group(2)
        if name.endswith("_") or RUNTIME_REF_RE.match(tail):
            runtime.add(name)
        else:
            complete.add(name)
    return sorted(complete), sorted(runtime - complete)


def get_deepnote_apps(
    api_key: Optional[str] = None,
    with_status: bool = True,
) -> Tuple[List[DeepnoteApp], List[DeepnoteFetchError]]:
    """Every Streamlit app in the workspace, and the calls that failed.

    Callers must treat a non-empty error list as an incomplete run.
    """
    session = _session(get_api_key(api_key))
    projects = list_projects(session)
    sql_by_project = bigquery_sql_by_project(session)

    def collect(project: dict):
        try:
            return list_streamlit_apps(session, project["id"]), project, None
        except DeepnoteFetchError as error:
            return [], project, error

    apps: List[DeepnoteApp] = []
    errors: List[DeepnoteFetchError] = []
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
        for raw_apps, project, error in pool.map(collect, projects):
            if error is not None:
                errors.append(error)
                continue
            for raw in raw_apps:
                apps.append(
                    DeepnoteApp(
                        id=raw["id"],
                        project_id=project["id"],
                        project_name=project.get("name") or project["id"],
                        entrypoint=raw.get("entrypoint") or DEFAULT_ENTRYPOINT,
                        url=raw.get("url") or "",
                        created_at=raw.get("createdAt"),
                        folder_path=(project.get("folder") or {}).get("path"),
                        siblings=len(raw_apps),
                    )
                )

    for app in apps:
        # The notebook SQL plus whatever the served file itself queries.
        sources = list(sql_by_project.get(app.project_id) or [])
        try:
            served = _get_text(
                session,
                "/files/download",
                projectId=app.project_id,
                path=app.entrypoint,
            )
            if served:
                sources.append(served)
            if with_status:
                app.status = get_app_status(session, app.id)
        except DeepnoteFetchError as error:
            errors.append(error)
            continue
        app.tables, app.runtime_table_refs = find_tables("\n".join(sources))

    return apps, errors
