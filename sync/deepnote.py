import os
import posixpath
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEEPNOTE_API = "https://api.deepnote.com/v2"

# Mozilla workspace. Workgroup workspaces hold restricted data and are not ingested.
DEFAULT_WORKSPACE_ID = "5730346c-9297-42b0-80b0-35611206a1d7"

METADATA_FILE = "metadata.yaml"
PROD_TIER = "prod"

REQUEST_TIMEOUT = 30
PROJECT_PAGE_SIZE = 100
FETCH_WORKERS = 8


class DeepnoteFetchError(Exception):
    pass


class DeepnoteWorkspaceError(Exception):
    pass


@dataclass
class DeepnoteApp:
    id: str
    project_id: str
    project_name: str
    entrypoint: str
    url: str
    created_at: Optional[str]
    folder_path: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    metadata_error: Optional[str] = None

    @property
    def key(self) -> str:
        # The app id changes when a file is re-served.
        return f"{self.project_id}/{self.entrypoint}"

    def metadata_value(self, *path: str, placeholders: bool = False) -> Optional[str]:
        value: Any = self.metadata or {}
        for name in path:
            value = value.get(name) if isinstance(value, dict) else None
        if value is None or isinstance(value, (dict, list)):
            return None
        text = str(value).strip()
        # "{{" is an unfilled placeholder from the skill's template.
        if not text or text.lower() in ("null", "none"):
            return None
        if "{{" in text and not placeholders:
            return None
        return text

    @property
    def title(self) -> str:
        return self.metadata_value("title") or self.project_name

    @property
    def tier(self) -> Optional[str]:
        tier = self.metadata_value("tier")
        return tier.lower() if tier else None

    @property
    def sensitivity(self) -> Optional[str]:
        sensitivity = self.metadata_value("sensitivity")
        return sensitivity.lower() if sensitivity else None

    @property
    def sql(self) -> Optional[str]:
        return self.metadata_value("query", "sql", placeholders=True)

    @property
    def owner_email(self) -> Optional[str]:
        owner = self.metadata_value("owner")
        return owner if owner and "@" in owner else None

    @property
    def metric_term(self) -> Optional[str]:
        if self.metadata_value("grounding", "source") != "metric_hub":
            return None
        platform = self.metadata_value("grounding", "platform")
        metric = self.metadata_value("grounding", "metric_name")
        if platform and metric:
            return f"Metric Hub.{platform}.{metric}"
        return None


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
    retry = Retry(total=3, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _request(
    session: requests.Session, endpoint: str, params: dict
) -> Optional[requests.Response]:
    url = f"{DEEPNOTE_API}{endpoint}"
    try:
        response = session.get(url, params=params or None, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as error:
        raise DeepnoteFetchError(f"{endpoint}: {error}")
    if response.status_code == 404:
        return None
    if not response.ok:
        raise DeepnoteFetchError(f"{endpoint}: HTTP {response.status_code}")
    return response


def _get(session: requests.Session, endpoint: str, **params) -> Optional[dict]:
    response = _request(session, endpoint, params)
    if response is None:
        return None
    try:
        return response.json()
    except ValueError as error:
        raise DeepnoteFetchError(f"{endpoint}: response was not JSON: {error}")


def _get_text(session: requests.Session, endpoint: str, **params) -> Optional[str]:
    response = _request(session, endpoint, params)
    return None if response is None else response.text


def check_workspace(session: requests.Session) -> None:
    body = _get(session, "/me") or {}
    workspace = body.get("workspace")
    workspace_id = workspace.get("id") if isinstance(workspace, dict) else None
    if workspace_id != DEFAULT_WORKSPACE_ID:
        raise DeepnoteWorkspaceError(
            f"the API key is for workspace {workspace_id!r}, not the default "
            f"Mozilla workspace {DEFAULT_WORKSPACE_ID}. Restricted workspaces are "
            "not ingested."
        )


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
    body = _get(session, "/streamlit-apps", projectId=project_id) or {}
    return body.get("streamlitApps") or []


def folder_path(project: dict) -> Optional[str]:
    folder = project.get("folder")
    if not isinstance(folder, dict):
        return None
    path = folder.get("path")
    if isinstance(path, list):
        names = [p.get("name") for p in path if isinstance(p, dict) and p.get("name")]
        if names:
            return "/".join(names)
    return folder.get("name")


def parse_metadata(text: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        parsed = yaml.safe_load(text)
    except yaml.YAMLError as error:
        return None, f"not valid YAML: {error}"
    if not isinstance(parsed, dict):
        return None, "not a YAML mapping"
    return parsed, None


def metadata_path(entrypoint: str) -> str:
    return posixpath.join(posixpath.dirname(entrypoint), METADATA_FILE)


def get_deepnote_apps(
    api_key: Optional[str] = None,
) -> Tuple[List[DeepnoteApp], List[DeepnoteFetchError]]:
    session = _session(get_api_key(api_key))
    check_workspace(session)
    projects = list_projects(session)

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
                if not raw.get("entrypoint"):
                    continue
                apps.append(
                    DeepnoteApp(
                        id=raw["id"],
                        project_id=project["id"],
                        project_name=project.get("name") or project["id"],
                        entrypoint=raw["entrypoint"],
                        url=raw.get("url") or "",
                        created_at=raw.get("createdAt"),
                        folder_path=folder_path(project),
                    )
                )

    def read_metadata(app: DeepnoteApp) -> Optional[DeepnoteFetchError]:
        try:
            text = _get_text(
                session,
                "/files/download",
                projectId=app.project_id,
                path=metadata_path(app.entrypoint),
            )
        except DeepnoteFetchError as error:
            return error
        if text is not None:
            app.metadata, app.metadata_error = parse_metadata(text)
        return None

    complete: List[DeepnoteApp] = []
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
        for app, error in zip(apps, pool.map(read_metadata, apps)):
            if error is None:
                complete.append(app)
            else:
                errors.append(error)

    return complete, errors
