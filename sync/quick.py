import json
import os
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import requests
from google.auth import impersonated_credentials
from google.auth.transport.requests import Request
from google.oauth2 import service_account

QUICK_DOMAIN = "quick.mozilla.cloud"
QUICK_PROJECT = "moz-fx-quick-prod"
# Values from the environments table in mozilla/quick cli/main.go.
QUICK_IAP_AUDIENCE = (
    "250441855759-2u6sq18cg5b729n2ag2310nt4cki1ieq.apps.googleusercontent.com"
)
QUICK_CLI_SERVICE_ACCOUNT = f"quick-cli@{QUICK_PROJECT}.iam.gserviceaccount.com"
CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"

REQUEST_TIMEOUT = 30
MAX_PAGE_BYTES = 2 * 1024 * 1024
MAX_SCRIPTS_PER_SITE = 12
FETCH_WORKERS = 16

TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)
META_RE = re.compile(r"<meta\b[^>]*>", re.IGNORECASE)
META_ATTR_RE = re.compile(
    r"""\b(name|content)\s*=\s*("([^"]*)"|'([^']*)'|([^\s>]+))""",
    re.IGNORECASE | re.DOTALL,
)
SCRIPT_SRC_RE = re.compile(
    r"""<script\b[^>]*\bsrc\s*=\s*["']([^"']+)["']""", re.IGNORECASE
)
# Group 2 is the few characters after the name, used to spot a runtime ref.
TABLE_RE = re.compile(
    r"(?<![A-Za-z0-9_.\-])"
    r"((?:mozdata|moz-fx-[a-z0-9\-]+)\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+)"
    r"(?![A-Za-z0-9_.])"
    r"(?=(.{0,4}))",
    re.DOTALL,
)
# quick.query reads the warehouse. quick.bq is per-site app storage in the
# quick_app dataset and does not make a page a dashboard.
QUICK_QUERY_RE = re.compile(r"\bquick\.query\b")
# A name cut short where interpolation, concatenation or a LIKE wildcard starts
# is a prefix, not a table. Those URNs do not resolve.
RUNTIME_REF_RE = re.compile(r"""^(?:\$\{|\{\{|%|["'`]\s*\+|\s*\+)""")


class QuickFetchError(Exception):
    """A Quick page could not be read.

    Distinct from a site having no index.html, which is a fact about the site.
    A run that hits one of these is incomplete and must not soft-delete anything.
    """


@dataclass
class QuickSite:
    name: str
    title: Optional[str]
    description: Optional[str]
    deployed_by: Optional[str]
    updated: Optional[int]
    tables: Sequence[str]
    runtime_table_refs: Sequence[str]
    scripts: Sequence[str]
    calls_query: bool

    @property
    def url(self) -> str:
        return f"https://{self.name}.{QUICK_DOMAIN}/"

    @property
    def is_dashboard(self) -> bool:
        return bool(self.tables) or self.calls_query

    @property
    def bigquery_fully_qualified_names(self) -> Sequence[str]:
        return list(self.tables)


def get_iap_token(
    service_account_key: Optional[str] = None,
    impersonate: Optional[str] = QUICK_CLI_SERVICE_ACCOUNT,
) -> str:
    """An IAP token for Quick. Good for one hour, so mint one per run.

    Uses the service account key if given, which is what scheduled ingestion
    does. Otherwise QUICK_IAP_TOKEN, otherwise gcloud for local runs.
    """
    if service_account_key:
        return _token_from_service_account(service_account_key, impersonate)
    token = os.environ.get("QUICK_IAP_TOKEN")
    if token:
        return token
    return _token_from_gcloud(impersonate)


def _token_from_service_account(key_json: str, impersonate: Optional[str]) -> str:
    info = json.loads(key_json)
    if impersonate:
        source = service_account.Credentials.from_service_account_info(
            info, scopes=[CLOUD_PLATFORM_SCOPE]
        )
        credentials = impersonated_credentials.IDTokenCredentials(
            impersonated_credentials.Credentials(
                source_credentials=source,
                target_principal=impersonate,
                target_scopes=[CLOUD_PLATFORM_SCOPE],
            ),
            target_audience=QUICK_IAP_AUDIENCE,
            include_email=True,
        )
    else:
        credentials = service_account.IDTokenCredentials.from_service_account_info(
            info, target_audience=QUICK_IAP_AUDIENCE
        )
    credentials.refresh(Request())
    return credentials.token


def _token_from_gcloud(impersonate: Optional[str]) -> str:
    command = ["gcloud", "auth", "print-identity-token"]
    if impersonate:
        command.append(f"--impersonate-service-account={impersonate}")
    command += [f"--audiences={QUICK_IAP_AUDIENCE}", "--include-email"]
    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"could not mint an IAP token: {result.stderr.strip()}")
    return result.stdout.strip()


def _get_page(token: str, url: str) -> Optional[str]:
    """Body, or None on 404. Anything else means we could not tell, so it raises."""
    try:
        response = requests.get(
            url, headers={"Authorization": f"Bearer {token}"}, timeout=REQUEST_TIMEOUT
        )
    except requests.RequestException as error:
        raise QuickFetchError(f"{url}: {error}")
    if response.status_code == 404:
        return None
    if not response.ok:
        raise QuickFetchError(f"{url}: HTTP {response.status_code}")
    return response.text[:MAX_PAGE_BYTES]


def list_sites(token: str) -> Dict[str, dict]:
    body = requests.get(
        f"https://{QUICK_DOMAIN}/api/sites?detail=1",
        headers={"Authorization": f"Bearer {token}"},
        timeout=REQUEST_TIMEOUT,
    )
    body.raise_for_status()
    data = body.json()
    updated = data.get("updated") or {}
    deployers = data.get("deployers") or {}
    return {
        name: {"updated": updated.get(name), "deployed_by": deployers.get(name)}
        for name in data.get("sites", [])
    }


def parse_title(html: str) -> Optional[str]:
    match = TITLE_RE.search(html)
    return _normalize(match.group(1)) if match else None


def parse_description(html: str) -> Optional[str]:
    """Attribute order varies, so read the meta tag before reading content."""
    for tag in META_RE.findall(html):
        attrs = {}
        for match in META_ATTR_RE.finditer(tag):
            attrs[match.group(1).lower()] = (
                match.group(3) or match.group(4) or match.group(5) or ""
            )
        if attrs.get("name", "").lower() == "description" and attrs.get("content"):
            return _normalize(attrs["content"])
    return None


def _normalize(value: str) -> str:
    import html as html_module

    return " ".join(html_module.unescape(value).split())


def local_script_paths(html: str) -> List[str]:
    """Same-origin scripts worth reading. quick-nav.js is injected by Quick."""
    paths: List[str] = []
    for src in SCRIPT_SRC_RE.findall(html):
        src = src.split("?")[0].split("#")[0]
        if src.startswith(("http://", "https://", "//", "data:")):
            continue
        if "quick-nav" in src or src in paths:
            continue
        paths.append(src)
    return paths[:MAX_SCRIPTS_PER_SITE]


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


def fetch_site(token: str, name: str, meta: dict) -> Optional[QuickSite]:
    """None when the site has no index.html, which Quick allows.

    Raises QuickFetchError if a page could not be read.
    """
    base = f"https://{name}.{QUICK_DOMAIN}"
    html = _get_page(token, f"{base}/")
    if html is None:
        return None

    scripts = local_script_paths(html)
    bodies = [html]
    for path in scripts:
        # A 404 here is a broken reference in the page, not a failed run.
        body = _get_page(token, f"{base}/{path}")
        if body is not None:
            bodies.append(body)

    blob = "\n".join(bodies)
    tables, runtime_refs = find_tables(blob)
    return QuickSite(
        name=name,
        title=parse_title(html),
        description=parse_description(html),
        deployed_by=meta.get("deployed_by"),
        updated=meta.get("updated"),
        tables=tables,
        runtime_table_refs=runtime_refs,
        scripts=scripts,
        calls_query=bool(QUICK_QUERY_RE.search(blob)),
    )


def get_quick_dashboards(
    token: Optional[str] = None,
) -> Tuple[List[QuickSite], List[QuickFetchError]]:
    """Quick sites that read the warehouse, and the sites we failed to read.

    Most Quick sites are not dashboards. Callers must treat a non-empty error
    list as an incomplete run.
    """
    token = token or get_iap_token()
    sites = list_sites(token)

    def fetch(item):
        name, meta = item
        try:
            return fetch_site(token, name, meta), None
        except QuickFetchError as error:
            return None, error

    dashboards: List[QuickSite] = []
    errors: List[QuickFetchError] = []
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
        for site, error in pool.map(fetch, sorted(sites.items())):
            if error is not None:
                errors.append(error)
            elif site is not None and site.is_dashboard:
                dashboards.append(site)
    return dashboards, errors
