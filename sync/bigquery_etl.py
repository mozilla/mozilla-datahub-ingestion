import collections
import re
from pathlib import Path
from typing import Dict

import requests
from io import BytesIO
import tarfile

import yaml

FILE_PREFIX = "bigquery-etl-generated-sql/sql"
QUERY_FILES = {
    "query.sql",
    "view.sql",
    "query.py",
}
METADATA_FILE = "metadata.yaml"
SUPPORTED_PROJECTS = {
    "moz-fx-data-shared-prod",
    "moz-fx-data-marketing-prod",
}
VALID_TABLE_RE = re.compile(
    rf"{FILE_PREFIX}\/({'|'.join(SUPPORTED_PROJECTS)})\/([a-z0-9_-]+)\/([a-z0-9_-]+)\/({'|'.join(QUERY_FILES)}|{METADATA_FILE})"
)

REPO_ARCHIVE = "https://github.com/mozilla/bigquery_etl/archive/generated-sql.tar.gz"
REPO_URL = "https://github.com/mozilla/bigquery-etl/blob/generated-sql/sql"
WTMO_URL = "https://workflow.telemetry.mozilla.org/dags"


def get_bigquery_etl_table_references() -> Dict:
    table_references = collections.defaultdict(dict)
    # {qualified_name: {bigquery_etl_url: ..., wtmo_url: ...}}

    print("Fetching metadata from GitHub...")

    repo_content = BytesIO(requests.get(REPO_ARCHIVE).content)
    with tarfile.open(fileobj=repo_content, mode="r|gz") as tar:
        for member in tar:
            match = VALID_TABLE_RE.match(member.name)
            if match is None:
                continue

            project, dataset, table, filename = match.groups()
            qualified_name = f"{project}.{dataset}.{table}"

            if filename == METADATA_FILE:
                tar.extract(member)
                metadata = yaml.safe_load(Path(member.name).read_text())
                if "scheduling" in metadata and "dag_name" in metadata["scheduling"]:
                    wtmo_url = f"{WTMO_URL}/{metadata['scheduling']['dag_name']}/grid"
                    table_references[qualified_name]["wtmo_url"] = wtmo_url

            elif filename in QUERY_FILES:
                bigquery_etl_url = f"{REPO_URL}/{project}/{dataset}/{table}/{filename}"
                table_references[qualified_name]["bigquery_etl_url"] = bigquery_etl_url

    return table_references
