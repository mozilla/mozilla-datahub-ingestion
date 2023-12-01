import collections
from typing import List

import requests
from io import BytesIO
import tarfile

FILE_PREFIX = "bigquery-etl-generated-sql/sql/"
FILE_SUFFIX = "/query.sql"

REPO_ARCHIVE = "https://github.com/mozilla/bigquery_etl/archive/generated-sql.tar.gz"
REPO_URL = "https://github.com/mozilla/bigquery-etl/blob/generated-sql/sql"

SUPPORTED_PROJECTS = {
    "moz-fx-data-shared-prod",
    "moz-fx-data-marketing-prod",
}

TableReference = collections.namedtuple("TableReference", ("url", "qualified_name"))


def get_bigquery_etl_table_references() -> List[TableReference]:
    table_references = []
    print("Fetching metadata from GitHub...")
    repo_content = BytesIO(requests.get(REPO_ARCHIVE).content)
    with tarfile.open(fileobj=repo_content, mode="r|gz") as tar:
        for member in tar:
            if not (
                member.name.endswith(FILE_SUFFIX)
                and member.name.startswith(FILE_PREFIX)
            ):
                continue

            table = member.name.replace(FILE_PREFIX, "").replace(FILE_SUFFIX, "")

            project, _, _ = table.split("/")
            if project not in SUPPORTED_PROJECTS:
                continue

            qualified_name = table.replace("/", ".")
            url = f"{REPO_URL}/{table}"

            table_references.append(TableReference(url, qualified_name))

    return table_references
