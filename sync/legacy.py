from collections import defaultdict
from dataclasses import dataclass
from typing import Sequence, Dict

import requests
import tarfile
from io import BytesIO

SCHEMA_URL = "https://github.com/mozilla-services/mozilla-pipeline-schemas/archive/generated-schemas.tar.gz"


@dataclass
class LegacyPing:
    name: str
    versions: Sequence[int]

    @property
    def bigquery_fully_qualified_names(self) -> Sequence[str]:
        table_names = []
        for version in self.versions:
            table_name = f"{self.name.replace('-', '_')}_v{version}"
            table_names.append(f"moz-fx-data-shared-prod.telemetry_live.{table_name}")
        return table_names


def _get_ping_schemas() -> Dict[str, Sequence[str]]:
    """
    Fetches the latest version of the schema tarball from GitHub and returns a dict of ping names and their schema versions.

    Example:
    {'account-ecosystem': ['account-ecosystem.4.schema.json'], 'android-anr-report': ['android-anr-report.1.schema.json', 'android-anr-report.2.schema.json'], 'anonymous': ['anonymous.4.schema.json']}
    """
    print("Fetching schemas from GitHub...")
    schemas = requests.get(SCHEMA_URL)
    schema_file = BytesIO(schemas.content)

    with tarfile.open(fileobj=schema_file, mode="r|gz") as tar:
        schema_versions = defaultdict(list)
        for member in tar:
            if member.name.startswith(
                "mozilla-pipeline-schemas-generated-schemas/schemas/telemetry/"
            ) and member.name.endswith(".schema.json"):
                *_, ping_name, schema_name = member.name.split("/")
                schema_versions[ping_name].append(schema_name)
    return schema_versions


def get_legacy_pings() -> Sequence[LegacyPing]:
    pings = []
    schema_versions = _get_ping_schemas()
    for key, value in schema_versions.items():
        pings.append(
            LegacyPing(name=key, versions=[int(v.split(".")[1]) for v in value])
        )
    return pings
