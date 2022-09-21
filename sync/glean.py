from dataclasses import dataclass
from typing import Sequence

import requests

GLEAN_DICTIONARY_URL = "https://dictionary.telemetry.mozilla.org"


@dataclass
class GleanPing:
    name: str
    description: str
    app_name: str
    app_ids: Sequence[str]

    @property
    def glean_dictionary_url(self) -> str:
        return f"{GLEAN_DICTIONARY_URL}/apps/{self.app_name}/pings/{self.name}"

    @property
    def qualified_name(self) -> str:
        return f"{self.app_name}.{self.name}"

    @property
    def bigquery_dataset_names(self) -> Sequence[str]:
        return [app_id.replace(".", "_").lower() + "_live" for app_id in self.app_ids]

    @property
    def bigquery_table_name(self) -> str:
        return self.name + "_v1"  # TODO: Update this to get the right ping version

    @property
    def bigquery_fully_qualified_names(self) -> Sequence[str]:
        return [
            f"moz-fx-data-shared-prod.{dataset_name}.{self.bigquery_table_name}"
            for dataset_name in self.bigquery_dataset_names
        ]


def get_glean_pings() -> Sequence[GleanPing]:
    pings = []
    apps = requests.get(f"{GLEAN_DICTIONARY_URL}/data/apps.json").json()
    for app in apps:
        app_name = app["app_name"]
        app_data = requests.get(
            f"{GLEAN_DICTIONARY_URL}/data/{app_name}/index.json"
        ).json()
        app_ids = [app_id["name"] for app_id in app_data["app_ids"]]
        for ping_data in app_data["pings"]:

            pings.append(
                GleanPing(
                    name=ping_data["name"],
                    description=ping_data["description"],
                    app_name=app_name,
                    app_ids=app_ids,
                )
            )

    return pings
