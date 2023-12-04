import os
from dataclasses import dataclass
from pathlib import Path
import tempfile
from unittest.mock import patch

from sync.bigquery_etl import get_bigquery_etl_table_references
import tarfile


@dataclass
class MockApiResponse:
    content: bytes


@patch("requests.get")
def test_get_legacy_pings(mock_get):
    with (tempfile.NamedTemporaryFile(suffix=".tar.gz") as fp):
        with tarfile.open(fileobj=fp, mode="w:gz") as tar:

            tar.add(
                Path("tests/sample_data/bigquery_etl"),
                arcname=os.path.basename("bigquery-etl-generated-sql"),
            )

        fp.seek(0)
        mock_get.side_effect = [MockApiResponse(fp.read())]

    data = get_bigquery_etl_table_references()

    assert "moz-fx-data-shared-prod.test_dataset.test_table" in data
    assert (
        data["moz-fx-data-shared-prod.test_dataset.test_table"]["bigquery_etl_url"]
        == "https://github.com/mozilla/bigquery-etl/blob/generated-sql/sql/moz-fx-data-shared-prod/test_dataset/test_table/query.sql"
    )
    assert (
        data["moz-fx-data-shared-prod.test_dataset.test_table"]["wtmo_url"]
        == "https://workflow.telemetry.mozilla.org/dags/test_dag/grid"
    )
