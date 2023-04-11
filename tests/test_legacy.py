from dataclasses import dataclass
from pathlib import Path
import tempfile
from typing import Union
from unittest.mock import patch

from sync.legacy import _get_ping_schemas
import tarfile
import os.path


@dataclass
class MockApiResponse:
    content: bytes


@patch("requests.get")
def test_get_legacy_pings(mock_get):
    # make sample dir into a tarfile
    with tempfile.NamedTemporaryFile(suffix=".tar.gz") as fp:
        with tarfile.open(fileobj=fp, mode="w:gz") as tar:
            tar.add(
                Path("tests/sample_data/mozilla-pipeline-schemas-generated-schemas"),
                arcname=os.path.basename("tests/sample_data/mozilla-pipeline-schemas-generated-schemas"),
            )
        fp.seek(0)
        mock_get.side_effect = [MockApiResponse(fp.read())]

    pings = _get_ping_schemas()

    assert len(pings) == 1
    assert "fake-ping" in pings
    assert len(pings["fake-ping"]) == 2

