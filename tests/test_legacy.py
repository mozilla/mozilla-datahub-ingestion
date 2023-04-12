from dataclasses import dataclass
from pathlib import Path
import tempfile
from unittest.mock import patch

from sync.legacy import get_legacy_pings
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
                arcname=os.path.basename("mozilla-pipeline-schemas-generated-schemas"),
            )
        fp.seek(0)
        mock_get.side_effect = [MockApiResponse(fp.read())]

    pings = get_legacy_pings()

    assert len(pings) == 1
    assert pings[0].name == "fake-ping"
    assert len(pings[0].versions) == 2
    assert "fake-ping.3" in pings[0].versions
