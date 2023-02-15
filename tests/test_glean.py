from dataclasses import dataclass
from unittest.mock import patch

from sync.glean import get_glean_pings


@dataclass
class MockApiResponse:
    data: dict | list

    def json(self):
        return self.data


@patch('requests.get')
def test_get_glean_pings(mock_get):
    list_apps_response = [
        {
            "app_name": "glean_test_app",
            "app_description": "A test glean application",
            "canonical_app_name": "Glean Test App",
            "deprecated": False,
            "url": "https://github.com/fake-organization/fake-repository",
            "notification_emails": [
                "fake_email@email.com"
            ],
            "app_ids": [
                {
                    "name": "org.mozilla.fake",
                    "description": "Nightly channel of the Glean test app",
                    "channel": "nightly",
                    "deprecated": False,
                    "prototype": False
                },
            ],
            "has_annotation": True,
            "logo": "/data/fake/logo.svg",
            "featured": True,
            "app_tags": [
                "App Tag"
            ]
        },
    ]
    app_data_response = {
        "app_name": "glean_test_app",
        "app_description": "A test glean application",
        "canonical_app_name": "Glean Test App",
        "deprecated": False,
        "url": "https://github.com/fake-organization/fake-repository",
        "notification_emails": [
            "fake_email@email.com"
        ],
        "app_ids": [
            {
                "name": "org.mozilla.fake",
                "description": "Nightly channel of the Glean test app",
                "channel": "nightly",
                "deprecated": False,
                "prototype": False
            },
        ],
        "has_annotation": True,
        "logo": "/data/fake/logo.svg",
        "featured": True,
        "app_tags": [
            "App Tag"
        ],
        "pings": [
            {
                "bugs": [
                    "https://bugzilla.mozilla.com/00000/",
                ],
                "data_reviews": [
                    "https://github.com/fake-organization/fake-repository/pull/00000"
                ],
                "dates": {
                    "first": "2000-01-01 00:00:00",
                    "last": "2100-12-31 23:59:59"
                },
                "description": "This is a test ping for the test app",
                "git-commits": {
                    "first": "aaaaaaaaa",
                    "last": "bbbbbbbbb"
                },
                "include_client_id": False,
                "metadata": {},
                "no_lint": [],
                "notification_emails": [
                    "test-app@mozilla.com"
                ],
                "reasons": {},
                "reflog-index": {
                    "first": 100,
                    "last": 0
                },
                "send_if_empty": False,
                "source_url": "https://github.com/fake-organization/fake-repository/blob/aaaaaaaaa/test_app/app/pings.yaml#L1",
                "name": "test_ping",
                "origin": "testorigin",
                "date_first_seen": "2000-01-01 00:00:00",
                "tags": [],
                "variants": [
                    {
                        "id": "org.mozilla.test",
                        "description": "release",
                        "table": "org_mozilla_test.test_ping",
                        "channel": "release",
                        "looker_explore": {
                            "name": "test_ping",
                            "url": "https://mozilla.cloud.looker.com/explore/test/test"
                        }
                    },
                    {
                        "id": "org.mozilla.test_beta",
                        "description": "beta",
                        "table": "org_mozilla_test_beta.test_ping",
                        "channel": "beta",
                        "looker_explore": {
                            "name": "test_piong",
                            "url": "https://mozilla.cloud.looker.com/explore/test/test"
                        }
                    },
                ],
                "has_annotation": False
            },
        ]
    }

    mock_get.side_effect = [
        MockApiResponse(list_apps_response),
        MockApiResponse(app_data_response)
    ]

    actual = get_glean_pings()

    assert len(actual) == 1
    assert actual[0].name == "test_ping"
    assert actual[0].app_name == 'glean_test_app'

