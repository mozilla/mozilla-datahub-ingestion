import datetime
import json
from unittest.mock import MagicMock, patch

import pytest
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.metadata.schema_classes import (
    ChartUsageStatisticsClass,
    DashboardUsageStatisticsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipSourceClass,
    StatusClass,
)

from sync.datahub import redash_usage_source
from sync.datahub.redash_usage_source import OWNER_SOURCE_URL, RedashUsageSource

UTC = datetime.timezone.utc
DAY_1 = datetime.date(2026, 9, 1)
DAY_2 = datetime.date(2026, 9, 2)
DAY_1_MILLIS = 1788220800000  # 2026-09-01 00:00 UTC
DAY_2_MILLIS = DAY_1_MILLIS + 86400000

CHART = "urn:li:chart:(redash,10)"
OTHER_CHART = "urn:li:chart:(redash,11)"
DASHBOARD = "urn:li:dashboard:(redash,1)"
ALICE = "urn:li:corpuser:alice@mozilla.com"
BOB = "urn:li:corpuser:bob@mozilla.com"


def view_row(object_type, object_id, day, email, views, hour=12):
    return {
        "object_type": object_type,
        "object_id": object_id,
        "submission_date": day,
        "user_email": email,
        "views": views,
        "last_viewed_at": datetime.datetime.combine(
            day, datetime.time(hour), tzinfo=UTC
        ),
    }


DAILY_ROWS = [
    view_row("visualization", 10, DAY_1, "alice@mozilla.com", 3),
    view_row("visualization", 10, DAY_1, "bob@mozilla.com", 5),
    view_row("visualization", 10, DAY_1, None, 2),
    view_row("visualization", 10, DAY_2, "alice@mozilla.com", 1),
    view_row("visualization", 11, DAY_2, "bob@mozilla.com", 4),
    view_row("dashboard", 1, DAY_1, "alice@mozilla.com", 2, hour=9),
    view_row("dashboard", 1, DAY_1, "bob@mozilla.com", 1, hour=17),
    # Not in DataHub, e.g. a draft
    view_row("visualization", 99, DAY_1, "alice@mozilla.com", 7),
    view_row("dashboard", 9, DAY_1, "alice@mozilla.com", 7),
]


def total_row(object_type, object_id, views_90d, last_viewed_at=None):
    return {
        "object_type": object_type,
        "object_id": object_id,
        "views_90d": views_90d,
        "last_viewed_at": last_viewed_at,
    }


TOTAL_ROWS = [
    total_row("visualization", 10, 12),
    # No views in the last 90 days
    total_row("visualization", 11, 0),
    total_row("dashboard", 1, 20, datetime.datetime(2026, 9, 2, 8, tzinfo=UTC)),
    total_row("visualization", 99, 7),
]

OWNER_ROWS = [
    {
        "object_type": "visualization",
        "object_id": 10,
        "owner_email": "alice@mozilla.com",
    },
    {"object_type": "dashboard", "object_id": 1, "owner_email": "bob@mozilla.com"},
    {
        "object_type": "visualization",
        "object_id": 99,
        "owner_email": "alice@mozilla.com",
    },
]


CHART_USER_ROWS = [
    {"object_id": 10, "user_email": email, "views": views}
    for email, views in [
        ("alice@mozilla.com", 5),
        ("bob@mozilla.com", 5),
        ("carol@mozilla.com", 1),
        ("dave@mozilla.com", 3),
        ("erin@mozilla.com", 2),
        ("frank@mozilla.com", 1),
    ]
] + [
    # Not in DataHub
    {"object_id": 99, "user_email": "alice@mozilla.com", "views": 7},
]


def fake_query(sql, job_config=None):
    job = MagicMock()
    if "owner_email" in sql:
        job.result.return_value = OWNER_ROWS
    elif "user_email IS NOT NULL" in sql:
        job.result.return_value = CHART_USER_ROWS
    elif "views_90d" in sql:
        job.result.return_value = TOTAL_ROWS
    else:
        job.result.return_value = DAILY_ROWS
    return job


@pytest.fixture
def bigquery_client():
    with patch("sync.datahub.redash_usage_source.bigquery.Client") as client_class:
        client = client_class.return_value
        client.query.side_effect = fake_query
        yield client_class


def make_source(config=None, ownership=None):
    """ownership maps urns to the owners DataHub already has for them."""
    ownership = ownership or {}
    graph = MagicMock()
    graph.get_urns_by_filter.side_effect = lambda entity_types, platform: {
        "chart": [CHART, OTHER_CHART],
        "dashboard": [DASHBOARD],
    }[entity_types[0]]
    # Like DataHub, entities with no ownership aspect are left out
    graph.get_entities.side_effect = lambda entity_type, urns, aspects: {
        urn: {"ownership": (OwnershipClass(owners=ownership[urn]), None)}
        for urn in urns
        if urn in ownership
    }
    ctx = PipelineContext(run_id="test", graph=graph)
    return RedashUsageSource.create(config or {}, ctx)


def run(config=None, ownership=None):
    source = make_source(config, ownership)
    workunits = list(source.get_workunits())
    return source, workunits


def structured_properties(mcp):
    """Property urn -> values after the patch, or None if the patch removes it."""
    assert mcp.changeType == "PATCH"
    value = json.loads(mcp.aspect.value)
    operations = value["patch"] if isinstance(value, dict) else value
    result = {}
    for operation in operations:
        urn = operation["path"].split("/")[2]
        if operation["op"] == "remove":
            result[urn] = None
        else:
            assert operation["value"]["propertyUrn"] == urn
            result[urn] = operation["value"]["values"]
    return result


def patches(workunits, aspect_name):
    return {
        wu.get_urn(): wu.metadata
        for wu in workunits
        if wu.metadata.aspectName == aspect_name
    }


def usage_aspects(workunits, urn, daily):
    aspects = [
        wu.metadata.aspect
        for wu in workunits
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and wu.metadata.entityUrn == urn
        and isinstance(
            wu.metadata.aspect,
            (ChartUsageStatisticsClass, DashboardUsageStatisticsClass),
        )
        and (wu.metadata.aspect.eventGranularity is not None) == daily
    ]
    return sorted(aspects, key=lambda aspect: aspect.timestampMillis)


def test_chart_daily_usage(bigquery_client):
    _, workunits = run()
    day_1, day_2 = usage_aspects(workunits, CHART, daily=True)

    assert day_1.timestampMillis == DAY_1_MILLIS
    assert day_1.eventGranularity.unit == "DAY"
    assert day_1.eventGranularity.multiple == 1
    # The row with no email counts toward views but not users
    assert day_1.viewsCount == 10
    assert day_1.uniqueUserCount == 2
    assert [(c.user, c.viewsCount) for c in day_1.userCounts] == [(BOB, 5), (ALICE, 3)]

    assert day_2.timestampMillis == DAY_2_MILLIS
    assert day_2.viewsCount == 1
    assert [(c.user, c.viewsCount) for c in day_2.userCounts] == [(ALICE, 1)]


def test_dashboard_daily_usage(bigquery_client):
    _, workunits = run()
    (day_1,) = usage_aspects(workunits, DASHBOARD, daily=True)

    assert isinstance(day_1, DashboardUsageStatisticsClass)
    assert day_1.timestampMillis == DAY_1_MILLIS
    assert day_1.viewsCount == 3
    assert day_1.uniqueUserCount == 2
    assert day_1.lastViewedAt == DAY_1_MILLIS + 17 * 3600000
    assert [(c.user, c.viewsCount, c.userEmail) for c in day_1.userCounts] == [
        (ALICE, 2, "alice@mozilla.com"),
        (BOB, 1, "bob@mozilla.com"),
    ]


def test_dashboard_total_usage(bigquery_client):
    _, workunits = run()

    (dashboard_total,) = usage_aspects(workunits, DASHBOARD, daily=False)
    assert dashboard_total.viewsCount == 20
    assert dashboard_total.lastViewedAt == 1788336000000  # 2026-09-02 08:00 UTC

    # DataHub doesn't show chart totals, so charts only get daily buckets
    assert not usage_aspects(workunits, CHART, daily=False)


VIEWS_PROPERTY = "urn:li:structuredProperty:mozilla.redash.views_90d"
USERS_PROPERTY = "urn:li:structuredProperty:mozilla.redash.users_90d"
TOP_USERS_PROPERTY = "urn:li:structuredProperty:mozilla.redash.top_users_90d"


def test_structured_properties(bigquery_client):
    source, workunits = run()

    property_patches = patches(workunits, "structuredProperties")
    assert set(property_patches) == {CHART, OTHER_CHART, DASHBOARD}
    assert source.report.structured_property_patches == 3

    # Top users are capped at 5, most views first, then by email
    assert structured_properties(property_patches[CHART]) == {
        VIEWS_PROPERTY: [{"double": 12.0}],
        USERS_PROPERTY: [{"double": 6.0}],
        TOP_USERS_PROPERTY: [
            {"string": f"urn:li:corpuser:{name}@mozilla.com"}
            for name in ["alice", "bob", "dave", "erin", "carol"]
        ],
    }
    # No views in the last 90 days, so top users from an earlier run are cleared
    assert structured_properties(property_patches[OTHER_CHART]) == {
        VIEWS_PROPERTY: [{"double": 0.0}],
        USERS_PROPERTY: [{"double": 0.0}],
        TOP_USERS_PROPERTY: None,
    }
    # Dashboards only get views, since DataHub shows their users itself
    assert structured_properties(property_patches[DASHBOARD]) == {
        VIEWS_PROPERTY: [{"double": 20.0}],
    }


def test_emit_structured_properties_disabled(bigquery_client):
    _, workunits = run({"emit_structured_properties": False})

    assert not patches(workunits, "structuredProperties")
    # Dashboard totals still come from the same query
    assert usage_aspects(workunits, DASHBOARD, daily=False)


def test_entities_missing_from_datahub_are_skipped(bigquery_client):
    source, workunits = run()

    urns = {wu.get_urn() for wu in workunits}
    assert urns == {CHART, OTHER_CHART, DASHBOARD}
    assert source.report.entities_not_in_datahub == 2
    assert source.report.charts_in_datahub == 2
    assert source.report.dashboards_in_datahub == 1


def owner(
    user, source_type="SERVICE", url=OWNER_SOURCE_URL, owner_type="TECHNICAL_OWNER"
):
    return OwnerClass(
        owner=user,
        type=owner_type,
        source=OwnershipSourceClass(type=source_type, url=url),
    )


def owner_keys(aspect):
    return [
        (
            o.owner,
            o.type,
            o.source.type if o.source else None,
            o.source.url if o.source else None,
        )
        for o in aspect.owners
    ]


def ownership_aspects(workunits):
    return {
        wu.get_urn(): wu.metadata.aspect
        for wu in workunits
        if wu.metadata.aspectName == "ownership"
    }


def test_ownership(bigquery_client):
    source, workunits = run()

    aspects = ownership_aspects(workunits)
    # OTHER_CHART has no owner in either place, so it isn't written
    assert set(aspects) == {CHART, DASHBOARD}
    assert source.report.ownership_updates == 2
    assert all(
        wu.metadata.changeType == "UPSERT"
        for wu in workunits
        if wu.metadata.aspectName == "ownership"
    )
    assert owner_keys(aspects[CHART]) == [
        (ALICE, "TECHNICAL_OWNER", "SERVICE", OWNER_SOURCE_URL)
    ]
    assert owner_keys(aspects[DASHBOARD]) == [
        (BOB, "TECHNICAL_OWNER", "SERVICE", OWNER_SOURCE_URL)
    ]


def test_ownership_replaces_previous_owner(bigquery_client):
    carol = "urn:li:corpuser:carol@mozilla.com"
    _, workunits = run(
        ownership={
            CHART: [
                # Added by a run from before OWNER_SOURCE_URL was set
                owner(BOB, url=None),
                # Added in the UI
                owner(carol, source_type="MANUAL", url=None, owner_type="DATAOWNER"),
            ]
        }
    )

    assert owner_keys(ownership_aspects(workunits)[CHART]) == [
        (carol, "DATAOWNER", "MANUAL", None),
        (ALICE, "TECHNICAL_OWNER", "SERVICE", OWNER_SOURCE_URL),
    ]


def test_ownership_removed_when_redash_owner_is_gone(bigquery_client):
    other_service = "https://example.com/other-source"
    _, workunits = run(
        ownership={OTHER_CHART: [owner(ALICE), owner(BOB, url=other_service)]}
    )

    # Only this source's owner is removed
    assert owner_keys(ownership_aspects(workunits)[OTHER_CHART]) == [
        (BOB, "TECHNICAL_OWNER", "SERVICE", other_service)
    ]


def test_unchanged_ownership_is_skipped(bigquery_client):
    source, workunits = run(ownership={CHART: [owner(ALICE)], DASHBOARD: [owner(BOB)]})

    assert not ownership_aspects(workunits)
    assert source.report.ownership_updates == 0


def test_ownership_read_in_batches(bigquery_client, monkeypatch):
    monkeypatch.setattr(redash_usage_source, "OWNERSHIP_BATCH_SIZE", 1)
    source = make_source()
    list(source.get_workunits())

    calls = source.ctx.graph.get_entities.call_args_list
    assert [(c.args[0], c.args[1]) for c in calls] == [
        ("chart", [CHART]),
        ("chart", [OTHER_CHART]),
        ("dashboard", [DASHBOARD]),
    ]


def test_emit_ownership_disabled(bigquery_client):
    source = make_source({"emit_ownership": False})
    workunits = list(source.get_workunits())

    assert not ownership_aspects(workunits)
    assert not source.ctx.graph.get_entities.called
    queries = [
        call.args[0] for call in bigquery_client.return_value.query.call_args_list
    ]
    assert not [sql for sql in queries if "owner_email" in sql]


def test_no_status_aspects(bigquery_client):
    # Status belongs to the Redash source, so this source must not add one
    _, workunits = run()

    assert all(not wu.is_primary_source for wu in workunits)
    assert not [
        wu
        for wu in workunits
        if wu.metadata.aspectName == "status"
        or isinstance(getattr(wu.metadata, "aspect", None), StatusClass)
    ]


def test_query_config(bigquery_client):
    run(
        {
            "billing_project": "billing",
            "lookback_days": 400,
            "views_table": "sandbox.test.views",
            "owners_table": "sandbox.test.owners",
        }
    )

    bigquery_client.assert_called_once_with(project="billing", credentials=None)
    calls = bigquery_client.return_value.query.call_args_list
    daily_call = calls[0]
    assert "`sandbox.test.views`" in daily_call.args[0]
    (parameter,) = daily_call.kwargs["job_config"].query_parameters
    assert (parameter.name, parameter.value) == ("lookback_days", 400)
    assert "`sandbox.test.views`" in calls[2].args[0]
    assert "`sandbox.test.owners`" in calls[3].args[0]
