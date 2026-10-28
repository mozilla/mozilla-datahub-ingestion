import json
from unittest.mock import patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.metadata.business_glossary import (
    BusinessGlossaryFileSource,
)

from sync.datahub import metrichub_glossary
from sync.datahub.metrichub_glossary_source import MetricHubGlossarySource
from sync.metrichub import MetricHubDefinition, MetricLevel, MetricStatistic

TABLE = "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table,PROD)"
OTHER_TABLE = "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.other,PROD)"


def metric(name, product, tables):
    return MetricHubDefinition(
        name=name,
        description=f"{name} description",
        sql_definition="COUNT(*)",
        product=product,
        owners=["owner@mozilla.com"],
        level=MetricLevel.GOLD,
        bigquery_tables=tables,
        data_source="clients_daily",
        statistics=[MetricStatistic(name="sum")],
        friendly_name=None,
    )


METRICS = [
    metric("active_hours", "firefox_desktop", ["project.dataset.table"]),
    metric(
        "uri_count",
        "firefox_desktop",
        ["project.dataset.table", "project.dataset.other"],
    ),
    metric("ad_click_rate", "firefox_desktop", None),
    metric("dau", "fenix", ["project.dataset.other"]),
]


def run_source():
    with patch(
        "sync.datahub.metrichub_glossary_source.get_metric_definitions",
        return_value=METRICS,
    ):
        source = MetricHubGlossarySource.create({}, PipelineContext(run_id="test"))
        return [wu.metadata for wu in source.get_workunits_internal()]


def as_json(records):
    return [json.dumps(record.to_obj(), sort_keys=True) for record in records]


def test_glossary_matches_file_based_recipe(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with patch.object(
        metrichub_glossary, "get_metric_definitions", return_value=METRICS
    ):
        metrichub_glossary.main()
    file_source = BusinessGlossaryFileSource.create(
        {"file": metrichub_glossary.GLOSSARY_FILENAME, "enable_auto_id": False},
        PipelineContext(run_id="test"),
    )
    expected = as_json(wu.metadata for wu in file_source.get_workunits_internal())

    records = run_source()
    glossary = as_json(records[: len(expected)])

    assert len(expected) > 0
    # Ignore audit stamps, which hold the run time
    assert [_strip_times(r) for r in glossary] == [_strip_times(r) for r in expected]


def _strip_times(record_json):
    record = json.loads(record_json)

    def strip(value):
        if isinstance(value, dict):
            return {k: strip(v) for k, v in value.items() if k != "time"}
        if isinstance(value, list):
            return [strip(v) for v in value]
        return value

    return strip(record)


def test_tables_get_term_patches():
    records = run_source()
    patches = [r for r in records if getattr(r, "changeType", None) == "PATCH"]

    terms_by_table = {}
    for mcp in patches:
        assert mcp.aspectName == "glossaryTerms"
        ops = json.loads(mcp.aspect.value)["patch"]
        assert all(op["op"] == "add" for op in ops)
        terms_by_table[mcp.entityUrn] = sorted(op["value"]["urn"] for op in ops)

    assert terms_by_table == {
        TABLE: [
            "urn:li:glossaryTerm:Metric Hub.firefox_desktop.active_hours",
            "urn:li:glossaryTerm:Metric Hub.firefox_desktop.uri_count",
        ],
        OTHER_TABLE: [
            "urn:li:glossaryTerm:Metric Hub.fenix.dau",
            "urn:li:glossaryTerm:Metric Hub.firefox_desktop.uri_count",
        ],
    }


def test_one_node_per_product():
    # Products not next to each other, as when both Metric Hub repos define the same one
    metrics = [
        metric("dau", "fenix", None),
        metric("active_hours", "firefox_desktop", None),
        metric("uri_count", "fenix", None),
    ]

    nodes = metrichub_glossary.build_glossary(metrics)["nodes"][0]["nodes"]

    assert [node["name"] for node in nodes] == ["fenix", "firefox_desktop"]
    assert [term["name"] for term in nodes[0]["terms"]] == ["dau 🥇", "uri_count 🥇"]
