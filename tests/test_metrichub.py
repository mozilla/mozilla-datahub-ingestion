from sync.metrichub import MetricHubDefinition, MetricLevel, MetricStatistic
from sync.metrichub import get_metric_definitions
from unittest.mock import patch, MagicMock


def test_metric_statistic_title_cased_name():
    metric_stat = MetricStatistic(name="test_metric_name")
    assert metric_stat.title_cased_name == "Test Metric Name"


def test_metric_hub_definition_display_name():
    metric_def = MetricHubDefinition(
        name="example_metric",
        description="",
        sql_definition="",
        product="TestProduct",
        owners=None,
        level=MetricLevel.GOLD,
        bigquery_tables=None,
        data_source=None,
        statistics=None,
        friendly_name=None,
        deprecated=True,
    )
    assert metric_def.display_name == "example_metric ⚠️ 🥇"


class MockConfigCollection:

    def get_data_source_definition(self, slug, app_name):
        return MagicMock(default_dataset="test dataset", from_expression="FROM table")

    @property
    def definitions(self):
        metric_mock = MagicMock(
            description="A test metric",
            select_expression="SELECT * FROM table",
            owner="owner@example.com",
            level=MagicMock(value="GOLD"),  # Ensure this matches MetricLevel.GOLD
            deprecated=False,
            friendly_name="Example Metric",
            data_source=MagicMock(name="test_source"),
        )

        metric_mock.name = "example_metric"
        spec_mock = MagicMock()
        spec_mock.metrics.definitions = {"example_metric": metric_mock}

        definition_mock = MagicMock(platform="TestProduct")
        definition_mock.spec = spec_mock

        return [definition_mock]


@patch("sync.metrichub.ConfigCollection")
def test_get_metric_definitions(mock_config_collection):
    mock_config_collection.from_github_repos.return_value = MockConfigCollection()

    metrics = get_metric_definitions()
    assert len(metrics) == 1
    assert metrics[0].name == "example_metric"


def test_metric_hub_definition_urn():
    metric_def = MetricHubDefinition(
        name="example_metric",
        description="",
        sql_definition="",
        product="TestProduct",
        owners=None,
        level=None,
        bigquery_tables=None,
        data_source=None,
        statistics=None,
        friendly_name=None,
        deprecated=False,
    )
    print(metric_def.urn)
    assert metric_def.urn == "urn:li:glossaryTerm:Metric Hub.TestProduct.example_metric"
