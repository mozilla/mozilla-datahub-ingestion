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
        deprecated=True
    )
    assert metric_def.display_name == "example_metric ⚠️ 🥇"

class MockConfigCollection:
    @property
    def definitions(self):
        return [
            MagicMock(
                platform="TestProduct",
                spec={
                    'metrics': MagicMock(
                        definitions={
                            'example_metric': MagicMock(
                                name="example_metric",
                                description="A test metric",
                                select_expression="SELECT * FROM table",
                                owner="owner@example.com",
                                level={'value': MetricLevel.GOLD},
                                deprecated=False,
                                friendly_name="Example Metric",
                                data_source=MagicMock(name="test_source")
                            )
                        }
                    )
                }
            )
        ]

@patch('sync.metrichub.ConfigCollection.from_github_repos')
def test_get_metric_definitions(mock_from_github_repos):
    mock_from_github_repos.return_value = MockConfigCollection()
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
        deprecated=False
    )
    print(metric_def.urn)
    assert metric_def.urn == "urn:li:glossaryTerm:Metric Hub.TestProduct.example_metric"