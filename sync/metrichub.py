import re
from datahub.emitter.mce_builder import make_term_urn
from dataclasses import dataclass
from typing import List, Optional

import sqlglot as sqlglot
from metric_config_parser.config import ConfigCollection
from metric_config_parser.metric import MetricLevel

METRIC_HUB_REPO_URL = "https://github.com/mozilla/metric-hub"
LOOKER_METRICS_URL = "https://github.com/mozilla/metric-hub/tree/main/looker"


@dataclass
class MetricStatistic:
    """
    Defines and aggregation of a metric across a specific population.

    Metric-hub allows users to specify statistics on top of metrics.
    Statistics summarize the distribution of metrics within a specific
    time frame and population segment. These statistics allow for basic
    analyses of a metric and are represented in Looker as measures.
    """

    name: str

    @property
    def title_cased_name(self) -> str:
        return self.name.replace("_", " ").title()


@dataclass
class MetricHubDefinition:
    name: str
    description: str
    sql_definition: str
    product: str
    owners: Optional[List[str]]
    level: Optional[MetricLevel]
    bigquery_tables: Optional[List[str]]
    data_source: Optional[str]
    statistics: Optional[List[MetricStatistic]]
    friendly_name: Optional[str]
    deprecated: bool = False

    @property
    def display_name(self) -> str:
        metric_name = self.name

        if self.deprecated:
            metric_name += " ⚠️"

        if self.level:
            if self.level == MetricLevel.GOLD:
                metric_name += " 🥇"
            elif self.level == MetricLevel.SILVER:
                metric_name += " 🥈"
            elif self.level == MetricLevel.BRONZE:
                metric_name += " 🥉"

        return metric_name

    @property
    def urn(self) -> str:
        return f"{make_term_urn(f'Metric Hub.{self.product}.{self.name}')}"

    @property
    def title_cased_name(self) -> str:
        return self.name.replace("_", " ").title()


def _raw_table_name(table: sqlglot.exp.Table) -> str:
    """
    Adapted from bigquery-etl:
    https://github.com/mozilla/bigquery-etl/blob/12c27464b1d5c41f15a6d3d9e2463547164e3518/bigquery_etl/dependency.py#L21
    """
    return (
        table.sql("bigquery", comments=False)
        .split(" AS ", 1)[0]  # remove alias
        .replace("`", "")  # remove quotes
    )


def _extract_table_references(sql: str) -> List[str]:
    """
    Return a list of tables referenced in the given SQL. Adapted from bigquery-etl:
    https://github.com/mozilla/bigquery-etl/blob/12c27464b1d5c41f15a6d3d9e2463547164e3518/bigquery_etl/dependency.py#L31
    """
    # sqlglot cannot handle scripts with variables and control statements
    if re.search(r"^\s*DECLARE\b", sql, flags=re.MULTILINE):
        return []
    # sqlglot parses UDFs with keyword names incorrectly:
    # https://github.com/tobymao/sqlglot/issues/1535
    sql = re.sub(
        r"\.(range|true|false|null)\(",
        r".\1_(",
        sql,
        flags=re.IGNORECASE,
    )
    # sqlglot doesn't suppport OPTIONS on UDFs
    sql = re.sub(
        r"""OPTIONS\s*\(("([^"]|\\")*"|'([^']|\\')*'|[^)])*\)""",
        "",
        sql,
        flags=re.MULTILINE | re.IGNORECASE,
    )
    # sqlglot doesn't fully support byte strings
    sql = re.sub(
        r"""b(["'])""",
        r"\1",
        sql,
        flags=re.IGNORECASE,
    )
    query_statements = sqlglot.parse(sql, read="bigquery")

    # If there's only one statement, and it's a Column token, it's the table name:
    if len(query_statements) == 1 and isinstance(
        query_statements[0], sqlglot.exp.Column
    ):
        return [sql.replace("`", "")]

    creates, tables = set(), set()
    for statement in query_statements:
        if statement is None:
            continue
        creates |= {
            _raw_table_name(expr.this)
            for expr in statement.find_all(sqlglot.exp.Create)
        }
        tables |= (
            {_raw_table_name(table) for table in statement.find_all(sqlglot.exp.Table)}
            # ignore references created in this query
            - creates
            # ignore CTEs created in this statement
            - {cte.alias_or_name for cte in statement.find_all(sqlglot.exp.CTE)}
        )
    return sorted(tables)


def get_metric_definitions() -> List[MetricHubDefinition]:
    config_collection = ConfigCollection.from_github_repos(
        [METRIC_HUB_REPO_URL, LOOKER_METRICS_URL]
    )

    metrics = []
    for definition in config_collection.definitions:
        for (
            metric_name,
            metric,
        ) in definition.spec.metrics.definitions.items():
            # Some metrics don't have data sources
            # (e.g. ad_click_rate, chained metric used in jetstream)
            tables = None
            datasource = None
            if metric.data_source is not None:
                datasource = config_collection.get_data_source_definition(
                    slug=metric.data_source.name, app_name=definition.platform
                )
                tables = [
                    table.format(dataset=datasource.default_dataset)
                    for table in _extract_table_references(datasource.from_expression)
                ]

            statistics = []
            if metric.statistics is not None:
                for statistic_name, _ in metric.statistics.items():
                    statistics.append(MetricStatistic(name=statistic_name))

            metrics.append(
                MetricHubDefinition(
                    name=metric.name,
                    description=metric.description or "",
                    owners=[metric.owner]
                    if isinstance(metric.owner, str)
                    else metric.owner,
                    level=metric.level.value if metric.level else None,
                    friendly_name=metric.friendly_name or False,
                    deprecated=metric.deprecated or False,
                    sql_definition=metric.select_expression,
                    product=definition.platform,
                    bigquery_tables=tables,
                    statistics=statistics,
                    data_source=datasource.name if datasource else None,
                )
            )

    return metrics
