"""Builds the metric-hub glossary YAML file for syncing to DataHub."""

from collections import defaultdict
import itertools
import operator
from os import linesep
from typing import Any, Dict, List
from metric_config_parser.metric import MetricLevel
from datahub.emitter.mce_builder import make_term_urn, make_dataset_urn

import yaml

from sync.metrichub import (
    METRIC_HUB_REPO_URL,
    get_metric_definitions,
    MetricHubDefinition,
)

GLOSSARY_FILENAME = "metric_hub_glossary.yaml"
TABLE_TO_METRIC_FILENAME = "datasets.yaml"


def _build_metric_dict(metric: MetricHubDefinition) -> Dict:
    metric_content = ""

    if metric.deprecated:
        metric_content += "#### ⚠️ **This metric has been deprecated**\n\n"

    if metric.friendly_name:
        metric_content += f"## {metric.friendly_name} \n\n"

    if metric.level:
        metric_content += (
            f"**Metric Level:** {_get_metric_level_link_text(metric.level)}\n\n"
        )

    if metric.description:
        metric_content += f"{metric.description.strip().replace(linesep, ' ')}\n\n"

    if metric.sql_definition:
        metric_content += (
            f"**SQL Definition:**\n```sql\n{metric.sql_definition.strip()}\n```\n\n"
        )

    if metric.statistics:
        metric_content += "**Explore this metric in Looker:**\n"
        metric_content += "\n".join(_get_looker_statistics_links(metric))

    return {
        "id": metric.urn,
        "name": metric.display_name,
        "description": metric_content,
        "owners": {"users": metric.owners},
        "term_source": "EXTERNAL",
    }


def _get_metric_level_link_text(level: MetricLevel) -> str:
    url = "https://mozilla.acryl.io/glossaryTerm"
    if level == MetricLevel.GOLD:
        urn = make_term_urn("5fbb70ef-0a69-4db5-a301-907dd13148bc")
        text = "🥇Gold Metric"
    elif level == MetricLevel.SILVER:
        urn = make_term_urn("548b65c5-581c-4572-b544-8bd1cbbdc7a5")
        text = "🥈Silver Metric"
    elif level == MetricLevel.BRONZE:
        urn = make_term_urn("3be839fc-9782-433f-8e82-0632dc780c1c")
        text = "🥉Bronze Metric"
    else:
        return ""

    return f"[{text}]({url}/{urn})\n\n"


def _get_looker_statistics_links(metric: MetricHubDefinition) -> List[str]:
    url = "https://mozilla.cloud.looker.com/explore"
    links = []

    for statistic in metric.statistics:
        if metric.data_source is not None:
            explore = f"metric_definitions_{metric.data_source}"
            fields = ",".join(
                [
                    f"{explore}.submission_date",
                    f"{explore}.{metric.name}_{statistic.name}",
                ]
            )
            title = f"{metric.title_cased_name} {statistic.title_cased_name}"
            links.append(
                f"[{title}]({url}/{metric.product}/{explore}?fields={fields}&toggle=vis)"
            )

    return links


def _build_product_dict(product: str, metrics: List[MetricHubDefinition]) -> Dict:
    return {
        "name": product,
        "description": f"{product} metrics",
        "terms": [_build_metric_dict(metric) for metric in metrics],
    }


def _generate_table_to_term_data(
    metrics: List[MetricHubDefinition],
) -> List[Dict[str, Any]]:
    source_table_to_metric = defaultdict(list)
    yaml_data = []

    for metric in metrics:
        if metric.bigquery_tables is None:
            continue
        for bigquery_table in metric.bigquery_tables:
            source_table_urn = make_dataset_urn(
                platform="bigquery",
                name=bigquery_table,
            )
            source_table_to_metric[source_table_urn].append(metric.urn)

    for urn, glossary_terms in source_table_to_metric.items():
        yaml_data.append({"urn": urn, "glossary_terms": glossary_terms})

    return yaml_data


def main() -> None:
    metric_hub_definitions = get_metric_definitions()
    product_nodes = [
        _build_product_dict(product, product_metrics)
        for product, product_metrics in itertools.groupby(
            metric_hub_definitions, operator.attrgetter("product")
        )
    ]

    glossary = {
        "version": 1,
        "source": "Metric-Hub",
        "url": METRIC_HUB_REPO_URL,
        "owners": [],
        "nodes": [
            {
                "name": "Metric Hub",
                "description": "Central hub for metric definitions that are considered the source of truth.",  # noqa: E501
                "nodes": product_nodes,
            }
        ],
    }

    yaml_data = _generate_table_to_term_data(metric_hub_definitions)

    with open(GLOSSARY_FILENAME, "w+") as f:
        yaml.dump(glossary, f)

    with open(TABLE_TO_METRIC_FILENAME, "w+") as f:
        yaml.dump(yaml_data, f, sort_keys=False)


if __name__ == "__main__":
    main()
