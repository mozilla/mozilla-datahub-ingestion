"""Builds the metric-hub glossary YAML file for syncing to DataHub."""
import itertools
import operator
from typing import Dict, List
from metric_config_parser.metric import MetricLevel
from datahub.emitter.mce_builder import make_term_urn

import yaml

from sync.metrichub import (
    METRIC_HUB_REPO_URL,
    get_metric_definitions,
    MetricHubDefinition,
)

GLOSSARY_FILENAME = "metric_hub_glossary.yaml"


def _build_metric_dict(metric: MetricHubDefinition) -> Dict:
    metric_content = ""

    if metric.deprecated:
        metric_content += "⚠️ **This metric has been deprecated**\n\n"

    if metric.level:
        metric_content += f"Metric Level: {_get_metric_level_link_text(metric.level)}"

    if metric.description:
        metric_content += f"_{metric.description.strip()}_\n\n"

    if metric.sql_definition:
        metric_content += f"SQL Definition:\n```{metric.sql_definition.strip()}```\n\n"

    if metric.statistics and len(metric.statistics) > 0:
        metric_content += "Explore this metric in Looker:\n"
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
            fields = ",".join(["submission_date", f"{metric.name}_{statistic.name}"])
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

    with open(GLOSSARY_FILENAME, "w+") as f:
        yaml.dump(glossary, f)


if __name__ == "__main__":
    main()
