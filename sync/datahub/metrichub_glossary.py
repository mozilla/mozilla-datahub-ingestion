"""Builds the metric-hub glossary YAML file for syncing to DataHub."""
import itertools
import operator
from typing import Dict, List
from metric_config_parser.metric import MetricLevel

import yaml

from sync.metrichub import (
    METRIC_HUB_REPO_URL,
    get_metric_definitions,
    MetricHubDefinition,
)

GLOSSARY_FILENAME = "metric_hub_glossary.yaml"


def _build_metric_dict(metric: MetricHubDefinition) -> Dict:
    metric_name = metric.name
    metric_content = ""

    if metric.deprecated:
        metric_content += "⚠️ **This metric has been deprecated**\n\n"
        metric_name += " ⚠️"

    if metric.level:
        metric_content += "Metric Level: "

        if metric.level == MetricLevel.GOLD:
            metric_content += (
                "[🥇Gold Metric](https://mozilla.acryl.io/glossaryTerm/urn:li:glossaryTerm:"
                + "5fbb70ef-0a69-4db5-a301-907dd13148bc/Documentation?is_lineage_mode=false)\n\n"
            )
            metric_name += " 🥇"
        elif metric.level == MetricLevel.SILVER:
            metric_content += (
                "[🥈Silver Metric](https://mozilla.acryl.io/glossaryTerm/urn:li:glossaryTerm:"
                + "548b65c5-581c-4572-b544-8bd1cbbdc7a5/Related%20Entities?"
                + "is_lineage_mode=false)\n\n"
            )
            metric_name += " 🥈"
        elif metric.level == MetricLevel.BRONZE:
            metric_content += (
                "[🥉Bronze Metric](https://mozilla.acryl.io/glossaryTerm/urn:li:glossaryTerm:"
                + "3be839fc-9782-433f-8e82-0632dc780c1c/Documentation?is_lineage_mode=false)\n\n"
            )
            metric_name += " 🥉"

    if metric.description:
        metric_content += f"_{metric.description.strip()}_\n\n"

    if metric.sql_definition:
        metric_content += f"SQL Definition:\n```{metric.sql_definition.strip()}```"

    return {
        "name": metric_name,
        "description": metric_content,
        "owners": {"users": metric.owners},
        "term_source": "EXTERNAL",
    }


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
