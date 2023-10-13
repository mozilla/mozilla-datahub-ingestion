"""Builds the metric-hub glossary YAML file for syncing to DataHub."""
import itertools
import operator
from typing import Dict, List

import yaml

from sync.metrichub import (
    METRIC_HUB_DOCS_URL,
    get_metric_definitions,
    MetricHubDefinition,
)

GLOSSARY_FILENAME = "metric_hub_glossary.yaml"


def _build_metric_dict(metric: MetricHubDefinition) -> Dict:
    metric_content = ""
    if metric.description:
        metric_content += f"_{metric.description.strip()}_\n\n"

    if metric.sql_definition:
        metric_content += f"SQL Definition:\n```{metric.sql_definition.strip()}```"

    return {
        "name": metric.name,
        "description": metric_content,
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
        "url": METRIC_HUB_DOCS_URL,
        "owners": [],
        "nodes": [
            {
                "name": "Metric Hub",
                "description": "Central hub for metric definitions that are considered the source of truth.",
                "nodes": product_nodes,
            }
        ],
    }

    with open(GLOSSARY_FILENAME, "w+") as f:
        yaml.dump(glossary, f)


if __name__ == "__main__":
    main()
