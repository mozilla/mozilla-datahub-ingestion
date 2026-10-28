"""Metric Hub metrics as glossary terms, linked to the BigQuery tables they read from.

Terms are built in memory from Metric Hub and written with the business glossary source's
code. Links to tables are patches that add terms, so terms added to a table by hand
are kept. A link stays on a table after its metric stops reading from it.
"""

from typing import Iterable, List

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.metadata.business_glossary import (
    BusinessGlossaryConfig,
    BusinessGlossarySourceConfig,
    get_mces,
    materialize_all_node_urns,
    populate_path_vs_id,
)
from datahub.metadata.schema_classes import GlossaryTermAssociationClass
from datahub.specific.dataset import DatasetPatchBuilder

from sync.datahub.metrichub_glossary import (
    build_glossary,
    generate_table_to_term_data,
)
from sync.metrichub import (
    METRIC_HUB_REPO_URL,
    MetricHubDefinition,
    get_metric_definitions,
)


class MetricHubGlossarySourceConfig(ConfigModel):
    pass


class MetricHubGlossarySource(Source):
    def __init__(self, config: MetricHubGlossarySourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()
        # Same settings as recipes/metrichub_recipe.dhub.yaml. The glossary is built in
        # memory, so the file is only a label.
        self.glossary_source_config = BusinessGlossarySourceConfig(
            file=METRIC_HUB_REPO_URL, enable_auto_id=False
        )

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "MetricHubGlossarySource":
        config = MetricHubGlossarySourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        metrics = get_metric_definitions()
        yield from self._glossary_workunits(metrics)
        yield from self._table_term_workunits(metrics)

    def _glossary_workunits(
        self, metrics: List[MetricHubDefinition]
    ) -> Iterable[MetadataWorkUnit]:
        glossary = BusinessGlossaryConfig.model_validate(build_glossary(metrics))
        materialize_all_node_urns(glossary, self.glossary_source_config.enable_auto_id)
        path_vs_id = populate_path_vs_id(glossary)
        yield from auto_workunit(
            get_mces(
                glossary,
                path_vs_id,
                ingestion_config=self.glossary_source_config,
                ctx=self.ctx,
            )
        )

    def _table_term_workunits(
        self, metrics: List[MetricHubDefinition]
    ) -> Iterable[MetadataWorkUnit]:
        for table in generate_table_to_term_data(metrics):
            patch = DatasetPatchBuilder(table["urn"])
            for term_urn in table["glossary_terms"]:
                patch.add_term(GlossaryTermAssociationClass(urn=term_urn))
            for mcp in patch.build():
                # Not the primary source, so DataHub doesn't add status or browse path
                # aspects to tables the BigQuery source owns
                yield MetadataWorkUnit(
                    id=f"{mcp.entityUrn}-{mcp.aspectName}-patch",
                    mcp_raw=mcp,
                    is_primary_source=False,
                )

    def get_report(self) -> SourceReport:
        return self.report
