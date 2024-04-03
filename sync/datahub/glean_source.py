from typing import Iterable, Optional, List

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.emitter.mcp import MetadataChangeProposalWrapper, ChangeTypeClass
import datahub.emitter.mce_builder as builder
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    DatasetPropertiesClass,
    SubTypesClass,
    UpstreamLineageClass,
    UpstreamClass,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
    StaleEntityRemovalSourceReport,
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from sync.datahub.utils import get_current_timestamp
from sync.glean import get_glean_pings


class GleanSourceConfig(StatefulIngestionConfigBase):
    env: str = "PROD"
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None


class GleanSource(StatefulIngestionSourceBase):
    def __init__(self, config: GleanSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.platform = "Glean"

    def get_platform_instance_id(self) -> str:
        return f"{self.platform}"

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext):
        config = GleanSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for glean_ping in get_glean_pings():
            glean_qualified_urn = builder.make_dataset_urn(
                platform=self.platform,
                name=glean_ping.qualified_name,
                env=self.config.env,
            )
            glean_ping_aspects = [
                InstitutionalMemoryClass(
                    elements=[
                        InstitutionalMemoryMetadataClass(
                            url=glean_ping.glean_dictionary_url,
                            description="Glean Dictionary Ping Documentation",
                            createStamp=get_current_timestamp(),
                        )
                    ],
                ),
                DatasetPropertiesClass(
                    name=glean_ping.name, description=glean_ping.description
                ),
                SubTypesClass(typeNames=["Ping"]),
                BrowsePathsClass(
                    paths=[f"/{self.config.env.lower()}/glean/{glean_ping.app_name}"]
                ),
            ]
            glean_ping_mcps = MetadataChangeProposalWrapper.construct_many(
                entityUrn=glean_qualified_urn, aspects=glean_ping_aspects
            )

            upstream_lineage = UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=glean_qualified_urn,
                        type="TRANSFORMED",
                    )
                ]
            )
            upstream_lineage_mcps = [
                MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=builder.make_dataset_urn(
                        platform="bigquery",
                        name=qualified_table_name,
                        env=self.config.env,
                    ),
                    aspectName="upstreamLineage",
                    aspect=upstream_lineage,
                )
                for qualified_table_name in glean_ping.bigquery_fully_qualified_names
            ]

            for mcp in glean_ping_mcps + upstream_lineage_mcps:
                wu = mcp.as_workunit()
                yield wu

    def get_report(self) -> StaleEntityRemovalSourceReport:
        return self.report
