from typing import Iterable, Optional, List

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.emitter.mcp import MetadataChangeProposalWrapper, ChangeTypeClass
import datahub.emitter.mce_builder as builder
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
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

from sync.legacy import get_legacy_pings


class LegacySourceConfig(StatefulIngestionConfigBase):
    env: str = "PROD"
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None


class LegacySource(StatefulIngestionSourceBase):
    def __init__(self, config: LegacySourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.platform = "LegacyTelemetry"

    def get_platform_instance_id(self) -> str:
        return f"{self.platform}"

    @classmethod
    def create(cls, config_dict, ctx):
        config = LegacySourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for legacy_ping in get_legacy_pings():
            legacy_qualified_urn = builder.make_dataset_urn(
                platform=self.platform,
                name=legacy_ping.name,
                env=self.config.env,
            )
            legacy_ping_aspects = [
                SubTypesClass(typeNames=["Ping"]),
                BrowsePathsClass(
                    paths=[f"/{self.config.env.lower()}/legacy/{legacy_ping.name}"]
                ),
            ]
            legacy_ping_mcps = MetadataChangeProposalWrapper.construct_many(
                entityUrn=legacy_qualified_urn, aspects=legacy_ping_aspects
            )

            upstream_lineage = UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=legacy_qualified_urn,
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
                for qualified_table_name in legacy_ping.bigquery_fully_qualified_names
            ]
            for mcp in legacy_ping_mcps + upstream_lineage_mcps:
                wu = mcp.as_workunit()
                yield wu

    def get_report(self) -> StaleEntityRemovalSourceReport:
        return self.report
