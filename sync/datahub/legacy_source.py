from typing import Iterable, Optional

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.emitter.mcp import MetadataChangeProposalWrapper, ChangeTypeClass
import datahub.emitter.mce_builder as builder
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    SubTypesClass,
    UpstreamLineageClass,
    UpstreamClass,
)
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
    StaleEntityRemovalSourceReport,
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.api.source_helpers import (
    auto_stale_entity_removal,
    auto_workunit_reporter,
)

from sync.legacy import get_legacy_pings


class LegacySourceConfig(StatefulIngestionConfigBase):
    env: str = "PROD"
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None


class LegacySource(StatefulIngestionSourceBase):

    source_config: LegacySourceConfig
    report: StaleEntityRemovalSourceReport

    def __init__(self, config: LegacySourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.platform = "LegacyTelemetry"
        self.report = StaleEntityRemovalSourceReport()

        self.stale_entity_removal_handler = StaleEntityRemovalHandler(
            source=self,
            config=self.source_config,
            state_type_class=GenericCheckpointState,
            pipeline_name=self.ctx.pipeline_name,
            run_id=self.ctx.run_id,
        )

    def get_platform_instance_id(self) -> str:
        return f"{self.platform}"

    @classmethod
    def create(cls, config_dict, ctx):
        config = LegacySourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        return auto_stale_entity_removal(
            self.stale_entity_removal_handler,
            auto_workunit_reporter(
                self.report,
                self.get_workunits_internal(),
            ),
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for legacy_ping in get_legacy_pings():
            legacy_qualified_urn = builder.make_dataset_urn(
                platform=self.platform,
                name=legacy_ping.name,
                env=self.source_config.env,
            )
            legacy_ping_aspects = [
                SubTypesClass(typeNames=["Ping"]),
                BrowsePathsClass(
                    paths=[
                        f"/{self.source_config.env.lower()}/legacy/{legacy_ping.name}"
                    ]
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
                        env=self.source_config.env,
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
