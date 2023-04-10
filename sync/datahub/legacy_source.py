from typing import Iterable

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.emitter.mcp import MetadataChangeProposalWrapper, ChangeTypeClass
import datahub.emitter.mce_builder as builder
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    SubTypesClass,
    UpstreamLineageClass,
    UpstreamClass,
)

from sync.legacy import get_legacy_pings


class LegacySourceConfig(ConfigModel):
    env: str = "PROD"


class LegacySource(Source):
    source_config: LegacySourceConfig
    report: SourceReport = SourceReport()

    def __init__(self, config: LegacySourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config

    @classmethod
    def create(cls, config_dict, ctx):
        config = LegacySourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        for legacy_ping in get_legacy_pings():
            legacy_qualified_urn = builder.make_dataset_urn(
                platform="LegacyTelemetry",
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
                self.report.report_workunit(wu)
                yield wu

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass
