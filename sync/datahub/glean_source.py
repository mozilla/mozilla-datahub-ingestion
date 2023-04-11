import time
from typing import Iterable

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.emitter.mcp import MetadataChangeProposalWrapper, ChangeTypeClass
import datahub.emitter.mce_builder as builder
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    AuditStampClass,
    DatasetPropertiesClass,
    SubTypesClass,
    UpstreamLineageClass,
    UpstreamClass,
)

from sync.glean import get_glean_pings


def _get_current_timestamp() -> AuditStampClass:
    now = int(time.time() * 1000)  # milliseconds since epoch
    return AuditStampClass(time=now, actor="urn:li:corpuser:ingestion")


class GleanSourceConfig(ConfigModel):
    env: str = "PROD"


class GleanSource(Source):
    source_config: GleanSourceConfig
    report: SourceReport = SourceReport()

    def __init__(self, config: GleanSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config

    @classmethod
    def create(cls, config_dict, ctx):
        config = GleanSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        for glean_ping in get_glean_pings():
            glean_qualified_urn = builder.make_dataset_urn(
                platform="Glean",
                name=glean_ping.qualified_name,
                env=self.source_config.env,
            )
            glean_ping_aspects = [
                InstitutionalMemoryClass(
                    elements=[
                        InstitutionalMemoryMetadataClass(
                            url=glean_ping.glean_dictionary_url,
                            description="Glean Dictionary Ping Documentation",
                            createStamp=_get_current_timestamp(),
                        )
                    ],
                ),
                DatasetPropertiesClass(
                    name=glean_ping.name, description=glean_ping.description
                ),
                SubTypesClass(typeNames=["Ping"]),
                BrowsePathsClass(
                    paths=[
                        f"/{self.source_config.env.lower()}/glean/{glean_ping.app_name}"
                    ]
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
                        env=self.source_config.env,
                    ),
                    aspectName="upstreamLineage",
                    aspect=upstream_lineage,
                )
                for qualified_table_name in glean_ping.bigquery_fully_qualified_names
            ]

            for mcp in glean_ping_mcps + upstream_lineage_mcps:
                wu = mcp.as_workunit()
                self.report.report_workunit(wu)
                yield wu

    def get_report(self) -> SourceReport:
        return self.report

    def close(self) -> None:
        pass
