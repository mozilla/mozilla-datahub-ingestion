from typing import Iterable

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.emitter.mcp import MetadataChangeProposalWrapper, ChangeTypeClass
import datahub.emitter.mce_builder as builder
from datahub.metadata.schema_classes import (
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
)
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.source_helpers import (
    auto_workunit_reporter,
)

from sync.bigquery_etl import get_bigquery_etl_table_references
from sync.datahub.utils import get_current_timestamp


class BigQueryEtlSourceConfig(ConfigModel):
    env: str = "PROD"


class BigQueryEtlSource(Source):
    def __init__(self, config: BigQueryEtlSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.report = SourceReport()
        self.platform = "bigquery"

    @classmethod
    def create(cls, config_dict, ctx):
        config = BigQueryEtlSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        return auto_workunit_reporter(self.report, self.get_workunits_internal())

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        for qualified_table_name, urls in get_bigquery_etl_table_references().items():

            bigquery_qualified_urn = builder.make_dataset_urn(
                platform=self.platform,
                name=qualified_table_name,
                env=self.source_config.env,
            )

            link_elements = []
            if "wtmo_url" in urls:
                link_elements.append(
                    InstitutionalMemoryMetadataClass(
                        url=urls["wtmo_url"],
                        description="Airflow DAG",
                        createStamp=get_current_timestamp(),
                    )
                )

            if "bigquery_etl_url" in urls:
                link_elements.append(
                    InstitutionalMemoryMetadataClass(
                        url=urls["bigquery_etl_url"],
                        description="BigQuery-ETL Source Code",
                        createStamp=get_current_timestamp(),
                    )
                )

            if not link_elements:
                continue

            mcp = MetadataChangeProposalWrapper(
                changeType=ChangeTypeClass.UPSERT,  # Should probably be UPDATE but this isn't supported
                entityUrn=bigquery_qualified_urn,
                aspect=InstitutionalMemoryClass(elements=link_elements),
            )

            wu = mcp.as_workunit()
            yield wu

    def get_report(self) -> SourceReport:
        return self.report
