from typing import Iterable, List, Optional

import datahub.emitter.mce_builder as builder
from pydantic import Field, SecretStr
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BrowsePathsClass,
    ChangeAuditStampsClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    EdgeClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    StatusClass,
    SubTypesClass,
)

from sync.datahub.utils import get_current_timestamp
from sync.quick import (
    QUICK_CLI_SERVICE_ACCOUNT,
    QuickSite,
    get_iap_token,
    get_quick_dashboards,
)


class QuickSourceConfig(StatefulIngestionConfigBase):
    env: str = "PROD"
    service_account_key: Optional[SecretStr] = Field(
        default=None,
        description=(
            "Service account key JSON, used to mint the Quick IAP token. "
            "Unset falls back to QUICK_IAP_TOKEN, then to gcloud."
        ),
    )
    impersonate_service_account: Optional[str] = Field(
        default=QUICK_CLI_SERVICE_ACCOUNT,
        description=(
            "Account to impersonate when minting the token. Set to null if the "
            "key's own account is allowed through Quick's IAP."
        ),
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None


class QuickSource(StatefulIngestionSourceBase):
    def __init__(self, config: QuickSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.platform = "Quick"

    def get_platform_instance_id(self) -> str:
        return f"{self.platform}"

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext):
        config = QuickSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def _deploy_stamp(self, site: QuickSite) -> AuditStampClass:
        stamp = get_current_timestamp()
        if site.updated is not None:
            stamp.time = site.updated
        if site.deployed_by:
            stamp.actor = builder.make_user_urn(site.deployed_by)
        return stamp

    def _dataset_urn(self, table_name: str) -> str:
        return builder.make_dataset_urn(
            platform="bigquery", name=table_name, env=self.config.env
        )

    def _token(self) -> str:
        key = self.config.service_account_key
        return get_iap_token(
            service_account_key=key.get_secret_value() if key else None,
            impersonate=self.config.impersonate_service_account,
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        dashboards, errors = get_quick_dashboards(self._token())
        for error in errors:
            # Reported as a failure so stale entity removal skips this run.
            # A site we could not read is not a site that was deleted.
            self.report.report_failure(
                title="Could not read a Quick site",
                message="Stale dashboards will not be soft-deleted this run.",
                context=str(error),
            )

        for site in dashboards:
            dashboard_urn = builder.make_dashboard_urn(
                platform=self.platform, name=site.name
            )
            stamp = self._deploy_stamp(site)
            dataset_urns = [
                self._dataset_urn(table_name)
                for table_name in site.bigquery_fully_qualified_names
            ]

            aspects = [
                DashboardInfoClass(
                    title=site.title or site.name,
                    description=site.description or "",
                    externalUrl=site.url,
                    dashboardUrl=site.url,
                    lastModified=ChangeAuditStampsClass(
                        created=stamp, lastModified=stamp
                    ),
                    customProperties={
                        "quick_site": site.name,
                        "deployed_by": site.deployed_by or "",
                        "scripts_scanned": str(len(site.scripts)),
                        "runtime_table_refs": str(len(site.runtime_table_refs)),
                    },
                    datasets=dataset_urns,
                    datasetEdges=[
                        EdgeClass(destinationUrn=urn, created=stamp)
                        for urn in dataset_urns
                    ],
                ),
                DataPlatformInstanceClass(
                    platform=builder.make_data_platform_urn(self.platform)
                ),
                SubTypesClass(typeNames=["Dashboard"]),
                BrowsePathsClass(paths=[f"/{self.config.env.lower()}/quick"]),
                StatusClass(removed=False),
            ]
            if site.deployed_by:
                aspects.append(
                    OwnershipClass(
                        owners=[
                            OwnerClass(
                                owner=builder.make_user_urn(site.deployed_by),
                                type=OwnershipTypeClass.TECHNICAL_OWNER,
                            )
                        ],
                        lastModified=get_current_timestamp(),
                    )
                )

            for mcp in MetadataChangeProposalWrapper.construct_many(
                entityUrn=dashboard_urn, aspects=aspects
            ):
                yield mcp.as_workunit()

    def get_report(self) -> StaleEntityRemovalSourceReport:
        return self.report
