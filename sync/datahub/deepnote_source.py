from datetime import datetime, timezone
from typing import Iterable, List, Optional

import datahub.emitter.mce_builder as builder
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
    BrowsePathsClass,
    ChangeAuditStampsClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    EdgeClass,
    StatusClass,
    SubTypesClass,
)
from pydantic import Field, SecretStr

from sync.datahub.utils import get_current_timestamp
from sync.deepnote import DeepnoteApp, get_deepnote_apps


def _epoch_ms(timestamp: Optional[str]) -> Optional[int]:
    if not timestamp:
        return None
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


class DeepnoteSourceConfig(StatefulIngestionConfigBase):
    env: str = "PROD"
    api_key: Optional[SecretStr] = Field(
        default=None,
        description="Deepnote API key. Unset falls back to DEEPNOTE_API_KEY.",
    )
    with_status: bool = Field(
        default=True,
        description=(
            "Fetch each app's serving state. One extra call per app. Apps are only "
            "served while their project machine runs, so this is often 'unavailable' "
            "and is recorded rather than used to filter."
        ),
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None


class DeepnoteSource(StatefulIngestionSourceBase):
    def __init__(self, config: DeepnoteSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.platform = "Deepnote"

    def get_platform_instance_id(self) -> str:
        return f"{self.platform}"

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext):
        config = DeepnoteSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def _dataset_urn(self, table_name: str) -> str:
        return builder.make_dataset_urn(
            platform="bigquery", name=table_name, env=self.config.env
        )

    def _aspects(self, app: DeepnoteApp):
        stamp = get_current_timestamp()
        created = _epoch_ms(app.created_at)
        if created is not None:
            stamp.time = created
        dataset_urns = [
            self._dataset_urn(name) for name in app.bigquery_fully_qualified_names
        ]
        properties = {
            "deepnote_app_id": app.id,
            "deepnote_project_id": app.project_id,
            "deepnote_project_name": app.project_name,
            "entrypoint": app.entrypoint,
            "runtime_table_refs": str(len(app.runtime_table_refs)),
        }
        if app.status:
            properties["serving_status"] = app.status
        if app.folder_path:
            properties["folder"] = app.folder_path

        return [
            DashboardInfoClass(
                title=app.title,
                # Deepnote has no description field on any resource, so this is
                # always empty. Curate in the DataHub UI instead.
                description="",
                externalUrl=app.url,
                dashboardUrl=app.url,
                lastModified=ChangeAuditStampsClass(created=stamp, lastModified=stamp),
                customProperties=properties,
                datasets=dataset_urns,
                datasetEdges=[
                    EdgeClass(destinationUrn=urn, created=stamp) for urn in dataset_urns
                ],
            ),
            DataPlatformInstanceClass(
                platform=builder.make_data_platform_urn(self.platform)
            ),
            SubTypesClass(typeNames=["Streamlit app"]),
            BrowsePathsClass(paths=[f"/{self.config.env.lower()}/deepnote"]),
            StatusClass(removed=False),
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        key = self.config.api_key
        apps, errors = get_deepnote_apps(
            api_key=key.get_secret_value() if key else None,
            with_status=self.config.with_status,
        )
        for error in errors:
            # Reported as a failure so stale entity removal skips this run.
            # A call that failed is not an app that was deleted.
            self.report.report_failure(
                title="Could not read from the Deepnote API",
                message="Stale apps will not be soft-deleted this run.",
                context=str(error),
            )

        for app in apps:
            urn = builder.make_dashboard_urn(platform=self.platform, name=app.id)
            for mcp in MetadataChangeProposalWrapper.construct_many(
                entityUrn=urn, aspects=self._aspects(app)
            ):
                yield mcp.as_workunit()

    def get_report(self) -> StaleEntityRemovalSourceReport:
        return self.report
