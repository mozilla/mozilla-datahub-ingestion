"""Prod dashboards (`tier: prod` in metadata.yaml) served as Streamlit apps from Deepnote."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stale_entity_removal_handler import (
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
    GlossaryTermAssociationClass,
    OwnerClass,
    OwnershipSourceClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
    SubTypesClass,
)
from datahub.specific.dashboard import DashboardPatchBuilder
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result
from pydantic import Field, SecretStr

from sync.datahub.utils import get_current_timestamp
from sync.deepnote import PROD_TIER, DeepnoteApp, get_deepnote_apps

PLATFORM = "Deepnote"
SOURCE_URL = "https://github.com/mozilla/mozilla-datahub-ingestion"
DEFAULT_PROJECT = "mozdata"
RESTRICTED = "restricted"


def _epoch_ms(timestamp: Optional[str]) -> Optional[int]:
    if not timestamp:
        return None
    try:
        parsed = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp() * 1000)


class DeepnoteSourceConfig(StatefulIngestionConfigBase):
    env: str = "PROD"
    api_key: Optional[SecretStr] = Field(
        default=None,
        description="API key for the Mozilla workspace. Defaults to DEEPNOTE_API_KEY.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = None


@dataclass
class DeepnoteSourceReport(StaleEntityRemovalSourceReport):
    apps_found: int = 0
    dashboards_emitted: int = 0
    skipped_without_metadata: int = 0
    skipped_not_prod: int = 0
    skipped_restricted: List[str] = field(default_factory=list)
    without_lineage: List[str] = field(default_factory=list)
    unknown_metric_terms: List[str] = field(default_factory=list)


class DeepnoteSource(StatefulIngestionSourceBase):
    def __init__(self, config: DeepnoteSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report: DeepnoteSourceReport = DeepnoteSourceReport()
        self.platform = PLATFORM

    def get_platform_instance_id(self) -> str:
        return self.platform

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext):
        config = DeepnoteSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _lineage(self, app: DeepnoteApp) -> List[str]:
        if app.sql:
            parsed = create_lineage_sql_parsed_result(
                app.sql,
                default_db=DEFAULT_PROJECT,
                platform="bigquery",
                platform_instance=None,
                env=self.config.env,
            )
            if parsed.in_tables and not parsed.debug_info.table_error:
                return sorted(set(parsed.in_tables))
        self.report.without_lineage.append(app.key)
        return []

    def _properties(self, app: DeepnoteApp) -> Dict[str, str]:
        properties = {
            "deepnote_app_id": app.id,
            "deepnote_project_id": app.project_id,
            "deepnote_project_name": app.project_name,
            "entrypoint": app.entrypoint,
        }
        for name, path in (
            ("folder", None),
            ("sensitivity", ("sensitivity",)),
            ("review_status", ("review_status",)),
            ("generated_by", ("generated_by",)),
            ("grounding", ("grounding", "source")),
            ("drift_status", ("drift_check", "status")),
            ("drift_checked_at", ("drift_check", "last_checked")),
            ("promoted_at", ("promotion", "promoted_at")),
        ):
            value = app.folder_path if path is None else app.metadata_value(*path)
            if value:
                properties[name] = value
        return properties

    def _term_exists(self, term_urn: str) -> bool:
        if self.ctx.graph is None:
            return True
        return self.ctx.graph.exists(term_urn)

    def _workunits(self, app: DeepnoteApp) -> Iterable[MetadataWorkUnit]:
        urn = builder.make_dashboard_urn(platform=self.platform, name=app.key)
        dataset_urns = self._lineage(app)

        now = get_current_timestamp()
        stamp = get_current_timestamp()
        created = _epoch_ms(app.metadata_value("created_at")) or _epoch_ms(
            app.created_at
        )
        if created is not None:
            stamp.time = created

        folders = [part for part in (app.folder_path or "").split("/") if part]
        browse_path = "/" + "/".join([self.config.env.lower(), "deepnote", *folders])

        aspects = [
            DashboardInfoClass(
                title=app.title,
                description="",
                externalUrl=app.url,
                dashboardUrl=app.url,
                lastModified=ChangeAuditStampsClass(created=stamp, lastModified=now),
                customProperties=self._properties(app),
                datasetEdges=[
                    EdgeClass(destinationUrn=dataset_urn, created=stamp)
                    for dataset_urn in dataset_urns
                ],
            ),
            DataPlatformInstanceClass(
                platform=builder.make_data_platform_urn(self.platform)
            ),
            SubTypesClass(typeNames=["Streamlit app"]),
            BrowsePathsClass(paths=[browse_path]),
        ]
        for mcp in MetadataChangeProposalWrapper.construct_many(
            entityUrn=urn, aspects=aspects
        ):
            yield mcp.as_workunit()

        patch = DashboardPatchBuilder(urn)
        changed = False
        if app.owner_email:
            patch.add_owner(
                OwnerClass(
                    owner=builder.make_user_urn(app.owner_email),
                    type=OwnershipTypeClass.TECHNICAL_OWNER,
                    source=OwnershipSourceClass(
                        type=OwnershipSourceTypeClass.SERVICE, url=SOURCE_URL
                    ),
                )
            )
            changed = True
        if app.metric_term:
            term_urn = builder.make_term_urn(app.metric_term)
            if self._term_exists(term_urn):
                patch.add_term(GlossaryTermAssociationClass(urn=term_urn))
                changed = True
            else:
                self.report.unknown_metric_terms.append(f"{app.key}: {term_urn}")
        if changed:
            for mcp in patch.build():
                yield MetadataWorkUnit(id=f"{urn}-patch-{mcp.aspectName}", mcp_raw=mcp)

    def _skip(self, app: DeepnoteApp) -> bool:
        if app.metadata is None:
            self.report.skipped_without_metadata += 1
            return True
        if app.tier != PROD_TIER:
            self.report.skipped_not_prod += 1
            return True
        if app.sensitivity == RESTRICTED:
            self.report.skipped_restricted.append(app.key)
            return True
        return False

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        key = self.config.api_key
        apps, errors = get_deepnote_apps(
            api_key=key.get_secret_value() if key else None
        )
        for error in errors:
            # A failure stops stale entity removal for this run.
            self.report.failure(
                title="Could not read from the Deepnote API",
                message="Stale dashboards will not be soft-deleted this run.",
                context=str(error),
            )

        for app in apps:
            self.report.apps_found += 1
            if app.metadata_error:
                self.report.failure(
                    title="Could not parse metadata.yaml",
                    message=app.metadata_error,
                    context=app.key,
                )
            if self._skip(app):
                continue
            self.report.dashboards_emitted += 1
            yield from self._workunits(app)

    def get_report(self) -> DeepnoteSourceReport:
        return self.report
