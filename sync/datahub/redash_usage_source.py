"""Usage statistics and owners for the Redash charts and dashboards in DataHub.

The Redash source only writes chartInfo and dashboardInfo, so this source writes the
aspects it never touches: chartUsageStatistics, dashboardUsageStatistics, and ownership
(as a patch). Counts come from the STMO views in bigquery-etl.
"""

import collections
import datetime
import time
from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Set

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import (
    make_chart_urn,
    make_dashboard_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.gcp_credentials_config import GCPCredential
from datahub.metadata.schema_classes import (
    CalendarIntervalClass,
    ChartUsageStatisticsClass,
    ChartUserUsageCountsClass,
    DashboardUsageStatisticsClass,
    DashboardUserUsageCountsClass,
    OwnerClass,
    OwnershipSourceClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
    TimeWindowSizeClass,
)
from datahub.specific.chart import ChartPatchBuilder
from datahub.specific.dashboard import DashboardPatchBuilder
from google.cloud import bigquery
from google.oauth2 import service_account

PLATFORM = "redash"


class RedashUsageSourceConfig(ConfigModel):
    # Project to run BigQuery jobs in
    billing_project: str = "moz-fx-data-datahub"
    # Same fields as the BigQuery source's `credential`, so the recipe can reuse its secrets.
    # Falls back to application default credentials when unset, for local runs.
    credential: Optional[GCPCredential] = None
    # Daily buckets re-emitted each run
    lookback_days: int = 30
    emit_ownership: bool = True
    # Overridable so a test recipe can read sandbox copies
    views_table: str = "moz-fx-data-shared-prod.monitoring.stmo_entity_views_daily"
    owners_table: str = "moz-fx-data-shared-prod.monitoring.stmo_entity_owners"


@dataclass
class RedashUsageSourceReport(SourceReport):
    charts_in_datahub: int = 0
    dashboards_in_datahub: int = 0
    # Distinct entities in the BigQuery results that DataHub doesn't have, e.g. drafts
    entities_not_in_datahub: int = 0
    daily_usage_aspects: int = 0
    total_usage_aspects: int = 0
    ownership_patches: int = 0


@dataclass
class _Bucket:
    views: int = 0
    last_viewed_at: Optional[datetime.datetime] = None


def _to_millis(value: datetime.datetime) -> int:
    return int(value.timestamp() * 1000)


def _day_millis(day: datetime.date) -> int:
    return _to_millis(
        datetime.datetime.combine(day, datetime.time.min, tzinfo=datetime.timezone.utc)
    )


def _later(
    a: Optional[datetime.datetime], b: Optional[datetime.datetime]
) -> Optional[datetime.datetime]:
    if a is None or b is None:
        return a or b
    return max(a, b)


class RedashUsageSource(Source):
    def __init__(self, config: RedashUsageSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.config = config
        self.report = RedashUsageSourceReport()
        self.platform = PLATFORM

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "RedashUsageSource":
        config = RedashUsageSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def _bigquery_client(self) -> bigquery.Client:
        credentials = None
        if self.config.credential is not None:
            credentials = service_account.Credentials.from_service_account_info(
                self.config.credential.to_dict()
            )
        return bigquery.Client(
            project=self.config.billing_project, credentials=credentials
        )

    def _entity_urn(self, entity_type: str, entity_id: int) -> str:
        if entity_type == "chart":
            return make_chart_urn(PLATFORM, str(entity_id))
        return make_dashboard_urn(PLATFORM, str(entity_id))

    def _workunit(self, mcp) -> MetadataWorkUnit:
        # Not the primary source, so DataHub doesn't add status or browse path aspects
        # to entities the Redash source owns
        if isinstance(mcp, MetadataChangeProposalWrapper):
            return mcp.as_workunit(is_primary_source=False)
        return MetadataWorkUnit(
            id=f"{mcp.entityUrn}-{mcp.aspectName}-patch",
            mcp_raw=mcp,
            is_primary_source=False,
        )

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        run_millis = int(time.time() * 1000)

        # Only write to entities the Redash source has ingested, so drafts, archived, and
        # denied objects never get stub entities
        graph = self.ctx.require_graph("Redash usage source")
        chart_urns = set(
            graph.get_urns_by_filter(entity_types=["chart"], platform=PLATFORM)
        )
        dashboard_urns = set(
            graph.get_urns_by_filter(entity_types=["dashboard"], platform=PLATFORM)
        )
        self.report.charts_in_datahub = len(chart_urns)
        self.report.dashboards_in_datahub = len(dashboard_urns)
        known_urns = chart_urns | dashboard_urns
        missing_urns: Set[str] = set()

        client = self._bigquery_client()

        # (urn, day) -> user email (None when unknown) -> bucket
        daily: Dict[tuple, Dict[Optional[str], _Bucket]] = collections.defaultdict(
            lambda: collections.defaultdict(_Bucket)
        )
        daily_query = f"""
            SELECT entity_type, entity_id, submission_date, user_email, views, last_viewed_at
            FROM `{self.config.views_table}`
            WHERE submission_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback_days DAY)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "lookback_days", "INT64", self.config.lookback_days
                )
            ]
        )
        for row in client.query(daily_query, job_config=job_config).result():
            urn = self._entity_urn(row["entity_type"], row["entity_id"])
            if urn not in known_urns:
                missing_urns.add(urn)
                continue
            bucket = daily[(urn, row["submission_date"])][row["user_email"]]
            bucket.views += row["views"]
            bucket.last_viewed_at = _later(bucket.last_viewed_at, row["last_viewed_at"])

        for (urn, day), users in sorted(daily.items()):
            yield self._workunit(
                MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=self._usage_aspect(urn, _day_millis(day), users),
                )
            )
            self.report.daily_usage_aspects += 1

        # One absolute aspect per entity, over everything the view covers
        totals_query = f"""
            SELECT entity_type, entity_id, SUM(views) AS views,
              MAX(last_viewed_at) AS last_viewed_at
            FROM `{self.config.views_table}`
            GROUP BY entity_type, entity_id
        """
        for row in client.query(totals_query).result():
            urn = self._entity_urn(row["entity_type"], row["entity_id"])
            if urn not in known_urns:
                missing_urns.add(urn)
                continue
            if urn in chart_urns:
                aspect = ChartUsageStatisticsClass(
                    timestampMillis=run_millis, viewsCount=row["views"]
                )
            else:
                aspect = DashboardUsageStatisticsClass(
                    timestampMillis=run_millis,
                    viewsCount=row["views"],
                    lastViewedAt=_to_millis(row["last_viewed_at"]),
                )
            yield self._workunit(
                MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect)
            )
            self.report.total_usage_aspects += 1

        if self.config.emit_ownership:
            owners_query = f"""
                SELECT entity_type, entity_id, owner_email
                FROM `{self.config.owners_table}`
            """
            for row in client.query(owners_query).result():
                urn = self._entity_urn(row["entity_type"], row["entity_id"])
                if urn not in known_urns:
                    missing_urns.add(urn)
                    continue
                builder_class = (
                    ChartPatchBuilder if urn in chart_urns else DashboardPatchBuilder
                )
                patch = builder_class(urn).add_owner(
                    OwnerClass(
                        owner=make_user_urn(row["owner_email"]),
                        type=OwnershipTypeClass.TECHNICAL_OWNER,
                        source=OwnershipSourceClass(
                            type=OwnershipSourceTypeClass.SERVICE
                        ),
                    )
                )
                for mcp in patch.build():
                    yield self._workunit(mcp)
                self.report.ownership_patches += 1

        self.report.entities_not_in_datahub = len(missing_urns)

    def _usage_aspect(
        self, urn: str, timestamp_millis: int, users: Dict[Optional[str], _Bucket]
    ):
        views = sum(bucket.views for bucket in users.values())
        # Rows with no email count toward views but not users
        known_users = sorted(
            ((email, bucket) for email, bucket in users.items() if email is not None),
            key=lambda item: (-item[1].views, item[0]),
        )
        granularity = TimeWindowSizeClass(unit=CalendarIntervalClass.DAY, multiple=1)

        if urn.startswith("urn:li:chart:"):
            return ChartUsageStatisticsClass(
                timestampMillis=timestamp_millis,
                eventGranularity=granularity,
                viewsCount=views,
                uniqueUserCount=len(known_users),
                userCounts=[
                    ChartUserUsageCountsClass(
                        user=make_user_urn(email), viewsCount=bucket.views
                    )
                    for email, bucket in known_users
                ],
            )

        last_viewed_at: Optional[datetime.datetime] = None
        for bucket in users.values():
            last_viewed_at = _later(last_viewed_at, bucket.last_viewed_at)
        return DashboardUsageStatisticsClass(
            timestampMillis=timestamp_millis,
            eventGranularity=granularity,
            viewsCount=views,
            uniqueUserCount=len(known_users),
            userCounts=[
                DashboardUserUsageCountsClass(
                    user=make_user_urn(email), viewsCount=bucket.views, userEmail=email
                )
                for email, bucket in known_users
            ],
            lastViewedAt=_to_millis(last_viewed_at) if last_viewed_at else None,
        )

    def get_report(self) -> SourceReport:
        return self.report
