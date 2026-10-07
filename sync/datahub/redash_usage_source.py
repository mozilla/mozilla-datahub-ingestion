"""Usage statistics and owners for the Redash charts and dashboards in DataHub.

The Redash source only writes chartInfo and dashboardInfo, so this source writes the
aspects it never touches: chartUsageStatistics, dashboardUsageStatistics, and patches to
ownership and structuredProperties. Counts come from the STMO views in bigquery-etl.

Charts and dashboards both get view counts over the last 90 days. DataHub doesn't display
chart usage statistics, so chart views, users, and top users are also written to structured
properties, defined in recipes/redash_structured_properties.json. Dashboards get the views
property too, since the dashboard page labels its count "Total Views" without saying 90 days.
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
from datahub.specific.aspect_helpers.structured_properties import (
    HasStructuredPropertiesPatch,
)
from datahub.specific.chart import ChartPatchBuilder
from datahub.specific.dashboard import DashboardPatchBuilder
from google.cloud import bigquery
from google.oauth2 import service_account

PLATFORM = "redash"
VIEWS_PROPERTY = "urn:li:structuredProperty:mozilla.redash.views_90d"
CHART_USERS_PROPERTY = "urn:li:structuredProperty:mozilla.redash.users_90d"
CHART_TOP_USERS_PROPERTY = "urn:li:structuredProperty:mozilla.redash.top_users_90d"
# Same as the dashboard page's top users
TOP_USER_COUNT = 5
# The 90 complete days before the run, for dashboard totals and chart properties. The view
# covers more than this, so objects whose views stop still get 0 instead of keeping old values.
LAST_90_DAYS = """
    submission_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
"""


class RedashUsageSourceConfig(ConfigModel):
    # Project to run BigQuery jobs in
    billing_project: str = "moz-fx-data-datahub"
    # Same fields as the BigQuery source's `credential`, so the recipe can reuse its secrets.
    # Falls back to application default credentials when unset, for local runs.
    credential: Optional[GCPCredential] = None
    # Daily buckets re-emitted each run
    lookback_days: int = 30
    emit_ownership: bool = True
    # Needs the property definitions applied first, or DataHub rejects the patches
    emit_structured_properties: bool = True
    # Overridable so a test recipe can read sandbox copies
    views_table: str = "moz-fx-data-shared-prod.stmo.object_views_daily"
    owners_table: str = "moz-fx-data-shared-prod.stmo.object_owners"


@dataclass
class RedashUsageSourceReport(SourceReport):
    charts_in_datahub: int = 0
    dashboards_in_datahub: int = 0
    # Distinct entities in the BigQuery results that DataHub doesn't have, e.g. drafts
    entities_not_in_datahub: int = 0
    daily_usage_aspects: int = 0
    total_usage_aspects: int = 0
    structured_property_patches: int = 0
    ownership_patches: int = 0


class _ChartPatchBuilder(HasStructuredPropertiesPatch, ChartPatchBuilder):
    pass


class _DashboardPatchBuilder(HasStructuredPropertiesPatch, DashboardPatchBuilder):
    pass


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

    def _urn(self, object_type: str, object_id: int) -> str:
        # The Redash source ingests each visualization as a chart
        if object_type == "visualization":
            return make_chart_urn(PLATFORM, str(object_id))
        return make_dashboard_urn(PLATFORM, str(object_id))

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
            SELECT object_type, object_id, submission_date, user_email, views, last_viewed_at
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
            urn = self._urn(row["object_type"], row["object_id"])
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

        totals_query = f"""
            SELECT object_type, object_id,
              SUM(IF({LAST_90_DAYS}, views, 0)) AS views_90d,
              MAX(last_viewed_at) AS last_viewed_at
            FROM `{self.config.views_table}`
            GROUP BY object_type, object_id
        """
        chart_views_90d: Dict[str, int] = {}
        for row in client.query(totals_query).result():
            urn = self._urn(row["object_type"], row["object_id"])
            if urn not in known_urns:
                missing_urns.add(urn)
                continue
            if urn in chart_urns:
                chart_views_90d[urn] = row["views_90d"]
                continue
            # The dashboard page shows the latest of these as "Total Views"
            aspect = DashboardUsageStatisticsClass(
                timestampMillis=run_millis,
                viewsCount=row["views_90d"],
                lastViewedAt=_to_millis(row["last_viewed_at"]),
            )
            yield self._workunit(
                MetadataChangeProposalWrapper(entityUrn=urn, aspect=aspect)
            )
            self.report.total_usage_aspects += 1
            if self.config.emit_structured_properties:
                patch = _DashboardPatchBuilder(urn).set_structured_property(
                    VIEWS_PROPERTY, float(row["views_90d"])
                )
                for mcp in patch.build():
                    yield self._workunit(mcp)
                self.report.structured_property_patches += 1

        if self.config.emit_structured_properties:
            yield from self._chart_property_workunits(client, chart_views_90d)

        if self.config.emit_ownership:
            owners_query = f"""
                SELECT object_type, object_id, owner_email
                FROM `{self.config.owners_table}`
            """
            for row in client.query(owners_query).result():
                urn = self._urn(row["object_type"], row["object_id"])
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

    def _chart_property_workunits(
        self, client: bigquery.Client, views_90d: Dict[str, int]
    ) -> Iterable[MetadataWorkUnit]:
        users_query = f"""
            SELECT object_id, user_email, SUM(views) AS views
            FROM `{self.config.views_table}`
            WHERE object_type = 'visualization' AND user_email IS NOT NULL AND {LAST_90_DAYS}
            GROUP BY object_id, user_email
        """
        # urn -> user email -> views
        users: Dict[str, Dict[str, int]] = collections.defaultdict(dict)
        for row in client.query(users_query).result():
            urn = self._urn("visualization", row["object_id"])
            if urn in views_90d:
                users[urn][row["user_email"]] = row["views"]

        for urn, views in sorted(views_90d.items()):
            top_users = sorted(
                users[urn].items(), key=lambda item: (-item[1], item[0])
            )[:TOP_USER_COUNT]
            patch = (
                _ChartPatchBuilder(urn)
                .set_structured_property(VIEWS_PROPERTY, float(views))
                .set_structured_property(CHART_USERS_PROPERTY, float(len(users[urn])))
            )
            if top_users:
                patch.set_structured_property(
                    CHART_TOP_USERS_PROPERTY,
                    [make_user_urn(email) for email, _ in top_users],
                )
            else:
                # Clear users from an earlier window
                patch.remove_structured_property(CHART_TOP_USERS_PROPERTY)
            for mcp in patch.build():
                yield self._workunit(mcp)
            self.report.structured_property_patches += 1

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
