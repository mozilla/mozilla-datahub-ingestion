import logging
import os
import time

import datahub.emitter.mce_builder as builder
from avrogen.dict_wrapper import DictWrapper
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.metadata.schema_classes import (
    AuditStampClass,
    ChangeTypeClass,
    DatasetPropertiesClass,
    InstitutionalMemoryClass,
    InstitutionalMemoryMetadataClass,
    SubTypesClass,
    UpstreamClass,
    UpstreamLineageClass,
)

from sync.glean import get_glean_pings

log = logging.getLogger(__name__)


def _get_current_timestamp() -> AuditStampClass:
    now = int(time.time() * 1000)  # milliseconds since epoch
    return AuditStampClass(time=now, actor="urn:li:corpuser:ingestion")


def _make_mcp_wrapper(
    urn: str, aspect_name: str, aspect: DictWrapper
) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityType="dataset",
        changeType=ChangeTypeClass.UPSERT,
        entityUrn=urn,
        aspectName=aspect_name,
        aspect=aspect,
    )


def glean_emitter():
    log.info("Running the Glean emitter")

    emitter = DataHubRestEmitter(
        gms_server=os.environ["DATAHUB_GMS_URL"],
        token=os.environ["DATAHUB_GMS_TOKEN"],
    )
    for glean_ping in get_glean_pings():
        glean_qualified_urn = builder.make_dataset_urn(
            platform="Glean", name=glean_ping.qualified_name, env="PROD"
        )

        # link out to the Glean Dictionary for more information
        institutional_memory = InstitutionalMemoryClass(
            elements=[
                InstitutionalMemoryMetadataClass(
                    url=glean_ping.glean_dictionary_url,
                    description="Glean Dictionary Ping Documentation",
                    createStamp=_get_current_timestamp(),
                )
            ],
        )
        institutional_memory_mcp = _make_mcp_wrapper(
            glean_qualified_urn, "institutionalMemory", institutional_memory
        )

        # add ping description to documentation
        dataset_properties = DatasetPropertiesClass(
            name=glean_ping.name, description=glean_ping.description
        )
        dataset_properties_mcp = _make_mcp_wrapper(
            glean_qualified_urn, "datasetProperties", dataset_properties
        )

        # mark the dataset type as Ping
        ping_type = SubTypesClass(typeNames=["Ping"])
        ping_type_mcp = _make_mcp_wrapper(glean_qualified_urn, "subTypes", ping_type)

        # mark the upstream lineage of BigQuery live tables as Glean ping
        upstream_lineage = UpstreamLineageClass(
            upstreams=[
                UpstreamClass(
                    dataset=glean_qualified_urn,
                    type="TRANSFORMED",
                )
            ]
        )
        upstream_lineage_mcps = [
            _make_mcp_wrapper(
                builder.make_dataset_urn(
                    platform="bigquery", name=qualified_table_name, env="PROD"
                ),
                "upstreamLineage",
                upstream_lineage,
            )
            for qualified_table_name in glean_ping.bigquery_fully_qualified_names
        ]

        emitter.emit(institutional_memory_mcp)
        emitter.emit(dataset_properties_mcp)
        emitter.emit(ping_type_mcp)

        for upstream_lineage_mcp in upstream_lineage_mcps:
            emitter.emit(upstream_lineage_mcp)

        # TODO: Add metrics as schema fields


def legacy_telemetry_emitter():
    log.info("Running the Legacy Telemetry emitter")


def bigquery_legacy_telemetry_lineage_emitter():
    log.info("Running the BigQuery Legacy Telemetry Lineage Emitter")


EMIT_FUNCTIONS = {
    "glean": glean_emitter,
    "legacy_telemetry": legacy_telemetry_emitter,
    "bigquery_legacy_lineage": bigquery_legacy_telemetry_lineage_emitter,
}


def run_emitter(emitter_name: str, dump_to_file: bool) -> None:
    # TODO: Emit based on dump_to_file for debugging.
    emit_function = EMIT_FUNCTIONS[emitter_name]
    emit_function()
