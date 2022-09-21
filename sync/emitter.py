import json
import logging
import os
from datetime import datetime
import time

from avrogen.dict_wrapper import DictWrapper
import datahub.emitter.mce_builder as builder
from datahub.emitter.rest_emitter import DataHubRestEmitter
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    InstitutionalMemoryClass,
    AuditStampClass,
    ChangeTypeClass,
    InstitutionalMemoryMetadataClass,
    DatasetPropertiesClass,
    SubTypesClass,
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


def glean_emitter(dump_to_file: bool = False):
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
        ping_type_mcp = _make_mcp_wrapper(
            glean_qualified_urn, "subTypes", ping_type
        )

        emitter.emit(institutional_memory_mcp)
        emitter.emit(dataset_properties_mcp)
        emitter.emit(ping_type_mcp)

        # TODO: Add metrics as schema fields
        # TODO: Add lineage

    log.info("Running the Glean emitter")


def legacy_telemetry_emitter(dump_to_file: bool = False):
    log.info("Running the Legacy Telemetry emitter")


def bigquery_glean_lineage_emitter(dump_to_file: bool = False):
    log.info("Running the BigQuery Glean Lineage emitter")

    lineage_mce = builder.make_lineage_mce(
        upstream_urns=[
            builder.make_dataset_urn("Glean", "test-app.main"),
        ],
        downstream_urn=builder.make_dataset_urn(
            "bigquery", "anich-sandbox.ping_live.test_app_main"
        ),
    )

    if dump_to_file:
        static_filename = f"bigquery_glean_lineage-{datetime.now()}.test.json"
        with open(static_filename, "w") as f:
            json.dump(lineage_mce.to_obj(), f, indent=4)
        return

    emitter = DataHubRestEmitter(
        gms_server=os.environ["DATAHUB_GMS_URL"], token=os.environ["DATAHUB_GMS_TOKEN"]
    )
    emitter.emit_mce(lineage_mce)


def bigquery_legacy_telemetry_lineage_emitter(dump_to_file: bool = False):
    log.info("Running the BigQuery Legacy Telemetry Lineage Emitter")


EMITTERS = {
    "glean": glean_emitter,
    "legacy_telemetry": legacy_telemetry_emitter,
    "bigquery_glean_lineage": bigquery_glean_lineage_emitter,
    "bigquery_legacy_lineage": bigquery_legacy_telemetry_lineage_emitter,
}


def run_emitter(emitter_name: str, dump_to_file: bool) -> None:
    emitter = EMITTERS[emitter_name]
    emitter(dump_to_file=dump_to_file)
