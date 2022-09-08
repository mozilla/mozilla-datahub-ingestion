import json
import requests
import logging
import os
from datetime import datetime
import time

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

GLEAN_DICTIONARY_URL = "https://dictionary.telemetry.mozilla.org"

log = logging.getLogger(__name__)


def getCurrentTimestamp(time: int):
    return AuditStampClass(time=time, actor="urn:li:corpuser:ingestion")


def glean_emitter(dump_to_file: bool = False):
    apps = requests.get(f"{GLEAN_DICTIONARY_URL}/data/apps.json").json()
    emitter = DataHubRestEmitter(
        gms_server=os.environ["DATAHUB_GMS_URL"],
        token=os.environ["DATAHUB_GMS_TOKEN"],
    )
    for app in apps:
        app_name = app["app_name"]
        app_data = requests.get(
            f"{GLEAN_DICTIONARY_URL}/data/{app_name}/index.json"
        ).json()
        pings = app_data["pings"]
        now = int(time.time() * 1000)  # milliseconds since epoch

        for ping in pings:
            ping_name = ping["name"]
            # link out to the Glean Dictionary for more information
            institutional_memory_element = InstitutionalMemoryMetadataClass(
                url=f"{GLEAN_DICTIONARY_URL}/apps/{app_name}/pings/{ping_name}",
                description="Glean Dictionary Ping Documentation",
                createStamp=getCurrentTimestamp(now),
            )
            ping_metadata: MetadataChangeProposalWrapper = (
                MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=builder.make_dataset_urn(
                        platform="Glean", name=f"{app_name}.{ping_name}", env="PROD"
                    ),
                    aspectName="institutionalMemory",
                    aspect=InstitutionalMemoryClass(
                        elements=[institutional_memory_element],
                    ),
                )
            )

            # add ping description to documentation
            dataset_properties = DatasetPropertiesClass(
                name=ping_name, description=ping["description"]
            )
            ping_properties: MetadataChangeProposalWrapper = (
                MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=builder.make_dataset_urn(
                        platform="Glean", name=f"{app_name}.{ping_name}", env="PROD"
                    ),
                    aspectName="datasetProperties",
                    aspect=dataset_properties,
                )
            )

            # mark the dataset type as Ping
            ping_type = SubTypesClass(typeNames=["Ping"])
            ping_type_proposal: MetadataChangeProposalWrapper = (
                MetadataChangeProposalWrapper(
                    entityType="dataset",
                    changeType=ChangeTypeClass.UPSERT,
                    entityUrn=builder.make_dataset_urn(
                        platform="Glean", name=f"{app_name}.{ping_name}", env="PROD"
                    ),
                    aspectName="subTypes",
                    aspect=ping_type,
                )
            )

            emitter.emit(ping_metadata)
            emitter.emit(ping_properties)
            emitter.emit(ping_type_proposal)

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
