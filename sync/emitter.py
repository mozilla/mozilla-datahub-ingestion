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
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
)

GLEAN_DICTIONARY_URL = "https://dictionary.telemetry.mozilla.org"

log = logging.getLogger(__name__)

def getCurrentTimestamp(time: int):
    return AuditStampClass(time=time, actor="urn:li:corpuser:ingestion")

def glean_emitter(dump_to_file: bool = False):
    apps = requests.get(f"{GLEAN_DICTIONARY_URL}/data/apps.json").json()

    for app in apps:
        app_name = app["app_name"]
        app_data = requests.get(
            f"{GLEAN_DICTIONARY_URL}/data/{app_name}/index.json"
        ).json()
        pings = app_data["pings"]
        now = int(time.time() * 1000)  # milliseconds since epoch
        # emitter = DataHubRestEmitter(
        #     gms_server=os.environ["DATAHUB_GMS_URL"],
        #     token=os.environ["DATAHUB_GMS_TOKEN"],
        # )
        emitter = DataHubRestEmitter(
            gms_server="http://localhost:8080", token="datahub"
        )
        for ping in pings:
            ping_name = ping["name"]
            ping_data = requests.get(
                f"{GLEAN_DICTIONARY_URL}/data/{app_name}/pings/{ping_name}.json"
            ).json()
            institutional_memory_element = InstitutionalMemoryMetadataClass(
                url=f"{GLEAN_DICTIONARY_URL}/apps/{app_name}/pings/{ping_name}",
                description=ping["description"],
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
            emitter.emit(ping_metadata)

            metric_fields = []
            for metric in ping_data["metrics"]:
                field = SchemaFieldClass(
                    fieldPath=metric["name"],
                    type=SchemaFieldDataTypeClass(type=StringTypeClass()),
                    nativeDataType="VARCHAR(50)",  # use this to provide the type of the field in the source system's vernacular
                    description=metric["description"],
                    lastModified=getCurrentTimestamp(now),
                )
                metric_fields.append(field)
            ping_metric_fields: MetadataChangeProposalWrapper = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType=ChangeTypeClass.UPSERT,
                entityUrn=builder.make_dataset_urn(
                    platform="Glean", name=f"{app_name}.{ping_name}", env="PROD"
                ),
                aspectName="schemaMetadata",
                aspect=SchemaMetadataClass(
                    schemaName=f"{app_name}.{ping_name}",
                    platform=builder.make_data_platform_urn(
                        "Glean"
                    ),
                    version=0,  # when the source system has a notion of versioning of schemas, insert this in, otherwise leave as 0
                    hash="",  # when the source system has a notion of unique schemas identified via hash, include a hash, else leave it as empty string
                    platformSchema=OtherSchemaClass(
                        rawSchema="__insert raw schema here__"
                    ),
                    lastModified=getCurrentTimestamp(now),
                    fields=metric_fields,
                ),
            )
            emitter.emit(ping_metric_fields)
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
