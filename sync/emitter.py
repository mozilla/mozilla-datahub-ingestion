import json
import logging
import os
from datetime import datetime

import datahub.emitter.mce_builder as builder
from datahub.emitter.rest_emitter import DataHubRestEmitter

log = logging.getLogger(__name__)


def glean_emitter(dump_to_file: bool = False):
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
