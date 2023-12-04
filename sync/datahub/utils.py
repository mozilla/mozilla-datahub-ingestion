import time

from datahub.metadata.schema_classes import AuditStampClass


def get_current_timestamp() -> AuditStampClass:
    now = int(time.time() * 1000)  # milliseconds since epoch
    return AuditStampClass(time=now, actor="urn:li:corpuser:ingestion")
