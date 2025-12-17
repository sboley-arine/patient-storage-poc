import os
import boto3
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")

current_state_table = dynamodb.Table(os.environ["CURRENT_STATE_TABLE"])
attribute_last_updated_table = dynamodb.Table(os.environ["ATTR_LAST_UPDATED_TABLE"])


def handler(event, context):
    """
    Handles DynamoDB stream events from PatientEvents.

    Writes to:
      - PatientCurrentState (latest profile values)
      - PatientAttributeLastUpdated
          * SNAP#  - canonical latest
          * EVT#   - latest by event type
          * SRC#   - latest by source
          * HIST#  - append-only attribute history
    """

    attribute_index_items = []

    for record in event.get("Records", []):
        if record["eventName"] not in ("INSERT", "MODIFY"):
            continue

        new_img = record["dynamodb"]["NewImage"]
        event_item = _deserialize(new_img)

        resource_type = event_item["resourceType"]
        resource_id = event_item["resourceId"]
        event_type = event_item["eventType"]
        changes = event_item["changes"]

        occurred_at = event_item["occurredAt"]
        actor_id = event_item.get("actorId")
        source = event_item.get("source")
        program_year = event_item.get("programYear")
        program_tag = event_item.get("programTag")

        pk = f"{resource_type}#{resource_id}"

        # -------------------------------------------------
        # PatientAttributeLastUpdated
        # -------------------------------------------------
        for attr, payload in changes.items():
            value = payload.get("value")

            # --- LATEST (canonical latest) ---
            attribute_index_items.append({
                "PK": pk,
                "SK": f"LATEST#{attr}",
                "attribute": attr,
                "value": value,
                "eventType": event_type,
                "actorId": actor_id,
                "source": source,
                "occurredAt": occurred_at,
            })

            # --- EVENT (latest by event type) ---
            attribute_index_items.append({
                "PK": pk,
                "SK": f"EVENT#{event_type}#{attr}",
                "attribute": attr,
                "value": value,
                "eventType": event_type,
                "actorId": actor_id,
                "source": source,
                "occurredAt": occurred_at,
            })

            # --- SOURCE (latest by source, if present) ---
            if source:
                attribute_index_items.append({
                    "PK": pk,
                    "SK": f"SOURCE#{source}#{attr}",
                    "attribute": attr,
                    "value": value,
                    "eventType": event_type,
                    "actorId": actor_id,
                    "source": source,
                    "occurredAt": occurred_at,
                })

            if program_year:
                program_key = f"{program_year}#{program_tag}" if program_tag else f"{program_year}"
                attribute_index_items.append({
                    "PK": pk,
                    "SK": f"PROGRAM#{program_key}#{attr}",
                    "attribute": attr,
                    "value": value,
                    "eventType": event_type,
                    "actorId": actor_id,
                    "source": source,
                    "occurredAt": occurred_at,
                })

            # --- HISTORY (append-only attribute history) ---
            attribute_index_items.append({
                "PK": pk,
                "SK": f"HISTORY#{attr}#{occurred_at}",
                "attribute": attr,
                "value": value,
                "eventType": event_type,
                "actorId": actor_id,
                "source": source,
                "occurredAt": occurred_at,
            })

            # -- HISTORY (by program year)


    # -------------------------------------------------
    # Batch writes (batch_writer handles retries & size)
    # -------------------------------------------------
    if attribute_index_items:
        _batch_write(attribute_last_updated_table, _dedupe_overwrite_records(attribute_index_items))


def _dedupe_overwrite_records(items):
    """
    Deduplicates SNAP / EVT / SRC records by PK+SK.
    Preserves all HIST records.
    """
    overwrite = {}
    history = []

    for item in items:
        if item["SK"].startswith("HISTORY#"):
            history.append(item)
            continue

        key = (item["PK"], item["SK"])
        if key not in overwrite or item["occurredAt"] > overwrite[key]["occurredAt"]:
            overwrite[key] = item

    return history + list(overwrite.values())


def _batch_write(table, items):
    """
    Writes items using DynamoDB batch_writer.
    Overwrites are intentional for SNAP / EVT / SRC items.
    """
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)


def _deserialize(d):
    """
    Recursively convert DynamoDB Streams JSON to native Python types.
    """
    if not isinstance(d, dict):
        return d

    if "S" in d:
        return d["S"]
    if "N" in d:
        return Decimal(d["N"])
    if "BOOL" in d:
        return d["BOOL"]
    if "NULL" in d:
        return None
    if "L" in d:
        return [_deserialize(v) for v in d["L"]]
    if "M" in d:
        return {k: _deserialize(v) for k, v in d["M"].items()}

    return {k: _deserialize(v) for k, v in d.items()}
