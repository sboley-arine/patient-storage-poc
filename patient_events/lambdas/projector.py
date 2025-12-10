import os
import boto3
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")
current_state_table = dynamodb.Table(os.environ["CURRENT_STATE_TABLE"])
attribute_last_updated_table = dynamodb.Table(os.environ["ATTR_LAST_UPDATED_TABLE"])


def handler(event, context):
    """
    Handles DynamoDB stream events produced by PatientEvents table.
    Writes to:
      - PatientCurrentState
      - PatientAttributeLastUpdated
    """

    current_state_items = []
    attribute_last_updated_items = []

    for record in event.get("Records", []):
        if record["eventName"] not in ("INSERT", "MODIFY"):
            continue

        new_img = record["dynamodb"]["NewImage"]
        print("New image from stream:", new_img)

        event_item = _deserialize(new_img)

        print("Processing event item:", event_item)

        resource_type = event_item["resourceType"]
        resource_id = event_item["resourceId"]
        changes = event_item["changes"]
        program_year = event_item.get("programYearName")
        program_tag = event_item.get("programTag", "")
        updated_by = event_item.get("updatedBy", "")
        source = event_item.get("source", "")

        occurred_at = event_item["occurredAt"]

        pk = f"{resource_type}#{resource_id}"

        # --------------------------------------------
        # 1. Prepare PatientCurrentState items
        # --------------------------------------------
        for attr, payload in changes.items():
            value = payload.get("value")

            current_state_items.append({
                "PK": pk,
                "SK": f"ATTR#{attr}",
                "attribute": attr,
                "value": value,
                "occurredAt": occurred_at,
                "updatedBy": updated_by,
            })

        # --------------------------------------------
        # 2. Prepare PatientAttributeLastUpdated items (history + latest)
        # --------------------------------------------
        for attr, payload in changes.items():
            value = payload.get("value")

            # Historical record with timestamp in SK
            attribute_last_updated_items.append({
                "PK": pk,
                "SK": f"ATTR#{attr}#{occurred_at}",
                "attribute": attr,
                "value": value,
                "programYearName": program_year,
                "programTag": program_tag,
                "occurredAt": occurred_at,
                "updatedBy": updated_by,
                "recordType": "HISTORY",
            })

            # Latest record for fast get_item lookups
            attribute_last_updated_items.append({
                "PK": pk,
                "SK": f"ATTR#{attr}#LATEST",
                "attribute": attr,
                "value": value,
                "programYearName": program_year,
                "programTag": program_tag,
                "occurredAt": occurred_at,
                "updatedBy": updated_by,
                "recordType": "LATEST",
            })

            if source:
                attribute_last_updated_items.append({
                    "PK": pk,
                    "SK": f"ATTR#{attr}#LATEST#{source}",
                    "attribute": attr,
                    "value": value,
                    "programYearName": program_year,
                    "programTag": program_tag,
                    "occurredAt": occurred_at,
                    "updatedBy": updated_by,
                    "recordType": "LATEST",
                })

    # --------------------------------------------
    # 3. Deduplicate and batch write using Table.batch_writer()
    # --------------------------------------------
    if current_state_items:
        deduplicated_current_state = _deduplicate_items(current_state_items)
        _batch_write_table(current_state_table, deduplicated_current_state)

    # PatientAttributeLastUpdated needs deduplication for LATEST records only
    # (HISTORY records with timestamps are naturally unique)
    if attribute_last_updated_items:
        deduplicated_attr_updated = _deduplicate_latest_records_only(attribute_last_updated_items)
        _batch_write_table(attribute_last_updated_table, deduplicated_attr_updated)


def _batch_write_table(table, items):
    """
    Writes items to DynamoDB using high-level batch_writer.
    Handles batching automatically.
    """
    with table.batch_writer() as batch:
        for item in items:
            batch.put_item(Item=item)


def _deduplicate_items(items):
    """
    Deduplicates items by PK+SK, keeping the most recent update (latest occurredAt).
    This prevents batch write failures when multiple updates affect the same key.
    """
    key_to_item = {}

    for item in items:
        key = (item["PK"], item["SK"])

        # If we haven't seen this key before, or this item is more recent, keep it
        if (key not in key_to_item or
                item["occurredAt"] > key_to_item[key]["occurredAt"]):
            key_to_item[key] = item

    return list(key_to_item.values())


def _deduplicate_latest_records_only(items):
    """
    Deduplicates only LATEST records by PK+SK, while preserving all HISTORY records.
    HISTORY records have unique timestamps in SK so they never conflict.
    LATEST records can conflict when multiple updates hit the same attribute.
    """
    history_records = []
    latest_records_by_key = {}

    for item in items:
        record_type = item.get("recordType", "HISTORY")

        if record_type == "HISTORY":
            # Always keep HISTORY records - they have unique timestamps
            history_records.append(item)
        elif record_type == "LATEST":
            # Deduplicate LATEST records by PK+SK
            key = (item["PK"], item["SK"])

            if (key not in latest_records_by_key or
                    item["occurredAt"] > latest_records_by_key[key]["occurredAt"]):
                latest_records_by_key[key] = item

    # Combine all history records with deduplicated latest records
    return history_records + list(latest_records_by_key.values())


def _deserialize(d):
    """
    Recursively convert DynamoDB JSON from streams to native Python types.
    """
    if not isinstance(d, dict):
        return d

    # DynamoDB type wrapper check
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

    # Normal Python dict (top-level item)
    return {k: _deserialize(v) for k, v in d.items()}
