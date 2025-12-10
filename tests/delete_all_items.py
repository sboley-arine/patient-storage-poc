import boto3

TABLE_NAME = "Sam-PatientEventsPOC-PatientEvents7A491A55-R3LW4MCL063J"

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)

# Extract PK + SK field names from table metadata
key_attrs = [k["AttributeName"] for k in table.key_schema]

scan_kwargs = {}
items = []

# Scan entire table
while True:
    resp = table.scan(**scan_kwargs)
    items.extend(resp.get("Items", []))

    if "LastEvaluatedKey" not in resp:
        break

    scan_kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]

print(f"Found {len(items)} items to delete.")

# Batch delete
with table.batch_writer() as batch:
    for item in items:
        key = {attr: item[attr] for attr in key_attrs}
        batch.delete_item(Key=key)

print("All items deleted.")
