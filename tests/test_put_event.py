import uuid
import boto3
from datetime import datetime, timezone
import time
import random

TABLE_NAME = "Sam-PatientEventsPOC-PatientEvents7A491A55-R3LW4MCL063J"

table = boto3.resource("dynamodb").Table(TABLE_NAME)

# Configuration for load testing
NUM_PATIENTS = 1000  # Number of different patients
EVENTS_PER_PATIENT = 10  # Number of events per patient
TOTAL_EVENTS = NUM_PATIENTS * EVENTS_PER_PATIENT

print(f"Starting load test: writing {TOTAL_EVENTS:,} events for {NUM_PATIENTS} patients...")

sources = ["USER", "ETL"]
event_types = ["USER_ATTRIBUTE_UPDATE", "ETL_UPDATE", "ETL_ROLLBACK"]
program_tags = ["Centene", "Humana", "UnitedHealth", "Aetna", "Cigna"]
update_field_types = ["email", "phone", "address", "status", "emergency_contact"]

def generate_random_change_data(field_type):
    """Generate random change data based on field type"""
    if field_type == "email":
        return {"email": {"value": f"patient{random.randint(1, 9999)}@example.com"}}
    elif field_type == "phone":
        return {"phone": {"value": f"+1{random.randint(1000000000, 9999999999)}"}}
    elif field_type == "address":
        return {"address": {"value": f"{random.randint(100, 9999)} Main St, City, State"}}
    elif field_type == "status":
        return {"status": {"value": random.choice(["active", "inactive", "pending"])}}
    elif field_type == "emergency_contact":
        return {"emergency_contact": {"value": f"Emergency Contact {random.randint(1, 100)}"}}
    else:
        return {"misc": {"value": "default_value"}}

def generate_multiple_changes():
    """Generate a random number of changes (1-5) with different field types"""
    num_changes = random.randint(1, 5)  # Random number of changes between 1 and 5
    selected_fields = random.sample(update_field_types, min(num_changes, len(update_field_types)))

    changes = {}
    for field_type in selected_fields:
        field_data = generate_random_change_data(field_type)
        changes.update(field_data)

    return changes

successful_writes = 0
failed_writes = 0

start_time = time.time()

# Generate and write events
for patient_id in range(1, NUM_PATIENTS + 1):
    patient_id_str = f"{patient_id:05d}"  # Zero-padded to 5 digits

    for event_num in range(EVENTS_PER_PATIENT):
        try:
            # Add small time offset to ensure unique timestamps
            event_time = datetime.now(timezone.utc)
            time_offset = random.uniform(-86400, 0)  # Random offset up to 24 hours ago
            event_time = datetime.fromtimestamp(event_time.timestamp() + time_offset, tz=timezone.utc)

            event = {
                "PK": f"PATIENT#{patient_id_str}",
                "SK": f"EVENT#{event_time.isoformat()}#{event_num:03d}",
                "id": str(uuid.uuid4()),
                "resourceType": "patient",
                "resourceId": patient_id_str,
                "eventType": random.choice(event_types),
                "changes": generate_multiple_changes(),
                "programYear": "2025",
                "programTag": random.choice(program_tags),
                "occurredAt": event_time.isoformat(),
                "updatedBy": f"user-{random.randint(100000, 999999)}",
                "source": random.choice(sources),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }

            table.put_item(Item=event)
            successful_writes += 1

            # Progress indicator
            if (patient_id * EVENTS_PER_PATIENT + event_num + 1) % 100 == 0:
                elapsed = time.time() - start_time
                rate = successful_writes / elapsed if elapsed > 0 else 0
                print(f"Progress: {successful_writes:,}/{TOTAL_EVENTS:,} events written ({rate:.1f} events/sec)")

        except Exception as e:
            failed_writes += 1
            print(f"Error writing event for patient {patient_id_str}: {e}")

end_time = time.time()
elapsed_time = end_time - start_time

print("\nLoad test completed!")
print(f"Total time: {elapsed_time:.2f} seconds")
print(f"Successful writes: {successful_writes:,}")
print(f"Failed writes: {failed_writes:,}")
print(f"Average write rate: {successful_writes / elapsed_time:.1f} events/sec")
