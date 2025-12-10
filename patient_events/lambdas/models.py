from typing import Any


class PatientEventChange(BaseModel):
    value: Any


class PatientEvent(BaseModel):
    id: UUID
    resourceType: Literal["patient", "practitioner", "report"]
    resourceId: UUID
    eventType: Literal[
        "ATTRIBUTE_UPDATED",
        "DISENROLLED",
        "PATIENT_PROFILE_UPDATED"
    ]
    changes: Dict[str, PatientEventChange]

    programYearName: Optional[str] = None
    programTags: Optional[List[str]] = None

    occurredAt: datetime
    actorId: UUID

    # DynamoDB-specific
    PK: str
    SK: str
