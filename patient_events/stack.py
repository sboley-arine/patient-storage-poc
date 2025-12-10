from aws_cdk import (
    Stack,
    Duration,
    aws_lambda as _lambda,
    aws_dynamodb as dynamodb,
    aws_iam as iam,
)
from constructs import Construct
import os

class PatientEventsStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # -----------------------------
        # 1. PatientEvents (Event Store)
        # -----------------------------
        event_store = dynamodb.Table(
            self, "PatientEvents",
            partition_key=dynamodb.Attribute(name="PK", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="SK", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            stream=dynamodb.StreamViewType.NEW_IMAGE
        )

        # -----------------------------
        # 2. PatientCurrentState
        # -----------------------------
        current_state = dynamodb.Table(
            self, "PatientCurrentState",
            partition_key=dynamodb.Attribute(name="PK", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="SK", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )

        # -----------------------------
        # 3. PatientAttributeLastUpdated
        # -----------------------------
        attribute_last_updated = dynamodb.Table(
            self, "PatientAttributeLastUpdated",
            partition_key=dynamodb.Attribute(name="PK", type=dynamodb.AttributeType.STRING),
            sort_key=dynamodb.Attribute(name="SK", type=dynamodb.AttributeType.STRING),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
        )

        # -----------------------------
        # 4. Projector Lambda
        # -----------------------------
        projector_fn = _lambda.Function(
            self, "ProjectorLambda",
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="projector.handler",
            timeout=Duration.seconds(30),
            code=_lambda.Code.from_asset(os.path.join("patient_events", "lambdas")),
            environment={
                "CURRENT_STATE_TABLE": current_state.table_name,
                "ATTR_LAST_UPDATED_TABLE": attribute_last_updated.table_name,
            }
        )

        # Lambda reads from stream
        event_store.grant_stream_read(projector_fn)
        projector_fn.add_event_source_mapping(
            "EventStoreStreamMapping",
            event_source_arn=event_store.table_stream_arn,
            starting_position=_lambda.StartingPosition.LATEST,
            batch_size=100,
        )

        # Lambda can write to read models
        current_state.grant_write_data(projector_fn)
        attribute_last_updated.grant_write_data(projector_fn)
