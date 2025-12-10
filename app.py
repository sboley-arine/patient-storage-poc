#!/usr/bin/env python3

import aws_cdk as cdk
from patient_events.stack import PatientEventsStack

app = cdk.App()
PatientEventsStack(app, "Sam-PatientEventsPOC")

app.synth()
