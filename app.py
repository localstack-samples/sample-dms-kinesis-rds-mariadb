import os
import aws_cdk as cdk

from dms_sample.stack import DmsSampleStack

STACK_NAME = os.getenv("STACK_NAME", "")

app = cdk.App()
DmsSampleStack(app, STACK_NAME)

app.synth()
