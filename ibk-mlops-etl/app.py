''' CDK Application '''

from dotenv import load_dotenv
load_dotenv()

import aws_cdk as cdk
from stacks.main_stack import MainStack

app = cdk.App()

MainStack(app, "IbkMlopsMainStack")

app.synth()
