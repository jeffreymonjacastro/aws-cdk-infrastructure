''' CDK App principal '''

from dotenv import load_dotenv
load_dotenv()

import aws_cdk as cdk

from infra.ibk_mcp_stack import IbkMcpStack


app = cdk.App()

IbkMcpStack(app, "IbkMcpStack")

app.synth()
