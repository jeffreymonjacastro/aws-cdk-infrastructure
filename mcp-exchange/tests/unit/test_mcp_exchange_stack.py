import aws_cdk as core
import aws_cdk.assertions as assertions

from mcp_exchange.mcp_exchange_stack import McpExchangeStack

# example tests. To run these tests, uncomment this file along with the example
# resource in mcp_exchange/mcp_exchange_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = McpExchangeStack(app, "mcp-exchange")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
