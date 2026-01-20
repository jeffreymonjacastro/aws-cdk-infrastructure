import aws_cdk as core
import aws_cdk.assertions as assertions

from ibk_mlops_etl.ibk_mlops_etl_stack import IbkMlopsEtlStack

# example tests. To run these tests, uncomment this file along with the example
# resource in ibk_mlops_etl/ibk_mlops_etl_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = IbkMlopsEtlStack(app, "ibk-mlops-etl")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
