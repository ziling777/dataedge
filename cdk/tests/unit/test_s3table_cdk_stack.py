import aws_cdk as cdk
from aws_cdk.assertions import Template
import aws_cdk.assertions as assertions

from s3table_cdk.s3table_cdk_stack import S3TableCdkStack

# example tests. To run these tests, uncomment this file along with the example
# resource in s3table_cdk/s3table_cdk_stack.py
def test_sqs_queue_created():
    app = cdk.App()
    stack = S3TableCdkStack(app, "s3table-cdk")
    template = Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
