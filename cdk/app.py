#!/usr/bin/env python3
from aws_cdk import App
from s3table_cdk.s3table_cdk_stack import S3TableCdkStack

app = App()
S3TableCdkStack(app, "S3TableCdkStack")
app.synth()
