# Welcome to your CDK Python project!

This is a blank project for CDK development with Python.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up like a standard Python project.  The initialization
process also creates a virtualenv within this project, stored under the `.venv`
directory.  To create the virtualenv it assumes that there is a `python3`
(or `python` for Windows) executable in your path with access to the `venv`
package. If for any reason the automatic creation of the virtualenv fails,
you can create the virtualenv manually.

## Setup

### Create and activate virtualenv

To manually create a virtualenv on MacOS and Linux:

```
$ python3 -m venv .venv
```

After the init process completes and the virtualenv is created, you can use the following
step to activate your virtualenv.

```
$ source .venv/bin/activate
```

If you are a Windows platform, you would activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

### Install dependencies

Once the virtualenv is activated, you can install the required dependencies.

```
$ pip install -r requirements.txt
```

### Create boto3 Lambda Layer

Before deploying the CDK stack, you need to create the boto3 Lambda Layer that will be used by the Lambda functions. This layer contains the latest version of boto3, which may be required for certain AWS API calls.

Run the following command to create the boto3 layer:

```
$ python create_boto3_layer.py
```

This script will:
1. Create the necessary directory structure (`lambda_layers/boto3/python`)
2. Install the latest version of boto3 into this directory
3. Prepare the layer for deployment with the CDK stack

### Deploy the stack

At this point you can now synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To deploy the stack to your AWS account:

```
$ cdk deploy
```

To add additional dependencies, for example other CDK libraries, just add
them to your `setup.py` file and rerun the `pip install -r requirements.txt`
command.

## Project Structure

- `lambda/` - Contains Lambda function code
  - `index.py` - Lambda function for processing S3 events
  - `glue_catalog_handler.py` - Lambda function for managing Glue Federated Catalog
  - `cfnresponse.py` - Helper module for CloudFormation custom resources
- `lambda_layers/` - Contains Lambda layers
  - `boto3/` - Layer with latest boto3 version
- `emr_job/` - Contains EMR job scripts
- `s3table_cdk/` - Contains CDK stack definition

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

## Troubleshooting

### Lambda Layer Issues

If you encounter issues with the Lambda Layer:

1. Make sure you've run `python create_boto3_layer.py` before deploying
2. Check that the `lambda_layers/boto3/python` directory contains boto3 and its dependencies
3. If you update the boto3 version, you may need to run the script again and redeploy

### Path Issues

If you encounter path-related errors during deployment:

1. Make sure you're running CDK commands from the `cdk` directory
2. Verify that the paths in `s3table_cdk_stack.py` are correct relative to where you're running the commands
3. Use absolute paths if necessary to avoid confusion

Enjoy!
