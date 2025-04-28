from aws_cdk import (
    Duration,
    Stack,
    RemovalPolicy,
    CfnOutput,
    Aws,
    aws_s3 as s3,
    aws_s3tables as s3tables,
    aws_sqs as sqs,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_event_sources,
    aws_s3_notifications as s3n,
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_glue as glue,
    aws_lakeformation as lakeformation,
    aws_emrserverless as emrs,
    aws_athena as athena,
    aws_s3_deployment as s3deploy,
    custom_resources as cr,
    CustomResource
)
from constructs import Construct
import time

class S3TableCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # S3 存储桶 - 用于存储原始数据、处理后的数据和最终表数据
        data_bucket = s3.Bucket(
            self, "TelematicsDataUploadBucket",
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            versioned=True
        )

        # SQS 队列 - 接收S3事件通知
        data_queue = sqs.Queue(
            self, "TelematicsDataDecodingQueue",
            visibility_timeout=Duration.seconds(300)
        )

        # 配置S3桶发送事件到SQS
        data_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(data_queue),
            s3.NotificationKeyFilter(prefix="raw/", suffix=".zip")
        )

        # 创建 lambda Layer
        lambda_layer = lambda_.LayerVersion(
            self, "greptime layer",
            code=lambda_.Code.from_asset("lambda_layers/"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_13],
            compatible_architectures=[lambda_.Architecture.X86_64, lambda_.Architecture.ARM_64],
            description="Layer containing greptime required packages"
        )

        # Lambda函数 - 处理SQS消息
        processing_lambda = lambda_.Function(
            self, "ProcessingFunction",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="index.handler",
            code=lambda_.Code.from_asset("lambda"),
            timeout=Duration.seconds(300),
            memory_size=1024,
            layers=[lambda_layer],
            environment={
                "S3_BUCKET": data_bucket.bucket_name
            }
        )

        # 将SQS作为Lambda的事件源
        processing_lambda.add_event_source(
            lambda_event_sources.SqsEventSource(data_queue)
        )

        # 给Lambda授予S3读写权限
        data_bucket.grant_read_write(processing_lambda)

        # 添加HeadObject权限
        processing_lambda.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:HeadObject"],
                resources=[data_bucket.arn_for_objects("*")]
            )
        )

        # EC2实例 - 生成数据并上传到S3
        vpc = ec2.Vpc(
            self, "DataGenerationVPC",
            max_azs=2,
            nat_gateways=1,  # 添加NAT网关
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24
                )
            ]
        )

        # EC2 IAM角色与策略
        ec2_role = iam.Role(
            self, "EC2Role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com")
        )
        
        data_bucket.grant_read_write(ec2_role)

        # EC2实例
        instance = ec2.Instance(
            self, "DataGenerationInstance",
            vpc=vpc,
            instance_type=ec2.InstanceType.of(
                ec2.InstanceClass.BURSTABLE3, 
                ec2.InstanceSize.MEDIUM
            ),
            machine_image=ec2.AmazonLinuxImage(
                generation=ec2.AmazonLinuxGeneration.AMAZON_LINUX_2023
            ),
            role=ec2_role,
            vpc_subnets=ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PUBLIC  # 指定使用公有子网
            ),
            associate_public_ip_address=True  # 启用公网IP
        )

        # EMR Serverless应用程序
        emr_execution_role = iam.Role(
            self, "EMRServerlessExecutionRole",
            assumed_by=iam.ServicePrincipal("emr-serverless.amazonaws.com")
        )
        
        data_bucket.grant_read_write(emr_execution_role)
        
        # 添加Glue管理员权限
        emr_execution_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["glue:*"],  # 授予所有Glue权限
                resources=["*"]
            )
        )
        
        # 输出EMR执行角色的ARN
        CfnOutput(
            self, "EMRExecutionRoleArn",
            value=emr_execution_role.role_arn,
            description="ARN of the EMR Serverless execution role"
        )
        
        emr_app = emrs.CfnApplication(
            self, "DataProcessingEMRApp",
            release_label="emr-7.7.0",
            type="SPARK",
            name="DataProcessingApp",
            initial_capacity=[
                emrs.CfnApplication.InitialCapacityConfigKeyValuePairProperty(
                    key="DRIVER",
                    value=emrs.CfnApplication.InitialCapacityConfigProperty(
                        worker_count=1,
                        worker_configuration=emrs.CfnApplication.WorkerConfigurationProperty(
                            cpu="4vCPU",
                            memory="16GB"
                        )
                    )
                ),
                emrs.CfnApplication.InitialCapacityConfigKeyValuePairProperty(
                    key="EXECUTOR",
                    value=emrs.CfnApplication.InitialCapacityConfigProperty(
                        worker_count=4,
                        worker_configuration=emrs.CfnApplication.WorkerConfigurationProperty(
                            cpu="4vCPU",
                            memory="16GB"
                        )
                    )
                )
            ],
            maximum_capacity={
                "cpu": "200vCPU",
                "memory": "800GB"
            }
        )

        # S3 Table Bucket
        cfn_table_bucket = s3tables.CfnTableBucket(
            self, "caredge-demo-s3table-bucket",
            table_bucket_name = "caredge-demo-s3table-bucket"
        )

        s3tables_lakeformation_role_policy = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            principals=[iam.ServicePrincipal("lakeformation.amazonaws.com")],
            actions=[
                "sts:SetContext",
                "sts:SetSourceIdentity"
            ],
            conditions={
                "StringEquals": {
                    "aws:SourceAccount": Aws.ACCOUNT_ID
                }
            }
        )

        # 创建Lake Formation用于访问S3 Tables的IAM角色
        s3tables_lakeformation_role = iam.Role(
            self, "S3TablesRoleForLakeFormationDemo",
            role_name="S3TablesRoleForLakeFormationDemo",
            assumed_by=iam.ServicePrincipal("lakeformation.amazonaws.com")
        )

        s3tables_lakeformation_role.assume_role_policy.add_statements(
            s3tables_lakeformation_role_policy
        )

        # 添加S3 Tables列表权限 - 这是身份策略，不要指定principals
        s3tables_lakeformation_role.add_to_policy(
            iam.PolicyStatement(
                sid="LakeFormationPermissionsForS3ListTableBucket",
                effect=iam.Effect.ALLOW,
                actions=["s3tables:ListTableBuckets"],
                resources=["*"]  # 已经指定了资源
            )
        )

        # 添加S3 Tables数据访问权限 - 这是身份策略，不要指定principals
        s3tables_lakeformation_role.add_to_policy(
            iam.PolicyStatement(
                sid="LakeFormationDataAccessPermissionsForS3TableBucket",
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3tables:CreateTableBucket",
                    "s3tables:GetTableBucket",
                    "s3tables:CreateNamespace",
                    "s3tables:GetNamespace",
                    "s3tables:ListNamespaces",
                    "s3tables:DeleteNamespace",
                    "s3tables:DeleteTableBucket",
                    "s3tables:CreateTable",
                    "s3tables:DeleteTable",
                    "s3tables:GetTable",
                    "s3tables:ListTables",
                    "s3tables:RenameTable",
                    "s3tables:UpdateTableMetadataLocation",
                    "s3tables:GetTableMetadataLocation",
                    "s3tables:GetTableData",
                    "s3tables:PutTableData"
                ],
                resources=[f"arn:aws:s3tables:{Aws.REGION}:{Aws.ACCOUNT_ID}:bucket/*"]  # 已经指定了资源
            )
        )

        # 创建Glue联邦目录连接到S3 Tables - 使用自定义资源
        glue_catalog_role = iam.Role(
            self, "GlueCatalogCustomResourceRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )
        
        # 添加Glue权限 - 只需要一处定义
        glue_catalog_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "*"
                ],
                resources=["*"]
            )
        )

                # 添加 Lake Formation 权限
        glue_catalog_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "lakeformation:GetDataAccess",
                    "lakeformation:GrantPermissions",
                    "lakeformation:GetCatalogResource",
                    "lakeformation:ListPermissions",
                    "lakeformation:GetDataLakeSettings"
                ],
                resources=["*"]
            )
        )
        
        # 添加基本的Lambda执行权限
        glue_catalog_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
        )

        # 修改 Lambda 函数，确保使用正确的角色
        create_catalog_lambda = lambda_.Function(
            self, "CreateCatalogLambda",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="glue_catalog_handler.handler",
            code=lambda_.Code.from_asset("lambda"),
            timeout=Duration.seconds(300),
            memory_size=256,
            role=glue_catalog_role,
            environment={
                "BUCKET_NAME": data_bucket.bucket_name
            }
        )
        
        # 使用自定义资源调用Lambda
        s3tables_catalog = cr.Provider(
            self, "GlueCatalogProvider",
            on_event_handler=create_catalog_lambda
        )
        
        # 创建自定义资源来触发Lambda
        s3tables_catalog_resource = CustomResource(
            self, "S3TablesCatalogResource",
            service_token=s3tables_catalog.service_token,
            properties={
                "Region": Aws.REGION,
                "AccountId": Aws.ACCOUNT_ID,
                "Version": "1.1",  # 每次需要更新时递增此值
                "Timestamp": str(int(time.time()))  # 添加时间戳确保每次部署都不同
            }
        )

        # 上传EMR作业脚本到S3
        script_deployment = s3deploy.BucketDeployment(
            self, "DeployProcessScript",
            sources=[s3deploy.Source.asset("emr_job")],
            destination_bucket=data_bucket,
            destination_key_prefix="scripts"
        )

        # 创建一个自定义资源的IAM策略，允许PassRole操作
        emr_custom_resource_role = iam.Role(
            self, "EMRCustomResourceRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )

        # 添加PassRole权限
        emr_custom_resource_role.add_to_policy(
            iam.PolicyStatement(
                actions=["iam:PassRole"],
                resources=[emr_execution_role.role_arn],
                effect=iam.Effect.ALLOW
            )
        )

        # 添加EMR Serverless权限
        emr_custom_resource_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "emr-serverless:StartJobRun",
                    "emr-serverless:GetJobRun",
                    "emr-serverless:CancelJobRun"
                ],
                resources=["*"],
                effect=iam.Effect.ALLOW
            )
        )

        # 添加基本的Lambda执行权限
        emr_custom_resource_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
        )

        # 修改自定义资源，使用新创建的角色
        emr_job_custom_resource = cr.AwsCustomResource(
            self, "EMRServerlessJobRun",
            on_create={
                "service": "EMRServerless", 
                "action": "startJobRun",
                "parameters": {
                    "applicationId": emr_app.attr_application_id,
                    "executionRoleArn": emr_execution_role.role_arn,
                    "jobDriver": {
                        "sparkSubmit": {
                            "entryPoint": f"s3://{data_bucket.bucket_name}/scripts/process_data.py",
                            "sparkSubmitParameters": "--conf spark.executor.cores=4 --conf spark.executor.memory=8g"
                        }
                    },
                    "configurationOverrides": {
                        "applicationConfiguration": [{
                            "classification": "spark-defaults",
                            "properties": {
                                "spark.dynamicAllocation.enabled": "true",
                                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hive.HiveCatalog",
                                "spark.hadoop.hive.metastore.glue.catalogid": Aws.ACCOUNT_ID,
                                "spark.sql.catalogImplementation": "hive"
                            }
                        }]
                    }
                },
                "physical_resource_id": cr.PhysicalResourceId.of("EMRServerlessJobRun")
            },
            policy=cr.AwsCustomResourcePolicy.from_statements([
                iam.PolicyStatement(
                    actions=["emr-serverless:StartJobRun"],
                    resources=["*"],
                    effect=iam.Effect.ALLOW
                ),
                iam.PolicyStatement(
                    actions=["iam:PassRole"],
                    resources=[emr_execution_role.role_arn],
                    effect=iam.Effect.ALLOW
                )
            ]),
            role=emr_custom_resource_role  # 使用新创建的角色
        )

        # 确保作业依赖于脚本部署
        emr_job_custom_resource.node.add_dependency(script_deployment)

        # 为Athena创建IAM角色
        athena_role = iam.Role(
            self, "AthenaQueryRole",
            assumed_by=iam.ServicePrincipal("athena.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonAthenaFullAccess")
            ]
        )

        # 输出重要资源信息
        CfnOutput(self, "DataBucketName", value=data_bucket.bucket_name)
        CfnOutput(self, "SQSQueueUrl", value=data_queue.queue_url)
        CfnOutput(self, "LambdaFunction", value=processing_lambda.function_name)
        CfnOutput(self, "EMRServerlessAppId", value=emr_app.attr_application_id)
        CfnOutput(self, "EC2InstanceId", value=instance.instance_id)
        CfnOutput(self, "GlueDatabaseName", value="data_lake_db")
        CfnOutput(self, "AthenaWorkgroup", value="data-analysis-workgroup")
        # 输出S3 Tables角色ARN
        CfnOutput(
            self, "S3TablesLakeFormationRoleArn", 
            value=s3tables_lakeformation_role.role_arn,
            description="ARN of the IAM role for Lake Formation to access S3 Tables"
        )

        # 输出Glue联邦目录名称
        CfnOutput(
            self, "S3TablesGlueCatalogName", 
            value="s3tablescatalog",
            description="Name of the Glue Federated Catalog for S3 Tables"
        )

        # 创建 Lake Formation 权限管理的 Lambda 角色
        lakeformation_permissions_role = iam.Role(
            self, "LakeFormationPermissionsRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )

        # 添加 Lake Formation 权限
        lakeformation_permissions_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "lakeformation:GetDataLakeSettings",
                    "lakeformation:PutDataLakeSettings"
                ],
                resources=["*"]
            )
        )

        # 添加基本的 Lambda 执行权限
        lakeformation_permissions_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
        )

        # 创建 Lake Formation 资源注册的 Lambda 角色
        lakeformation_resource_role = iam.Role(
            self, "LakeFormationResourceRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
        )

        # 添加 Lake Formation 权限
        lakeformation_resource_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "lakeformation:RegisterResource",
                    "lakeformation:DeregisterResource"
                ],
                resources=["*"]
            )
        )

        # 添加 IAM PassRole 权限
        lakeformation_resource_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["iam:PassRole"],
                resources=[s3tables_lakeformation_role.role_arn]  # 明确指定可以传递的角色
            )
        )

        # 添加基本的 Lambda 执行权限
        lakeformation_resource_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
        )

        # 在创建 lakeformation_resource_role 时添加 S3 权限
        lakeformation_resource_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:PutObject"  # 允许向 S3 写入响应
                ],
                resources=["*"]  # 您可以限制到特定的 S3 bucket
            )
        )

        # 添加 CloudFormation 相关权限
        lakeformation_resource_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "cloudformation:SignalResource"  # 允许向 CloudFormation 发送信号
                ],
                resources=["*"]
            )
        )

        # 创建 Lake Formation 权限管理的 Lambda 函数
        lakeformation_permissions_lambda = lambda_.Function(
            self, "LakeFormationPermissionsLambda",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="lakeformation_permissions_handler.handler",
            code=lambda_.Code.from_asset("lambda"),
            timeout=Duration.seconds(300),
            memory_size=256,
            role=lakeformation_permissions_role
        )

        # 创建自定义资源提供者
        lakeformation_permissions_provider = cr.Provider(
            self, "LakeFormationPermissionsProvider",
            on_event_handler=lakeformation_permissions_lambda
        )

        # 创建自定义资源，设置 Lake Formation 权限
        lakeformation_permissions_resource = CustomResource(
            self, "LakeFormationPermissionsResource",
            service_token=lakeformation_permissions_provider.service_token,
            properties={
                "RoleArns": [
                    glue_catalog_role.role_arn,
                    emr_execution_role.role_arn,
                    lakeformation_resource_role.role_arn
                ],
                "Version": "1.0",
                "Timestamp": str(int(time.time()))
            }
        )

        # 设置 Lake Formation 权限资源依赖于 IAM 角色
        lakeformation_permissions_resource.node.add_dependency(glue_catalog_role)
        lakeformation_permissions_resource.node.add_dependency(emr_execution_role)
        lakeformation_permissions_resource.node.add_dependency(lakeformation_resource_role)


        # 重要：修改 Glue Catalog 资源的创建顺序，使其依赖于 Lake Formation 权限
        # 注意：这里需要修改之前的代码，将 s3tables_catalog_resource 的创建移到 lakeformation_permissions_resource 之后
        # 或者添加依赖关系
        s3tables_catalog_resource.node.add_dependency(lakeformation_permissions_resource)

        # EMR 应用程序依赖于 Lake Formation 权限
        emr_app.node.add_dependency(lakeformation_permissions_resource)

        # 如果有 EMR 作业自定义资源，也添加依赖
        if 'emr_job_custom_resource' in vars():
            emr_job_custom_resource.node.add_dependency(lakeformation_permissions_resource)

        # 创建 Lake Formation 资源注册的 Lambda 函数
        lakeformation_resource_lambda = lambda_.Function(
            self, "LakeFormationResourceLambda",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="lakeformation_resource_handler.handler",
            code=lambda_.Code.from_asset("lambda"),
            timeout=Duration.seconds(300),
            memory_size=256,
            role=lakeformation_resource_role
        )

        # 创建自定义资源提供者
        lakeformation_resource_provider = cr.Provider(
            self, "LakeFormationResourceProvider",
            on_event_handler=lakeformation_resource_lambda
        )

        # 创建自定义资源，注册 S3 Tables 资源
        lakeformation_resource_registration = CustomResource(
            self, "LakeFormationResourceRegistration",
            service_token=lakeformation_resource_provider.service_token,
            properties={
                "ResourceArn": f"arn:aws:s3tables:{Aws.REGION}:{Aws.ACCOUNT_ID}:bucket/*",
                "ResourceRoleArn": s3tables_lakeformation_role.role_arn,
                "Version": "1.0",
                "Timestamp": str(int(time.time()))
            }
        )

        # 设置依赖关系
        lakeformation_resource_registration.node.add_dependency(s3tables_lakeformation_role)
        lakeformation_resource_registration.node.add_dependency(lakeformation_permissions_resource)

        # 确保 Glue Catalog 资源依赖于资源注册
        s3tables_catalog_resource.node.add_dependency(lakeformation_resource_registration)
