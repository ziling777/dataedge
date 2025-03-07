import boto3
import cfnresponse
import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Lambda 处理程序，用于注册 Lake Formation 资源
    """
    logger.info('Received event: %s', json.dumps(event, indent=2))
    logger.info(f"Using boto3 version: {boto3.__version__}")
    
    # 打印 Lambda 上下文信息
    logger.info(f"Function name: {context.function_name}")
    logger.info(f"Function version: {context.function_version}")
    logger.info(f"Memory limit: {context.memory_limit_in_mb}")
    logger.info(f"Time remaining: {context.get_remaining_time_in_millis()}")
    
    response_data = {}
    physical_id = 'LakeFormationResource'
    
    try:
        request_type = event['RequestType']
        properties = event['ResourceProperties']
        
        # 获取资源注册所需的参数
        resource_arn = properties.get('ResourceArn', '')
        role_arn = properties.get('ResourceRoleArn', '')
        
        if not resource_arn or not role_arn:
            logger.error("Missing required parameters: ResourceArn or ResourceRoleArn")
            cfnresponse.send(event, context, cfnresponse.FAILED, 
                            {"Error": "Missing required parameters"}, physical_id)
            return
        
        # 创建 Lake Formation 客户端
        lakeformation_client = boto3.client('lakeformation')
        
        if request_type == 'Create' or request_type == 'Update':
            # 注册资源
            try:
                logger.info(f"Registering resource {resource_arn} with role {role_arn}")
                lakeformation_client.register_resource(
                    ResourceArn=resource_arn,
                    UseServiceLinkedRole=False,
                    RoleArn=role_arn,
                    WithFederation=True
                )
                logger.info("Resource registered successfully")
                response_data['Status'] = "Resource registered successfully"
                cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data, physical_id)
            except lakeformation_client.exceptions.AlreadyExistsException:
                # 资源已经注册，这不是错误
                logger.info(f"Resource {resource_arn} is already registered. This is not an error.")
                response_data['Status'] = "Resource was already registered"
                cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data, physical_id)
            except Exception as e:
                logger.error(f"Error registering resource: {e}")
                cfnresponse.send(event, context, cfnresponse.FAILED, 
                                {"Error": f"Failed to register resource: {str(e)}"}, physical_id)
                return
        
        elif request_type == 'Delete':
            # 取消注册资源
            try:
                logger.info(f"Deregistering resource {resource_arn}")
                lakeformation_client.deregister_resource(
                    ResourceArn=resource_arn
                )
                logger.info("Resource deregistered successfully")
                response_data['Status'] = "Resource deregistered successfully"
                cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data, physical_id)
            except lakeformation_client.exceptions.EntityNotFoundException:
                # 资源不存在，这不是错误
                logger.info(f"Resource {resource_arn} does not exist or is already deregistered. This is not an error.")
                response_data['Status'] = "Resource was already deregistered or does not exist"
                cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data, physical_id)
            except Exception as e:
                logger.error(f"Error deregistering resource: {e}")
                # 删除操作通常应该继续，即使部分失败
                cfnresponse.send(event, context, cfnresponse.SUCCESS, 
                                {"Warning": f"Failed to deregister resource: {str(e)}"}, physical_id)
                return
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        cfnresponse.send(event, context, cfnresponse.FAILED, 
                        {"Error": f"Unexpected error: {str(e)}"}, physical_id)