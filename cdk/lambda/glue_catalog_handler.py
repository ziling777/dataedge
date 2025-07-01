import boto3
import cfnresponse
import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    logger.info('Received event: %s', event)
    # 打印 boto3 版本
    logger.info(f"Using boto3 version: {boto3.__version__}")
    
    response_data = {}
    physical_id = 'S3TablesCatalog'
    
    try:
        request_type = event['RequestType']
        glue_client = boto3.client('glue')
        
        if request_type == 'Create':
            logger.info('Creating Glue Federated Catalog')
            
            catalog_input = {
                "Name": "s3tablescatalog",
                "CatalogInput": {
                    "FederatedCatalog": {
                        "Identifier": f"arn:aws:s3tables:{event['ResourceProperties']['Region']}:{event['ResourceProperties']['AccountId']}:bucket/caredgedemo",
                        "ConnectionName": "aws:s3tables"
                    },
                    "CreateDatabaseDefaultPermissions": [],
                    "CreateTableDefaultPermissions": []
                }
            }
            
            logger.info(f'Creating catalog with input: {json.dumps(catalog_input)}')
            response = glue_client.create_catalog(**catalog_input)
            
            response_data['CatalogName'] = 's3tablescatalog'
            cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data, physical_id)
            
        elif request_type == 'Update':
            logger.info('Updating Glue Federated Catalog')
            try:
                # 先检查目录是否存在
                try:
                    # 使用 get_catalog 方法检查目录是否存在
                    logger.info('Checking if catalog exists')
                    glue_client.get_catalog(CatalogId='s3tablescatalog')
                    logger.info('Catalog exists, will update')
                    
                    # 如果存在，则更新
                    catalog_input = {
                        "Name": "s3tablescatalog",
                        "CatalogInput": {
                            "FederatedCatalog": {
                                "Identifier": f"arn:aws:s3tables:{event['ResourceProperties']['Region']}:{event['ResourceProperties']['AccountId']}:bucket/caredgedemo",
                                "ConnectionName": "aws:s3tables"
                            },
                            "CreateDatabaseDefaultPermissions": [],
                            "CreateTableDefaultPermissions": []
                        }
                    }
                    
                    logger.info(f'Updating catalog with input: {json.dumps(catalog_input)}')
                    response = glue_client.update_catalog(**catalog_input)
                    logger.info('Catalog updated successfully')
                    
                except Exception as e:
                    # 如果不存在或发生其他错误，记录错误并尝试创建
                    logger.info(f'Error checking catalog: {e}')
                    logger.info('Attempting to create catalog instead')
                    catalog_input = {
                        "Name": "s3tablescatalog",
                        "CatalogInput": {
                            "FederatedCatalog": {
                                "Identifier": f"arn:aws:s3tables:{event['ResourceProperties']['Region']}:{event['ResourceProperties']['AccountId']}:bucket/caredgedemo",
                                "ConnectionName": "aws:s3tables"
                            },
                            "CreateDatabaseDefaultPermissions": [],
                            "CreateTableDefaultPermissions": []
                        }
                    }
                    
                    logger.info(f'Creating catalog with input: {json.dumps(catalog_input)}')
                    try:
                        response = glue_client.create_catalog(**catalog_input)
                        logger.info('Catalog created successfully')
                    except glue_client.exceptions.AlreadyExistsException:
                        logger.info('Catalog already exists, but get_catalog failed. This is unexpected.')
                        # 尝试更新
                        response = glue_client.update_catalog(**catalog_input)
                        logger.info('Catalog updated successfully after failed get_catalog')
                
                response_data['CatalogName'] = 's3tablescatalog'
                cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data, physical_id)
                
            except Exception as e:
                logger.error(f'Error in catalog operation: {e}')
                cfnresponse.send(event, context, cfnresponse.FAILED, {'Error': str(e)}, physical_id)
            
        elif request_type == 'Delete':
            logger.info('Deleting Glue Federated Catalog')
            try:
                glue_client.delete_catalog(
                    Name='s3tablescatalog'
                )
            except Exception as e:
                logger.error('Error deleting catalog: %s', e)
                # 即使删除失败也继续，因为资源可能已经不存在
                
            cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data, physical_id)
            
    except Exception as e:
        logger.error('Error: %s', e)
        cfnresponse.send(event, context, cfnresponse.FAILED, {'Error': str(e)}, physical_id) 