import boto3
import cfnresponse
import logging
import json
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
    """
    Lambda 处理程序，用于管理 Lake Formation 权限
    """
    logger.info('Received event: %s', json.dumps(event))
    logger.info(f"Using boto3 version: {boto3.__version__}")
    
    response_data = {}
    physical_id = 'LakeFormationPermissions'
    
    try:
        request_type = event['RequestType']
        properties = event['ResourceProperties']
        
        # 获取需要授予权限的角色 ARN
        role_arns = properties.get('RoleArns', [])
        
        if not role_arns:
            logger.error("No role ARNs provided")
            cfnresponse.send(event, context, cfnresponse.FAILED, 
                            {"Error": "No role ARNs provided"}, physical_id)
            return
        
        # 创建 Lake Formation 客户端
        lakeformation_client = boto3.client('lakeformation')
        
        if request_type == 'Create' or request_type == 'Update':
            logger.info(f"Setting Lake Formation permissions for roles: {role_arns}")
            
            # 获取当前的数据湖设置
            try:
                data_lake_settings = lakeformation_client.get_data_lake_settings()
                logger.info(f"Current data lake settings: {json.dumps(data_lake_settings)}")
            except Exception as e:
                logger.error(f"Error getting data lake settings: {e}")
                cfnresponse.send(event, context, cfnresponse.FAILED, 
                                {"Error": f"Failed to get data lake settings: {str(e)}"}, physical_id)
                return
            
            # 准备更新的管理员列表
            current_admins = data_lake_settings.get('DataLakeSettings', {}).get('DataLakeAdmins', [])
            
            # 转换角色 ARN 为 Lake Formation 可接受的格式
            new_admins = []
            for role_arn in role_arns:
                new_admins.append({
                    'DataLakePrincipalIdentifier': role_arn
                })
            
            # 合并现有管理员和新管理员，避免重复
            updated_admins = current_admins.copy()
            for admin in new_admins:
                if admin not in updated_admins:
                    updated_admins.append(admin)
            
            # 更新数据湖设置
            try:
                logger.info(f"Updating data lake settings with admins: {json.dumps(updated_admins)}")
                lakeformation_client.put_data_lake_settings(
                    DataLakeSettings={
                        'DataLakeAdmins': updated_admins
                    }
                )
                logger.info("Successfully updated data lake settings")
                
                # 等待权限传播
                time.sleep(5)
                
                # 验证权限是否已设置
                updated_settings = lakeformation_client.get_data_lake_settings()
                updated_admins_list = updated_settings.get('DataLakeSettings', {}).get('DataLakeAdmins', [])
                logger.info(f"Updated data lake admins: {json.dumps(updated_admins_list)}")
                
                # 检查所有角色是否都已添加为管理员
                all_roles_added = True
                for role_arn in role_arns:
                    role_found = False
                    for admin in updated_admins_list:
                        if admin.get('DataLakePrincipalIdentifier') == role_arn:
                            role_found = True
                            break
                    if not role_found:
                        all_roles_added = False
                        logger.warning(f"Role {role_arn} was not added as a data lake admin")
                
                if all_roles_added:
                    response_data['Status'] = "All roles successfully added as data lake admins"
                else:
                    response_data['Status'] = "Some roles may not have been added as data lake admins"
                
                cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data, physical_id)
                
            except Exception as e:
                logger.error(f"Error updating data lake settings: {e}")
                cfnresponse.send(event, context, cfnresponse.FAILED, 
                                {"Error": f"Failed to update data lake settings: {str(e)}"}, physical_id)
                return
            
        elif request_type == 'Delete':
            logger.info(f"Removing Lake Formation permissions for roles: {role_arns}")
            
            # 获取当前的数据湖设置
            try:
                data_lake_settings = lakeformation_client.get_data_lake_settings()
                logger.info(f"Current data lake settings: {json.dumps(data_lake_settings)}")
            except Exception as e:
                logger.error(f"Error getting data lake settings: {e}")
                # 即使获取设置失败，也继续处理删除请求
                cfnresponse.send(event, context, cfnresponse.SUCCESS, 
                                {"Warning": f"Failed to get data lake settings: {str(e)}"}, physical_id)
                return
            
            # 准备更新的管理员列表，移除指定的角色
            current_admins = data_lake_settings.get('DataLakeSettings', {}).get('DataLakeAdmins', [])
            updated_admins = [admin for admin in current_admins 
                             if admin.get('DataLakePrincipalIdentifier') not in role_arns]
            
            # 更新数据湖设置
            try:
                logger.info(f"Updating data lake settings with admins: {json.dumps(updated_admins)}")
                lakeformation_client.put_data_lake_settings(
                    DataLakeSettings={
                        'DataLakeAdmins': updated_admins
                    }
                )
                logger.info("Successfully updated data lake settings")
                response_data['Status'] = "Roles successfully removed as data lake admins"
                cfnresponse.send(event, context, cfnresponse.SUCCESS, response_data, physical_id)
                
            except Exception as e:
                logger.error(f"Error updating data lake settings: {e}")
                # 即使更新失败，也返回成功，因为这是删除操作
                cfnresponse.send(event, context, cfnresponse.SUCCESS, 
                                {"Warning": f"Failed to update data lake settings: {str(e)}"}, physical_id)
                return
            
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        cfnresponse.send(event, context, cfnresponse.FAILED, 
                        {"Error": f"Unexpected error: {str(e)}"}, physical_id) 