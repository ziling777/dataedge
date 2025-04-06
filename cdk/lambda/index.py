import json
import boto3
import os
import tempfile
import logging
import py7zr
from urllib.parse import unquote_plus

# 配置日志
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 获取环境变量
S3_BUCKET = os.environ.get('S3_BUCKET', 'default-bucket-name')

# 创建S3客户端
s3_client = boto3.client('s3')

def handler(event, context):
    """
    处理S3事件通知，解压7z文件并保存到processed目录
    """
    logger.info(f"Lambda执行开始 - RequestId: {context.aws_request_id}")
    
    try:
        # 处理每条记录
        for record in event['Records']:
            if 'body' not in record:
                continue
                
            # 解析SQS消息体
            body = json.loads(record['body'])
            
            # 检查是否是S3事件通知
            if 'Records' in body:
                for s3_record in body['Records']:
                    # 获取对象键
                    if 's3' not in s3_record or 'object' not in s3_record['s3'] or 'key' not in s3_record['s3']['object']:
                        continue
                        
                    key = unquote_plus(s3_record['s3']['object']['key'])
                    logger.info(f"检查S3对象: {key}")
                    
                    # 检查是否是raw目录下的7z文件
                    if ('raw/' in key or '/raw/' in key) and (key.endswith('.zip') or key.endswith('.7z')):
                        logger.info(f"找到需要处理的文件: {key}")
                        process_file(key)
        
        return {'statusCode': 200, 'body': json.dumps('处理完成')}
    
    except Exception as e:
        logger.error(f"处理过程中发生错误: {str(e)}")
        raise

def process_file(key):
    """
    处理单个文件：下载、解压并上传到processed目录
    """
    logger.info(f"处理文件: {S3_BUCKET}/{key}")
    
    # 创建临时目录
    with tempfile.TemporaryDirectory() as tmp_dir:
        # 构建临时文件路径
        compressed_path = os.path.join(tmp_dir, os.path.basename(key))
        extracted_path = os.path.join(tmp_dir, 'extracted')
        os.makedirs(extracted_path, exist_ok=True)
        
        # 下载文件
        logger.info(f"下载文件: {key}")
        s3_client.download_file(S3_BUCKET, key, compressed_path)
        
        # 使用py7zr解压
        try:
            logger.info("开始解压文件")
            with py7zr.SevenZipFile(compressed_path, mode='r') as archive:
                # 获取文件列表
                file_list = archive.getnames()
                logger.info(f"压缩包包含 {len(file_list)} 个文件")
                
                # 解压所有内容
                archive.extractall(path=extracted_path)
                logger.info("解压成功")
                
                # 上传处理后的文件
                files_uploaded = 0
                for root, _, files in os.walk(extracted_path):
                    for file in files:
                        local_path = os.path.join(root, file)
                        s3_target_key = f"processed/{file}"
                        
                        logger.info(f"上传文件到: {S3_BUCKET}/{s3_target_key}")
                        s3_client.upload_file(local_path, S3_BUCKET, s3_target_key)
                        files_uploaded += 1
                        
                logger.info(f"处理完成: {key}. {files_uploaded} 个文件已上传到processed目录")
                
        except Exception as e:
            logger.error(f"解压过程中发生错误: {str(e)}")
            raise

    