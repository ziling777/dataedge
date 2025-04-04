import json
import boto3
import os
import tempfile
import logging
import zipfile  # 添加zipfile导入
import shutil   # 添加shutil导入
import subprocess  # 添加subprocess导入用于调用系统命令
from urllib.parse import unquote_plus

# 导入py7zr
try:
    import py7zr
    PY7ZR_AVAILABLE = True
except ImportError:
    PY7ZR_AVAILABLE = False
    print("Warning: py7zr not available. Files will not be processed correctly.")

# 获取环境变量
S3_BUCKET = os.environ.get('S3_BUCKET', 'default-bucket-name')
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()

# 配置日志
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# 获取现有日志处理器
handlers_found = False
for handler in logger.handlers:
    handlers_found = True
    level = getattr(logging, LOG_LEVEL, logging.INFO)
    handler.setLevel(level)
    formatter = logging.Formatter('[%(levelname)s] %(asctime)s - %(message)s')
    handler.setFormatter(formatter)

# 如果没有找到处理器，添加一个
if not handlers_found:
    console_handler = logging.StreamHandler()
    level = getattr(logging, LOG_LEVEL, logging.INFO)
    console_handler.setLevel(level)
    formatter = logging.Formatter('[%(levelname)s] %(asctime)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

# 打印环境变量
logger.info(f"S3_BUCKET={S3_BUCKET}, LOG_LEVEL={LOG_LEVEL}")

# 创建S3客户端
s3_client = boto3.client('s3')

def handler(event, context):
    """
    处理S3事件通知，解压文件并保存到processed目录
    """
    logger.info(f"Lambda执行开始 - RequestId: {context.aws_request_id}")
    
    try:
        # 检查事件结构
        if 'Records' not in event:
            logger.warning("事件中没有Records字段")
            return {'statusCode': 200, 'body': json.dumps('没有Records需要处理')}
        
        # 处理每条记录
        for record in event['Records']:
            if 'body' not in record:
                continue
                
            # 解析SQS消息体
            try:
                body = json.loads(record['body'])
            except Exception as e:
                logger.error(f"解析消息体出错: {str(e)}")
                continue
            
            # 检查是否是S3事件通知
            if 'Records' in body:
                for s3_record in body['Records']:
                    # 获取对象键
                    if 's3' not in s3_record or 'object' not in s3_record['s3'] or 'key' not in s3_record['s3']['object']:
                        continue
                        
                    key = unquote_plus(s3_record['s3']['object']['key'])
                    logger.info(f"检查S3对象: {key}")
                    
                    # 检查是否是raw目录下的zip/7z文件
                    if ('raw/' in key or '/raw/' in key) and (key.endswith('.zip') or key.endswith('.7z')):
                        logger.info(f"找到需要处理的文件: {key}")
                        process_file(key)
                    else:
                        logger.debug(f"跳过文件: {key}")
        
        logger.info("所有文件处理完成")
        return {'statusCode': 200, 'body': json.dumps('处理完成')}
    
    except Exception as e:
        logger.error(f"处理过程中发生错误: {str(e)}", exc_info=True)
        raise e
    finally:
        logger.info(f"Lambda执行结束 - RequestId: {context.aws_request_id}")

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
        
        # 尝试解压文件 - 先尝试7z格式，如果失败再尝试标准zip格式
        extraction_success = False
        extracted_files = []
        
        # 尝试使用py7zr解压（如果可用）
        if PY7ZR_AVAILABLE:
            try:
                logger.info("尝试使用py7zr解压文件")
                with py7zr.SevenZipFile(compressed_path, mode='r') as archive:
                    # 获取文件列表
                    file_list = archive.getnames()
                    logger.info(f"压缩包包含 {len(file_list)} 个文件: {', '.join(file_list[:5])}" +
                              (f"... 以及 {len(file_list) - 5} 个更多文件" if len(file_list) > 5 else ""))
                    
                    # 解压所有内容
                    archive.extractall(path=extracted_path)
                    logger.info("使用py7zr解压成功")
                    extraction_success = True
                    
                    # 记录基本文件名（不含路径）
                    extracted_files = [os.path.basename(f) for f in file_list]
            except py7zr.exceptions.Bad7zFile:
                logger.warning("文件不是7z格式，将尝试其他解压方法")
            except Exception as e:
                logger.warning(f"使用py7zr解压失败: {str(e)}")
        
        # 如果py7zr解压失败，尝试使用zipfile解压
        if not extraction_success:
            try:
                logger.info("尝试使用zipfile解压文件")
                with zipfile.ZipFile(compressed_path, 'r') as zip_ref:
                    # 获取文件列表
                    file_list = zip_ref.namelist()
                    logger.info(f"ZIP压缩包包含 {len(file_list)} 个文件: {', '.join(file_list[:5])}" +
                              (f"... 以及 {len(file_list) - 5} 个更多文件" if len(file_list) > 5 else ""))
                    
                    # 尝试解压所有内容
                    try:
                        # 不直接使用extractall，逐个文件提取到根目录
                        for file_info in zip_ref.infolist():
                            if not file_info.is_dir():  # 跳过目录项
                                # 只使用文件名，不使用路径
                                output_path = os.path.join(extracted_path, os.path.basename(file_info.filename))
                                with zip_ref.open(file_info) as source:
                                    with open(output_path, 'wb') as target:
                                        shutil.copyfileobj(source, target)
                                        logger.debug(f"解压文件到根目录: {os.path.basename(file_info.filename)}")
                                        
                        logger.info("使用zipfile解压成功")
                        extraction_success = True
                        extracted_files = [os.path.basename(f) for f in file_list if not f.endswith('/')]
                    except zipfile.BadZipFile:
                        logger.warning("无法解压ZIP文件，将尝试直接提取")
                    except Exception as e:
                        logger.warning(f"使用zipfile解压失败: {str(e)}")
                        
                    # 如果标准解压失败，尝试单独提取文件
                    if not extraction_success and len(file_list) > 0:
                        try:
                            logger.info("尝试直接提取单个文件")
                            # 遍历压缩包中的每个文件
                            for file_info in zip_ref.infolist():
                                if file_info.filename.endswith('.parquet'):
                                    logger.info(f"直接提取文件: {file_info.filename}")
                                    # 只使用文件名，不使用路径
                                    output_path = os.path.join(extracted_path, os.path.basename(file_info.filename))
                                    
                                    try:
                                        # 尝试直接读取文件
                                        with zip_ref.open(file_info) as source:
                                            with open(output_path, 'wb') as target:
                                                shutil.copyfileobj(source, target)
                                                logger.info(f"成功提取文件: {os.path.basename(file_info.filename)}")
                                                extracted_files.append(os.path.basename(file_info.filename))
                                    except Exception as e:
                                        logger.warning(f"无法提取文件 {file_info.filename}: {str(e)}")
                                        continue
                                        
                            if extracted_files:
                                extraction_success = True
                                logger.info(f"直接提取成功: {len(extracted_files)}个文件")
                        except Exception as e:
                            logger.warning(f"直接提取文件失败: {str(e)}")
            except zipfile.BadZipFile:
                logger.warning("文件不是标准ZIP格式")
            except Exception as e:
                logger.warning(f"使用zipfile解压失败: {str(e)}")
        
        # 尝试使用系统命令解压（仅在Lambda环境下可用）
        if not extraction_success:
            try:
                logger.info("尝试使用系统unzip命令解压文件")
                # 检查系统是否有unzip命令
                unzip_result = subprocess.run(
                    ["unzip", "-l", compressed_path], 
                    capture_output=True, 
                    text=True
                )
                
                if unzip_result.returncode == 0:
                    # unzip命令可用，尝试解压，-j选项忽略目录结构
                    extract_result = subprocess.run(
                        ["unzip", "-j", compressed_path, "-d", extracted_path],
                        capture_output=True,
                        text=True
                    )
                    
                    if extract_result.returncode == 0:
                        logger.info("使用系统unzip命令解压成功")
                        extraction_success = True
                        # 获取解压后的文件列表（只有文件名，没有路径）
                        extracted_files = os.listdir(extracted_path)
                    else:
                        logger.warning(f"系统unzip命令解压失败: {extract_result.stderr}")
                else:
                    logger.warning("系统unzip命令不可用或不是有效的zip文件")
            except Exception as e:
                logger.warning(f"使用系统命令解压失败: {str(e)}")
        
        # 如果所有解压方法都失败
        if not extraction_success:
            logger.error("所有解压方法都失败，无法处理该文件")
            return
        
        # 上传处理后的文件，所有文件直接放到processed目录下
        files_uploaded = 0
        for root, _, files in os.walk(extracted_path):
            for file in files:
                local_path = os.path.join(root, file)
                
                # 所有文件都直接放在processed目录下
                s3_target_key = f"processed/{file}"
                
                logger.info(f"上传文件到: {S3_BUCKET}/{s3_target_key}")
                s3_client.upload_file(local_path, S3_BUCKET, s3_target_key)
                files_uploaded += 1
                
        logger.info(f"处理完成: {key}. {files_uploaded} 个文件已上传到processed目录")

    