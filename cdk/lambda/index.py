import json
import boto3
import gzip
import io
import urllib.parse
import os

s3_client = boto3.client('s3')
bucket_name = os.environ['BUCKET_NAME']

def handler(event, context):
    # 从SQS消息获取S3事件信息
    for record in event['Records']:
        sqs_message = json.loads(record['body'])
        
        for s3_record in sqs_message['Records']:
            bucket = s3_record['s3']['bucket']['name']
            key = urllib.parse.unquote_plus(s3_record['s3']['object']['key'])
            
            # 获取压缩的parquet文件
            response = s3_client.get_object(Bucket=bucket, Key=key)
            compressed_data = response['Body'].read()
            
            # 解压数据
            decompressed_data = gzip.decompress(compressed_data)
            
            # 这里可以添加更多的处理逻辑，例如解析parquet文件
            # 此处省略具体的decode操作...
            
            # 将处理后的数据写回S3
            processed_key = key.replace('raw/', 'processed/')
            s3_client.put_object(
                Bucket=bucket,
                Key=processed_key,
                Body=decompressed_data  # 或者其他处理后的数据
            )
            
            print(f"已处理文件：{key} 并保存到 {processed_key}")
    
    return {
        'statusCode': 200,
        'body': json.dumps('处理完成')
    } 