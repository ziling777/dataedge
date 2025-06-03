from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col
import sys
import boto3

def main():
    # 获取命令行参数中的S3桶名和账户ID
    if len(sys.argv) > 2:
        bucket_name = sys.argv[1]
        account_id = sys.argv[2]  # 账户ID作为catalog名称
    else:
        # 默认值
        bucket_name = "default-bucket-name"
        # 如果没有提供账户ID，尝试自动获取
        try:
            sts_client = boto3.client('sts')
            account_id = sts_client.get_caller_identity()['Account']
        except:
            account_id = "default"
    
    # 创建SparkSession
    spark = SparkSession.builder \
        .appName("数据处理作业") \
        .getOrCreate()
    
    # 创建命名空间
    spark.sql(f"""create namespace if not exists {account_id}.greptime""")
    
    # 显示账户下的所有命名空间
    print("显示所有命名空间:")
    spark.sql(f"""show namespaces in {account_id}""").show()
    
    # 定义表存储路径
    table_store = f"{account_id}.greptime.canbus_01"
    print(f"表存储路径: {table_store}")
    
    # 从S3读取处理后的数据
    input_path = f"s3://{bucket_name}/processed/"
    df = spark.read.parquet(input_path)
    
    # 直接将原始df写入到表存储路径，不进行额外处理
    print(f"正在将原始数据写入表: {table_store}")
    df.write \
        .format("iceberg") \
        .mode("overwrite") \
        .saveAsTable(table_store)
    
    # 查询写入的数据
    print(f"查询表 {table_store} 中的数据:")
    result_df = spark.sql(f"SELECT * FROM {table_store} LIMIT 10")
    result_df.show()
    
    # 获取表的行数
    count_df = spark.sql(f"SELECT COUNT(*) AS total_rows FROM {table_store}")
    print("表中的总行数:")
    count_df.show()
    
    spark.stop()

if __name__ == "__main__":
    main()
