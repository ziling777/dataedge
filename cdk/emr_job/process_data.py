from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col, to_date, year, month, day, from_unixtime, to_timestamp
import sys
import boto3

def main():
    # 获取命令行参数中的S3桶名
    if len(sys.argv) > 1:
        bucket_name = sys.argv[1]
    else:
        # 默认值
        bucket_name = "default-bucket-name"
    
    # 使用固定的catalog名称"gpdemo"
    catalog_name = "gpdemo"
    
    # 创建SparkSession
    spark = SparkSession.builder \
        .appName("数据处理作业") \
        .getOrCreate()
    
    # 创建命名空间
    spark.sql(f"""create namespace if not exists {catalog_name}.greptime""")
    
    # 显示所有命名空间
    print("显示所有命名空间:")
    spark.sql(f"""show namespaces in {catalog_name}""").show()
    
    # 定义表存储路径
    table_store = f"{catalog_name}.greptime.canbus_01"
    print(f"表存储路径: {table_store}")
    
    # 从S3读取处理后的数据
    input_path = f"s3://{bucket_name}/processed/"
    df = spark.read.parquet(input_path)
    
    # 检查数据中是否有ts列，如果没有则添加当前日期作为时间列
    if "ts" not in df.columns:
        print("数据中没有ts列，添加当前时间戳作为时间列")
        df = df.withColumn("ts", current_date())
    
    # 将毫秒级时间戳转换为日期时间格式
    # 注意：毫秒时间戳需要除以1000转换为秒级时间戳
    df = df.withColumn("ts_date", from_unixtime(col("ts")/1000))
    
    # 从转换后的日期时间提取年、月、日用于分区
    df = df.withColumn("year", year("ts_date")) \
           .withColumn("month", month("ts_date")) \
           .withColumn("day", day("ts_date"))
    
    print(f"正在将数据按时间分区写入表: {table_store}")
    df.write \
        .format("iceberg") \
        .mode("overwrite") \
        .partitionBy("year", "month", "day") \
        .saveAsTable(table_store)
    
    # 查询写入的数据
    print(f"查询表 {table_store} 中的数据:")
    result_df = spark.sql(f"SELECT * FROM {table_store} LIMIT 10")
    result_df.show()
    
    # 获取表的行数
    count_df = spark.sql(f"SELECT COUNT(*) AS total_rows FROM {table_store}")
    print("表中的总行数:")
    count_df.show()
    
    # 查看表的分区信息
    print("表的分区信息:")
    spark.sql(f"SHOW PARTITIONS {table_store}").show()
    
    spark.stop()

if __name__ == "__main__":
    main()
