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
    # 首先检查ts列的数据类型并进行适当的转换
    # 使用unix_timestamp函数处理TIMESTAMP_NTZ类型
    from pyspark.sql.functions import unix_timestamp
    
    # 检查ts列的数据类型
    ts_data_type = df.schema["ts"].dataType.typeName()
    print(f"ts列的数据类型: {ts_data_type}")
    
    # 根据数据类型选择不同的转换方法
    if "timestamp" in ts_data_type.lower():
        # 如果是时间戳类型，先转换为unix时间戳（秒）
        df = df.withColumn("ts_date", col("ts"))
    else:
        # 如果是数值类型（假设是毫秒时间戳），除以1000转换为秒
        df = df.withColumn("ts_date", from_unixtime(col("ts")/1000))
    
    # 从转换后的日期时间提取年、月、日
    df = df.withColumn("year", year("ts_date")) \
           .withColumn("month", month("ts_date")) \
           .withColumn("day", day("ts_date"))
    
    # 先尝试创建表，使用Iceberg的分区语法
    try:
        spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table_store} (
            ts TIMESTAMP,
            ts_date TIMESTAMP,
            year INT,
            month INT,
            day INT
        )
        USING iceberg
        """)
        print(f"表 {table_store} 已创建或已存在")
    except Exception as e:
        print(f"创建表时出错: {str(e)}")
    
    print(f"正在将数据写入表: {table_store}")
    # 不使用partitionBy，直接写入表
    df.write \
        .format("iceberg") \
        .mode("overwrite") \
        .option("write.format.default", "parquet") \
        .saveAsTable(table_store)
    
    # 查询写入的数据
    print(f"查询表 {table_store} 中的数据:")
    result_df = spark.sql(f"SELECT * FROM {table_store} LIMIT 10")
    result_df.show()
    
    # 获取表的行数
    count_df = spark.sql(f"SELECT COUNT(*) AS total_rows FROM {table_store}")
    print("表中的总行数:")
    count_df.show()
    
    # 尝试查看表的元数据信息，而不是分区信息
    print("表的元数据信息:")
    try:
        spark.sql(f"DESCRIBE TABLE EXTENDED {table_store}").show(truncate=False)
    except Exception as e:
        print(f"获取表元数据时出错: {str(e)}")
    
    spark.stop()

if __name__ == "__main__":
    main()
