from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col
import sys

def main():
    # 获取命令行参数中的S3桶名
    if len(sys.argv) > 1:
        bucket_name = sys.argv[1]
    else:
        # 默认桶名，如果没有提供参数
        bucket_name = "default-bucket-name"
    # 创建SparkSession
    spark = SparkSession.builder \
        .appName("数据处理作业") \
        .config("spark.hadoop.hive.metastore.client.factory.class", 
                "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .enableHiveSupport() \
        .getOrCreate()
    
    # 从S3读取处理后的数据
    input_path = f"s3://{bucket_name}/processed/"
    df = spark.read.parquet(input_path)
    
    # 处理数据
    processed_df = df.filter(col("id") > 100) \
        .withColumn("processed_date", current_date())
    
    # 将数据写入到受Lake Formation管理的表
    # 使用Hive/Glue Catalog语法
    processed_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .partitionBy("processed_date") \
        .option("path", f"s3://{bucket_name}/table/") \
        .saveAsTable("data_lake_db.processed_data")
    
    spark.stop()

if __name__ == "__main__":
    main()
