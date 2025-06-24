from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col, monotonically_increasing_id, concat, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType, TimestampType
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
    
    # 定义canbus01表的结构 - 为S3 Tables添加主键
    canbus01_table = f"{catalog_name}.greptime.canbus01"
    
    # 创建canbus01表，基于MySQL表结构，添加主键
    create_canbus01_sql = f"""
    CREATE TABLE IF NOT EXISTS {canbus01_table} (
        __primary_key STRING,
        vin_id STRING,
        charging_time_remain_minute INT,
        extender_starting_point INT,
        fuel_cltc_mileage INT,
        fuel_wltc_mileage INT,
        fuel_percentage INT,
        clean_mode INT,
        road_mode INT,
        ress_power_low_flag BOOLEAN,
        target_soc INT,
        display_speed FLOAT,
        channel_id INT,
        endurance_type INT,
        ts TIMESTAMP
    ) USING ICEBERG
    PARTITIONED BY (days(ts))
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
    )
    """
    
    print(f"创建canbus01表: {canbus01_table}")
    spark.sql(create_canbus01_sql)
    
    # 从S3读取处理后的数据
    input_path = f"s3://{bucket_name}/processed/"
    print(f"从路径读取数据: {input_path}")
    
    try:
        df = spark.read.parquet(input_path)
        print("数据读取成功，原始数据schema:")
        df.printSchema()
        print(f"数据行数: {df.count()}")
        
        # 显示前几行数据
        print("数据预览:")
        df.show(5, truncate=False)
        
        # 为数据添加主键列 - 使用vin_id和ts的组合
        df_with_pk = df.withColumn("__primary_key", 
                                   concat(col("vin_id"), lit("_"), col("ts").cast("string")))
        
        print("添加主键后的数据schema:")
        df_with_pk.printSchema()
        
        # 将数据写入canbus01表
        print(f"正在将数据写入表: {canbus01_table}")
        df_with_pk.write \
            .format("iceberg") \
            .mode("append") \
            .saveAsTable(canbus01_table)
        
        print("数据写入成功!")
        
        # 查询写入的数据
        print(f"查询表 {canbus01_table} 中的数据:")
        result_df = spark.sql(f"SELECT * FROM {canbus01_table} LIMIT 10")
        result_df.show(truncate=False)
        
        # 获取表的行数
        count_df = spark.sql(f"SELECT COUNT(*) AS total_rows FROM {canbus01_table}")
        print("表中的总行数:")
        count_df.show()
        
        # 显示表的分区信息 - S3 Tables 不支持 SHOW PARTITIONS，跳过
        print("S3 Tables 不支持 SHOW PARTITIONS 命令，跳过分区信息显示")
        
    except Exception as e:
        print(f"处理数据时出错: {str(e)}")
        # 如果没有数据文件，创建一些示例数据用于测试
        print("创建示例数据用于测试...")
        
        from pyspark.sql import Row
        from datetime import datetime
        import uuid
        
        current_time = datetime.now()
        
        sample_data = [
            Row(
                __primary_key=f"TEST001_{int(current_time.timestamp())}",
                vin_id="TEST001",
                charging_time_remain_minute=120,
                extender_starting_point=50,
                fuel_cltc_mileage=500,
                fuel_wltc_mileage=480,
                fuel_percentage=75,
                clean_mode=1,
                road_mode=2,
                ress_power_low_flag=False,
                target_soc=80,
                display_speed=60.5,
                channel_id=1,
                endurance_type=1,
                ts=current_time
            ),
            Row(
                __primary_key=f"TEST002_{int(current_time.timestamp())}",
                vin_id="TEST002",
                charging_time_remain_minute=90,
                extender_starting_point=30,
                fuel_cltc_mileage=450,
                fuel_wltc_mileage=430,
                fuel_percentage=60,
                clean_mode=0,
                road_mode=1,
                ress_power_low_flag=True,
                target_soc=70,
                display_speed=45.2,
                channel_id=2,
                endurance_type=2,
                ts=current_time
            )
        ]
        
        sample_df = spark.createDataFrame(sample_data)
        print("示例数据schema:")
        sample_df.printSchema()
        
        # 写入示例数据（已经包含主键）
        sample_df.write \
            .format("iceberg") \
            .mode("append") \
            .saveAsTable(canbus01_table)
        
        print("示例数据写入成功!")
        
        # 查询示例数据
        result_df = spark.sql(f"SELECT * FROM {canbus01_table}")
        result_df.show(truncate=False)
    
    # 显示所有表
    print(f"显示 {catalog_name}.greptime 命名空间中的所有表:")
    spark.sql(f"SHOW TABLES IN {catalog_name}.greptime").show()
    
    spark.stop()

if __name__ == "__main__":
    main()
