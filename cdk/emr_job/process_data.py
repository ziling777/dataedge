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
    
    # 检查表是否存在
    table_exists = False
    try:
        # 使用 Spark Catalog 检查表是否存在
        table_exists = spark.catalog.tableExists(canbus01_table)
        print(f"表 {canbus01_table} 存在性检查: {table_exists}")
    except Exception as e:
        print(f"检查表存在性时出错: {str(e)}")
        table_exists = False
    
    # 如果表不存在，则创建表
    if not table_exists:
        print(f"表不存在，正在创建表: {canbus01_table}")
        create_canbus01_sql = f"""
        CREATE TABLE {canbus01_table} (
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
        
        try:
            spark.sql(create_canbus01_sql)
            print(f"表创建成功: {canbus01_table}")
        except Exception as e:
            print(f"创建表时出错: {str(e)}")
            raise e
    else:
        print(f"表已存在，将追加数据到: {canbus01_table}")
        
        # 显示现有表的schema以便对比
        try:
            existing_df = spark.sql(f"SELECT * FROM {canbus01_table} LIMIT 1")
            print("现有表的schema:")
            existing_df.printSchema()
            
            # 显示现有数据行数
            count_result = spark.sql(f"SELECT COUNT(*) as count FROM {canbus01_table}").collect()
            existing_count = count_result[0]['count']
            print(f"现有表中的数据行数: {existing_count}")
        except Exception as e:
            print(f"查询现有表信息时出错: {str(e)}")
    
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
        
        # 步骤1：移除GreptimeDB特有的列
        columns_to_drop = ["__sequence", "__op_type"]
        df_cleaned = df
        for col_name in columns_to_drop:
            if col_name in df.columns:
                df_cleaned = df_cleaned.drop(col_name)
                print(f"移除列: {col_name}")
        
        # 如果存在原始的__primary_key列，也删除它，我们会重新创建
        if "__primary_key" in df_cleaned.columns:
            df_cleaned = df_cleaned.drop("__primary_key")
            print("移除原始__primary_key列")
        
        # 步骤2：重命名列以匹配表结构（转换为小写）
        column_mapping = {
            "Charging_Time_Remain_Minute": "charging_time_remain_minute",
            "Extender_Starting_Point": "extender_starting_point", 
            "Fuel_CLTC_Mileage": "fuel_cltc_mileage",
            "Fuel_WLTC_Mileage": "fuel_wltc_mileage",
            "Fuel_Percentage": "fuel_percentage",
            "Clean_Mode": "clean_mode",
            "Road_Mode": "road_mode",
            "RESS_Power_Low_Flag": "ress_power_low_flag",
            "Target_SOC": "target_soc",
            "DISPLAY_SPEED": "display_speed",
            "Endurance_Type": "endurance_type"
        }
        
        # 应用列名映射
        for old_name, new_name in column_mapping.items():
            if old_name in df_cleaned.columns:
                df_cleaned = df_cleaned.withColumnRenamed(old_name, new_name)
                print(f"重命名列: {old_name} -> {new_name}")
        
        # 步骤3：为数据添加主键列
        df_with_pk = df_cleaned.withColumn("__primary_key", 
                                          concat(col("vin_id"), lit("_"), col("ts").cast("string")))
        
        print("数据清理和重命名后的schema:")
        df_with_pk.printSchema()
        
        # 步骤4：确保列顺序与表定义匹配
        expected_columns = [
            "__primary_key", "vin_id", "charging_time_remain_minute", "extender_starting_point",
            "fuel_cltc_mileage", "fuel_wltc_mileage", "fuel_percentage", "clean_mode", 
            "road_mode", "ress_power_low_flag", "target_soc", "display_speed", 
            "channel_id", "endurance_type", "ts"
        ]
        
        # 检查所有期望的列是否存在
        missing_columns = []
        for col_name in expected_columns:
            if col_name not in df_with_pk.columns:
                missing_columns.append(col_name)
        
        if missing_columns:
            print(f"警告：缺少以下列: {missing_columns}")
            print(f"实际列: {df_with_pk.columns}")
        
        # 选择并重新排序列（只选择存在的列）
        available_columns = [col_name for col_name in expected_columns if col_name in df_with_pk.columns]
        df_final = df_with_pk.select(*available_columns)
        
        print("最终准备插入的数据schema:")
        df_final.printSchema()
        print(f"最终数据行数: {df_final.count()}")
        
        # 显示最终数据预览
        print("最终数据预览:")
        df_final.show(5, truncate=False)
        
        # 将数据写入canbus01表
        print(f"正在将数据追加到表: {canbus01_table}")
        print(f"准备写入的数据行数: {df_final.count()}")
        
        try:
            df_final.write \
                .format("iceberg") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(canbus01_table)
            
            print("数据追加成功!")
            
            # 显示追加后的总行数
            count_result = spark.sql(f"SELECT COUNT(*) as count FROM {canbus01_table}").collect()
            total_count = count_result[0]['count']
            print(f"追加后表中的总行数: {total_count}")
            
        except Exception as write_error:
            print(f"写入数据时出错: {str(write_error)}")
            print("尝试检查数据兼容性...")
            
            # 如果写入失败，显示更多调试信息
            print("准备写入的数据schema:")
            df_final.printSchema()
            
            # 尝试查询现有表的schema进行对比
            try:
                existing_schema = spark.sql(f"DESCRIBE {canbus01_table}")
                print("现有表的schema:")
                existing_schema.show(truncate=False)
            except Exception as desc_error:
                print(f"无法获取现有表schema: {str(desc_error)}")
            
            raise write_error
        
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
        print("正在追加示例数据到表...")
        try:
            sample_df.write \
                .format("iceberg") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable(canbus01_table)
            
            print("示例数据追加成功!")
            
            # 显示追加后的总行数
            count_result = spark.sql(f"SELECT COUNT(*) as count FROM {canbus01_table}").collect()
            total_count = count_result[0]['count']
            print(f"追加示例数据后表中的总行数: {total_count}")
            
        except Exception as sample_write_error:
            print(f"写入示例数据时出错: {str(sample_write_error)}")
            raise sample_write_error
        
        # 查询示例数据
        result_df = spark.sql(f"SELECT * FROM {canbus01_table}")
        result_df.show(truncate=False)
    
    # 显示所有表
    print(f"显示 {catalog_name}.greptime 命名空间中的所有表:")
    spark.sql(f"SHOW TABLES IN {catalog_name}.greptime").show()
    
    spark.stop()

if __name__ == "__main__":
    main()
