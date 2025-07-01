from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sys

def create_aggregated_tables():
    # 获取命令行参数中的S3桶名
    if len(sys.argv) > 1:
        bucket_name = sys.argv[1]
    else:
        bucket_name = "default-bucket-name"
    
    # 使用固定的catalog名称"gpdemo"
    catalog_name = "gpdemo"
    
    # 创建SparkSession
    spark = SparkSession.builder \
        .appName("创建聚合表") \
        .getOrCreate()
    
    # 基础表名
    base_table = f"{catalog_name}.greptime.canbus01"
    
    print(f"基于表 {base_table} 创建聚合表...")
    
    # 1. 创建车辆日常汇总表
    daily_summary_table = f"{catalog_name}.greptime.vehicle_daily_summary"
    
    # 检查表是否存在
    if spark.catalog.tableExists(daily_summary_table):
        print(f"表 {daily_summary_table} 已存在，删除后重新创建...")
        spark.sql(f"DROP TABLE {daily_summary_table}")
    
    print(f"创建日常汇总表: {daily_summary_table}")
    spark.sql(f"""
    CREATE TABLE {daily_summary_table}
    USING ICEBERG
    PARTITIONED BY (date)
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
    )
    AS
    SELECT 
        vin_id,
        DATE(ts) as date,
        AVG(fuel_percentage) as avg_fuel_percentage,
        MIN(fuel_percentage) as min_fuel_percentage,
        MAX(fuel_percentage) as max_fuel_percentage,
        AVG(display_speed) as avg_display_speed,
        MAX(display_speed) as max_display_speed,
        AVG(target_soc) as avg_target_soc,
        COUNT(*) as record_count,
        MIN(ts) as first_record_time,
        MAX(ts) as last_record_time
    FROM {base_table}
    GROUP BY vin_id, DATE(ts)
    """)
    
    # 2. 创建充电行为分析表
    charging_analysis_table = f"{catalog_name}.greptime.charging_behavior_analysis"
    
    if spark.catalog.tableExists(charging_analysis_table):
        print(f"表 {charging_analysis_table} 已存在，删除后重新创建...")
        spark.sql(f"DROP TABLE {charging_analysis_table}")
    
    print(f"创建充电行为分析表: {charging_analysis_table}")
    spark.sql(f"""
    CREATE TABLE {charging_analysis_table}
    USING ICEBERG
    PARTITIONED BY (date)
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
    )
    AS
    SELECT 
        vin_id,
        DATE(ts) as date,
        AVG(charging_time_remain_minute) as avg_charging_time_remain,
        MAX(charging_time_remain_minute) as max_charging_time_remain,
        COUNT(CASE WHEN charging_time_remain_minute > 0 THEN 1 END) as charging_sessions,
        AVG(CASE WHEN charging_time_remain_minute > 0 THEN fuel_percentage END) as avg_fuel_during_charging,
        COUNT(*) as total_records
    FROM {base_table}
    GROUP BY vin_id, DATE(ts)
    """)
    
    # 3. 创建驾驶模式分析表
    driving_mode_table = f"{catalog_name}.greptime.driving_mode_analysis"
    
    if spark.catalog.tableExists(driving_mode_table):
        print(f"表 {driving_mode_table} 已存在，删除后重新创建...")
        spark.sql(f"DROP TABLE {driving_mode_table}")
    
    print(f"创建驾驶模式分析表: {driving_mode_table}")
    spark.sql(f"""
    CREATE TABLE {driving_mode_table}
    USING ICEBERG
    PARTITIONED BY (date)
    TBLPROPERTIES (
        'write.format.default' = 'parquet',
        'write.parquet.compression-codec' = 'snappy'
    )
    AS
    SELECT 
        vin_id,
        DATE(ts) as date,
        clean_mode,
        road_mode,
        COUNT(*) as mode_usage_count,
        AVG(display_speed) as avg_speed_in_mode,
        AVG(fuel_percentage) as avg_fuel_in_mode,
        SUM(CASE WHEN ress_power_low_flag THEN 1 ELSE 0 END) as low_power_incidents
    FROM {base_table}
    GROUP BY vin_id, DATE(ts), clean_mode, road_mode
    """)
    
    # 查询并显示创建的表
    print("\n=== 创建的聚合表汇总 ===")
    
    print(f"\n1. 车辆日常汇总表 ({daily_summary_table}):")
    spark.sql(f"SELECT * FROM {daily_summary_table} LIMIT 5").show(truncate=False)
    
    print(f"\n2. 充电行为分析表 ({charging_analysis_table}):")
    spark.sql(f"SELECT * FROM {charging_analysis_table} LIMIT 5").show(truncate=False)
    
    print(f"\n3. 驾驶模式分析表 ({driving_mode_table}):")
    spark.sql(f"SELECT * FROM {driving_mode_table} LIMIT 5").show(truncate=False)
    
    # 显示表统计信息
    print("\n=== 表统计信息 ===")
    for table_name in [daily_summary_table, charging_analysis_table, driving_mode_table]:
        count_result = spark.sql(f"SELECT COUNT(*) as count FROM {table_name}").collect()
        count = count_result[0]['count']
        print(f"{table_name}: {count} 行")
    
    spark.stop()

if __name__ == "__main__":
    create_aggregated_tables()
