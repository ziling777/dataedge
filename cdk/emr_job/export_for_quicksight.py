#!/usr/bin/env python3
"""
为 QuickSight 导出车辆遥测数据
从 S3 Tables 查询数据并导出为 CSV 文件供 QuickSight 使用
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def main():
    if len(sys.argv) != 2:
        print("Usage: export_for_quicksight.py <s3_bucket_name>")
        sys.exit(1)
    
    s3_bucket = sys.argv[1]
    
    # 创建 Spark 会话
    spark = SparkSession.builder \
        .appName("ExportForQuickSight") \
        .getOrCreate()
    
    # S3 Tables 配置
    canbus_table = "gpdemo.greptime.canbus01"
    
    print(f"开始导出 QuickSight 数据...")
    
    try:
        # 读取 S3 Tables 数据
        df = spark.table(canbus_table)
        print(f"成功读取表: {canbus_table}")
        print(f"数据行数: {df.count()}")
        
        # 1. 导出关键指标汇总
        print("导出关键指标汇总...")
        summary_df = df.agg(
            count("*").alias("total_records"),
            countDistinct("vin_id").alias("unique_vehicles"),
            round(avg("fuel_percentage"), 2).alias("avg_fuel_percentage"),
            round(avg("display_speed"), 2).alias("avg_speed"),
            sum(when(col("ress_power_low_flag") == True, 1).otherwise(0)).alias("low_power_alerts"),
            round(avg("fuel_cltc_mileage"), 2).alias("avg_cltc_mileage"),
            round(avg("target_soc"), 2).alias("avg_target_soc")
        )
        
        # 转换为长格式便于 QuickSight 使用
        summary_long = summary_df.select(
            lit("总记录数").alias("metric_name"), col("total_records").cast("double").alias("metric_value")
        ).union(
            summary_df.select(lit("车辆总数").alias("metric_name"), col("unique_vehicles").cast("double").alias("metric_value"))
        ).union(
            summary_df.select(lit("平均燃油百分比").alias("metric_name"), col("avg_fuel_percentage").alias("metric_value"))
        ).union(
            summary_df.select(lit("平均速度").alias("metric_name"), col("avg_speed").alias("metric_value"))
        ).union(
            summary_df.select(lit("低电量警告次数").alias("metric_name"), col("low_power_alerts").cast("double").alias("metric_value"))
        ).union(
            summary_df.select(lit("平均CLTC续航").alias("metric_name"), col("avg_cltc_mileage").alias("metric_value"))
        ).union(
            summary_df.select(lit("平均目标SOC").alias("metric_name"), col("avg_target_soc").alias("metric_value"))
        )
        
        summary_path = f"s3://{s3_bucket}/quicksight-data/summary_metrics.csv"
        summary_long.coalesce(1).write.mode("overwrite").option("header", "true").csv(summary_path)
        print(f"关键指标导出到: {summary_path}")
        
        # 2. 导出驾驶模式分布
        print("导出驾驶模式分布...")
        driving_modes = df.groupBy("clean_mode", "road_mode").agg(
            count("*").alias("record_count"),
            round(avg("fuel_percentage"), 2).alias("avg_fuel"),
            round(avg("display_speed"), 2).alias("avg_speed")
        ).withColumn(
            "mode_description", 
            concat(lit("清洁模式"), col("clean_mode"), lit("-道路模式"), col("road_mode"))
        ).orderBy(desc("record_count"))
        
        modes_path = f"s3://{s3_bucket}/quicksight-data/driving_modes.csv"
        driving_modes.coalesce(1).write.mode("overwrite").option("header", "true").csv(modes_path)
        print(f"驾驶模式分布导出到: {modes_path}")
        
        # 3. 导出速度分布
        print("导出速度分布...")
        speed_dist = df.withColumn(
            "speed_category",
            when(col("display_speed") <= 20, "低速(≤20km/h)")
            .when(col("display_speed") <= 40, "中低速(21-40km/h)")
            .when(col("display_speed") <= 60, "中速(41-60km/h)")
            .when(col("display_speed") <= 80, "中高速(61-80km/h)")
            .when(col("display_speed") <= 100, "高速(81-100km/h)")
            .otherwise("超高速(>100km/h)")
        ).groupBy("speed_category").agg(
            count("*").alias("record_count"),
            round(avg("fuel_percentage"), 2).alias("avg_fuel")
        ).orderBy(desc("record_count"))
        
        speed_path = f"s3://{s3_bucket}/quicksight-data/speed_distribution.csv"
        speed_dist.coalesce(1).write.mode("overwrite").option("header", "true").csv(speed_path)
        print(f"速度分布导出到: {speed_path}")
        
        # 4. 导出充电行为分析
        print("导出充电行为分析...")
        charging_behavior = df.withColumn(
            "charging_category",
            when(col("charging_time_remain_minute") == 0, "无需充电")
            .when(col("charging_time_remain_minute") <= 30, "快速充电(≤30分钟)")
            .when(col("charging_time_remain_minute") <= 120, "标准充电(31-120分钟)")
            .otherwise("长时间充电(>120分钟)")
        ).groupBy("charging_category").agg(
            count("*").alias("session_count"),
            round(avg("target_soc"), 2).alias("avg_target_soc"),
            round(avg("fuel_percentage"), 2).alias("avg_fuel_at_charging")
        ).orderBy(desc("session_count"))
        
        charging_path = f"s3://{s3_bucket}/quicksight-data/charging_behavior.csv"
        charging_behavior.coalesce(1).write.mode("overwrite").option("header", "true").csv(charging_path)
        print(f"充电行为分析导出到: {charging_path}")
        
        # 5. 导出时间序列数据（按小时聚合）
        print("导出时间序列数据...")
        time_series = df.withColumn("hour", date_trunc("hour", col("ts"))) \
            .groupBy("hour").agg(
                count("*").alias("record_count"),
                countDistinct("vin_id").alias("active_vehicles"),
                round(avg("fuel_percentage"), 2).alias("avg_fuel"),
                round(avg("display_speed"), 2).alias("avg_speed"),
                sum(when(col("ress_power_low_flag") == True, 1).otherwise(0)).alias("low_power_alerts")
            ).orderBy("hour")
        
        timeseries_path = f"s3://{s3_bucket}/quicksight-data/time_series.csv"
        time_series.coalesce(1).write.mode("overwrite").option("header", "true").csv(timeseries_path)
        print(f"时间序列数据导出到: {timeseries_path}")
        
        print("✅ 所有数据导出完成!")
        print(f"QuickSight 数据文件保存在: s3://{s3_bucket}/quicksight-data/")
        
        # 显示数据预览
        print("\n📊 关键指标预览:")
        summary_long.show(10, False)
        
        print("\n📈 驾驶模式分布预览:")
        driving_modes.show(5, False)
        
        print("\n🚗 速度分布预览:")
        speed_dist.show(10, False)
        
    except Exception as e:
        print(f"❌ 导出数据时出错: {str(e)}")
        raise e
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
