#!/usr/bin/env python3
"""
生成 QuickSight 看板数据的 EMR 作业
从 S3 Tables 查询数据并生成聚合结果保存到 S3
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def main():
    if len(sys.argv) != 2:
        print("Usage: generate_dashboard_data.py <s3_bucket_name>")
        sys.exit(1)
    
    s3_bucket = sys.argv[1]
    
    # 创建 Spark 会话
    spark = SparkSession.builder \
        .appName("GenerateDashboardData") \
        .getOrCreate()
    
    # S3 Tables 配置
    canbus01_table = "gpdemo.greptime.canbus01"
    
    print(f"开始生成看板数据...")
    
    try:
        # 读取 S3 Tables 数据
        df = spark.table(canbus01_table)
        print(f"成功读取表: {canbus01_table}")
        print(f"数据行数: {df.count()}")
        
        # 1. 生成关键指标统计
        print("生成关键指标统计...")
        key_metrics = spark.sql(f"""
            SELECT 
                '车辆总数' as metric_name,
                COUNT(DISTINCT vin_id) as metric_value,
                '辆' as unit
            FROM {canbus01_table}
            
            UNION ALL
            
            SELECT 
                '总记录数' as metric_name,
                COUNT(*) as metric_value,
                '条' as unit
            FROM {canbus01_table}
            
            UNION ALL
            
            SELECT 
                '平均燃油百分比' as metric_name,
                ROUND(AVG(fuel_percentage), 2) as metric_value,
                '%' as unit
            FROM {canbus01_table}
            
            UNION ALL
            
            SELECT 
                '平均速度' as metric_name,
                ROUND(AVG(display_speed), 2) as metric_value,
                'km/h' as unit
            FROM {canbus01_table}
            
            UNION ALL
            
            SELECT 
                '低电量警告' as metric_name,
                SUM(CASE WHEN ress_power_low_flag = true THEN 1 ELSE 0 END) as metric_value,
                '次' as unit
            FROM {canbus01_table}
            
            UNION ALL
            
            SELECT 
                '平均CLTC续航' as metric_name,
                ROUND(AVG(fuel_cltc_mileage), 2) as metric_value,
                'km' as unit
            FROM {canbus01_table}
            
            UNION ALL
            
            SELECT 
                '平均目标SOC' as metric_name,
                ROUND(AVG(target_soc), 2) as metric_value,
                '%' as unit
            FROM {canbus01_table}
        """)
        
        # 保存关键指标
        key_metrics_path = f"s3://{s3_bucket}/dashboard-data/key_metrics/"
        key_metrics.coalesce(1).write.mode("overwrite").option("header", "true").csv(key_metrics_path)
        print(f"关键指标保存到: {key_metrics_path}")
        
        # 2. 生成驾驶模式分布
        print("生成驾驶模式分布...")
        driving_modes = spark.sql(f"""
            SELECT 
                CONCAT('清洁模式', clean_mode) as mode_name,
                COUNT(*) as record_count,
                ROUND(AVG(fuel_percentage), 2) as avg_fuel,
                ROUND(AVG(display_speed), 2) as avg_speed,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM {canbus01_table}
            GROUP BY clean_mode
            ORDER BY record_count DESC
        """)
        
        driving_modes_path = f"s3://{s3_bucket}/dashboard-data/driving_modes/"
        driving_modes.coalesce(1).write.mode("overwrite").option("header", "true").csv(driving_modes_path)
        print(f"驾驶模式分布保存到: {driving_modes_path}")
        
        # 3. 生成速度分布
        print("生成速度分布...")
        speed_distribution = spark.sql(f"""
            SELECT 
                CASE 
                    WHEN display_speed <= 20 THEN '低速(≤20km/h)'
                    WHEN display_speed <= 40 THEN '中低速(21-40km/h)'
                    WHEN display_speed <= 60 THEN '中速(41-60km/h)'
                    WHEN display_speed <= 80 THEN '中高速(61-80km/h)'
                    WHEN display_speed <= 100 THEN '高速(81-100km/h)'
                    ELSE '超高速(>100km/h)'
                END as speed_category,
                COUNT(*) as record_count,
                ROUND(AVG(fuel_percentage), 2) as avg_fuel,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM {canbus01_table}
            GROUP BY 
                CASE 
                    WHEN display_speed <= 20 THEN '低速(≤20km/h)'
                    WHEN display_speed <= 40 THEN '中低速(21-40km/h)'
                    WHEN display_speed <= 60 THEN '中速(41-60km/h)'
                    WHEN display_speed <= 80 THEN '中高速(61-80km/h)'
                    WHEN display_speed <= 100 THEN '高速(81-100km/h)'
                    ELSE '超高速(>100km/h)'
                END
            ORDER BY record_count DESC
        """)
        
        speed_dist_path = f"s3://{s3_bucket}/dashboard-data/speed_distribution/"
        speed_distribution.coalesce(1).write.mode("overwrite").option("header", "true").csv(speed_dist_path)
        print(f"速度分布保存到: {speed_dist_path}")
        
        # 4. 生成充电行为分析
        print("生成充电行为分析...")
        charging_behavior = spark.sql(f"""
            SELECT 
                CASE 
                    WHEN charging_time_remain_minute = 0 THEN '无需充电'
                    WHEN charging_time_remain_minute <= 30 THEN '快速充电(≤30分钟)'
                    WHEN charging_time_remain_minute <= 120 THEN '标准充电(31-120分钟)'
                    ELSE '长时间充电(>120分钟)'
                END as charging_category,
                COUNT(*) as session_count,
                ROUND(AVG(target_soc), 2) as avg_target_soc,
                ROUND(AVG(fuel_percentage), 2) as avg_fuel_at_charging,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
            FROM {canbus01_table}
            GROUP BY 
                CASE 
                    WHEN charging_time_remain_minute = 0 THEN '无需充电'
                    WHEN charging_time_remain_minute <= 30 THEN '快速充电(≤30分钟)'
                    WHEN charging_time_remain_minute <= 120 THEN '标准充电(31-120分钟)'
                    ELSE '长时间充电(>120分钟)'
                END
            ORDER BY session_count DESC
        """)
        
        charging_path = f"s3://{s3_bucket}/dashboard-data/charging_behavior/"
        charging_behavior.coalesce(1).write.mode("overwrite").option("header", "true").csv(charging_path)
        print(f"充电行为分析保存到: {charging_path}")
        
        # 5. 生成时间序列数据
        print("生成时间序列数据...")
        time_series = spark.sql(f"""
            SELECT 
                DATE_TRUNC('hour', ts) as hour_timestamp,
                COUNT(*) as record_count,
                COUNT(DISTINCT vin_id) as active_vehicles,
                ROUND(AVG(fuel_percentage), 2) as avg_fuel,
                ROUND(AVG(display_speed), 2) as avg_speed,
                SUM(CASE WHEN ress_power_low_flag = true THEN 1 ELSE 0 END) as low_power_alerts,
                SUM(CASE WHEN charging_time_remain_minute > 0 THEN 1 ELSE 0 END) as charging_sessions
            FROM {canbus01_table}
            GROUP BY DATE_TRUNC('hour', ts)
            ORDER BY hour_timestamp
        """)
        
        time_series_path = f"s3://{s3_bucket}/dashboard-data/time_series/"
        time_series.coalesce(1).write.mode("overwrite").option("header", "true").csv(time_series_path)
        print(f"时间序列数据保存到: {time_series_path}")
        
        print("✅ 所有看板数据生成完成!")
        print(f"数据保存在: s3://{s3_bucket}/dashboard-data/")
        
        # 显示生成的数据预览
        print("\n📊 关键指标预览:")
        key_metrics.show(10, False)
        
        print("\n📈 驾驶模式分布预览:")
        driving_modes.show(10, False)
        
        print("\n🚗 速度分布预览:")
        speed_distribution.show(10, False)
        
    except Exception as e:
        print(f"❌ 生成看板数据时出错: {str(e)}")
        raise e
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
