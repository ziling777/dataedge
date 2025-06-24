#!/usr/bin/env python3
"""
使用 Spark SQL 在 EMR 中执行报表查询
这些查询可以直接在 EMR Spark 环境中运行
"""

# 报表1: 车辆基础统计报表
BASIC_STATS_REPORT = """
-- 车辆基础统计报表
SELECT 
    '车队基础统计' as report_type,
    COUNT(*) as total_records,
    COUNT(DISTINCT vin_id) as unique_vehicles,
    ROUND(AVG(fuel_percentage), 2) as avg_fuel_percentage,
    ROUND(AVG(display_speed), 2) as avg_speed,
    ROUND(AVG(fuel_cltc_mileage), 2) as avg_cltc_mileage,
    ROUND(AVG(fuel_wltc_mileage), 2) as avg_wltc_mileage,
    SUM(CASE WHEN ress_power_low_flag = true THEN 1 ELSE 0 END) as low_power_alerts,
    SUM(CASE WHEN charging_time_remain_minute > 0 THEN 1 ELSE 0 END) as charging_sessions,
    ROUND(AVG(target_soc), 2) as avg_target_soc
FROM gpdemo.greptime.canbus01;
"""

# 报表2: 驾驶模式分析
DRIVING_MODE_REPORT = """
-- 驾驶模式分析报表
SELECT 
    clean_mode,
    road_mode,
    COUNT(*) as record_count,
    ROUND(AVG(fuel_percentage), 2) as avg_fuel_percentage,
    ROUND(AVG(display_speed), 2) as avg_speed,
    ROUND(AVG(fuel_cltc_mileage), 2) as avg_cltc_mileage,
    SUM(CASE WHEN ress_power_low_flag = true THEN 1 ELSE 0 END) as low_power_incidents,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage_of_total
FROM gpdemo.greptime.canbus01
GROUP BY clean_mode, road_mode
ORDER BY record_count DESC;
"""

# 报表3: 速度分布分析
SPEED_DISTRIBUTION_REPORT = """
-- 速度分布分析报表
SELECT 
    CASE 
        WHEN display_speed <= 20 THEN '低速(≤20)'
        WHEN display_speed <= 40 THEN '中低速(21-40)'
        WHEN display_speed <= 60 THEN '中速(41-60)'
        WHEN display_speed <= 80 THEN '中高速(61-80)'
        WHEN display_speed <= 100 THEN '高速(81-100)'
        ELSE '超高速(>100)'
    END as speed_category,
    COUNT(*) as record_count,
    ROUND(AVG(fuel_percentage), 2) as avg_fuel_at_speed,
    ROUND(AVG(fuel_cltc_mileage), 2) as avg_cltc_mileage,
    SUM(CASE WHEN ress_power_low_flag = true THEN 1 ELSE 0 END) as low_power_incidents,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM gpdemo.greptime.canbus01
GROUP BY 
    CASE 
        WHEN display_speed <= 20 THEN '低速(≤20)'
        WHEN display_speed <= 40 THEN '中低速(21-40)'
        WHEN display_speed <= 60 THEN '中速(41-60)'
        WHEN display_speed <= 80 THEN '中高速(61-80)'
        WHEN display_speed <= 100 THEN '高速(81-100)'
        ELSE '超高速(>100)'
    END
ORDER BY 
    CASE 
        WHEN speed_category = '低速(≤20)' THEN 1
        WHEN speed_category = '中低速(21-40)' THEN 2
        WHEN speed_category = '中速(41-60)' THEN 3
        WHEN speed_category = '中高速(61-80)' THEN 4
        WHEN speed_category = '高速(81-100)' THEN 5
        ELSE 6
    END;
"""

# 报表4: 充电行为分析
CHARGING_BEHAVIOR_REPORT = """
-- 充电行为分析报表
SELECT 
    CASE 
        WHEN charging_time_remain_minute = 0 THEN '无需充电'
        WHEN charging_time_remain_minute <= 30 THEN '快速充电(≤30分钟)'
        WHEN charging_time_remain_minute <= 60 THEN '标准充电(31-60分钟)'
        WHEN charging_time_remain_minute <= 120 THEN '长时间充电(61-120分钟)'
        ELSE '超长充电(>120分钟)'
    END as charging_category,
    COUNT(*) as session_count,
    ROUND(AVG(charging_time_remain_minute), 2) as avg_charging_time,
    ROUND(AVG(fuel_percentage), 2) as avg_fuel_at_charging,
    ROUND(AVG(target_soc), 2) as avg_target_soc,
    ROUND(AVG(target_soc - fuel_percentage), 2) as avg_soc_gap,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM gpdemo.greptime.canbus01
GROUP BY 
    CASE 
        WHEN charging_time_remain_minute = 0 THEN '无需充电'
        WHEN charging_time_remain_minute <= 30 THEN '快速充电(≤30分钟)'
        WHEN charging_time_remain_minute <= 60 THEN '标准充电(31-60分钟)'
        WHEN charging_time_remain_minute <= 120 THEN '长时间充电(61-120分钟)'
        ELSE '超长充电(>120分钟)'
    END
ORDER BY session_count DESC;
"""

# 报表5: 燃油效率分析
FUEL_EFFICIENCY_REPORT = """
-- 燃油效率分析报表
SELECT 
    CASE 
        WHEN fuel_percentage <= 20 THEN '低油量(≤20%)'
        WHEN fuel_percentage <= 40 THEN '中低油量(21-40%)'
        WHEN fuel_percentage <= 60 THEN '中等油量(41-60%)'
        WHEN fuel_percentage <= 80 THEN '中高油量(61-80%)'
        ELSE '高油量(>80%)'
    END as fuel_level_category,
    COUNT(*) as record_count,
    ROUND(AVG(fuel_percentage), 2) as avg_fuel_percentage,
    ROUND(AVG(fuel_cltc_mileage), 2) as avg_cltc_mileage,
    ROUND(AVG(fuel_wltc_mileage), 2) as avg_wltc_mileage,
    ROUND(AVG(display_speed), 2) as avg_speed,
    ROUND(AVG(fuel_cltc_mileage) / NULLIF(AVG(fuel_percentage), 0), 3) as cltc_efficiency_ratio,
    ROUND(AVG(fuel_wltc_mileage) / NULLIF(AVG(fuel_percentage), 0), 3) as wltc_efficiency_ratio,
    SUM(CASE WHEN ress_power_low_flag = true THEN 1 ELSE 0 END) as low_power_alerts
FROM gpdemo.greptime.canbus01
GROUP BY 
    CASE 
        WHEN fuel_percentage <= 20 THEN '低油量(≤20%)'
        WHEN fuel_percentage <= 40 THEN '中低油量(21-40%)'
        WHEN fuel_percentage <= 60 THEN '中等油量(41-60%)'
        WHEN fuel_percentage <= 80 THEN '中高油量(61-80%)'
        ELSE '高油量(>80%)'
    END
ORDER BY 
    CASE 
        WHEN fuel_level_category = '低油量(≤20%)' THEN 1
        WHEN fuel_level_category = '中低油量(21-40%)' THEN 2
        WHEN fuel_level_category = '中等油量(41-60%)' THEN 3
        WHEN fuel_level_category = '中高油量(61-80%)' THEN 4
        ELSE 5
    END;
"""

# 报表6: 增程器使用分析
EXTENDER_USAGE_REPORT = """
-- 增程器使用分析报表
SELECT 
    CASE 
        WHEN extender_starting_point = 0 THEN '未使用增程器'
        WHEN extender_starting_point <= 20 THEN '低频使用(≤20)'
        WHEN extender_starting_point <= 40 THEN '中频使用(21-40)'
        WHEN extender_starting_point <= 60 THEN '高频使用(41-60)'
        ELSE '超高频使用(>60)'
    END as extender_usage_category,
    COUNT(*) as record_count,
    ROUND(AVG(extender_starting_point), 2) as avg_extender_point,
    ROUND(AVG(fuel_percentage), 2) as avg_fuel_percentage,
    ROUND(AVG(fuel_cltc_mileage), 2) as avg_cltc_mileage,
    ROUND(AVG(display_speed), 2) as avg_speed,
    SUM(CASE WHEN ress_power_low_flag = true THEN 1 ELSE 0 END) as low_power_incidents,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage,
    endurance_type,
    COUNT(DISTINCT endurance_type) as endurance_types_used
FROM gpdemo.greptime.canbus01
GROUP BY 
    CASE 
        WHEN extender_starting_point = 0 THEN '未使用增程器'
        WHEN extender_starting_point <= 20 THEN '低频使用(≤20)'
        WHEN extender_starting_point <= 40 THEN '中频使用(21-40)'
        WHEN extender_starting_point <= 60 THEN '高频使用(41-60)'
        ELSE '超高频使用(>60)'
    END,
    endurance_type
ORDER BY record_count DESC;
"""

print("=== S3 Tables 车辆遥测数据分析报表 ===")
print("\n以下是6个业务报表的SQL查询，可以在EMR Spark环境中执行：")
print("\n1. 车辆基础统计报表")
print(BASIC_STATS_REPORT)
print("\n" + "="*80)

print("\n2. 驾驶模式分析报表") 
print(DRIVING_MODE_REPORT)
print("\n" + "="*80)

print("\n3. 速度分布分析报表")
print(SPEED_DISTRIBUTION_REPORT)
print("\n" + "="*80)

print("\n4. 充电行为分析报表")
print(CHARGING_BEHAVIOR_REPORT)
print("\n" + "="*80)

print("\n5. 燃油效率分析报表")
print(FUEL_EFFICIENCY_REPORT)
print("\n" + "="*80)

print("\n6. 增程器使用分析报表")
print(EXTENDER_USAGE_REPORT)
print("\n" + "="*80)
