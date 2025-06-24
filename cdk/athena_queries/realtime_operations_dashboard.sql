-- 实时运营监控仪表板
-- 提供车队整体运营状况的实时概览

WITH latest_data AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY vin_id ORDER BY ts DESC) as rn
    FROM "s3tablescatalog"."greptime"."canbus01"
    WHERE ts >= DATE_ADD('hour', -24, CURRENT_TIMESTAMP)  -- 最近24小时
),

current_status AS (
    SELECT *
    FROM latest_data 
    WHERE rn = 1  -- 每辆车的最新状态
),

hourly_stats AS (
    SELECT 
        DATE_TRUNC('hour', ts) as hour_timestamp,
        COUNT(DISTINCT vin_id) as active_vehicles,
        COUNT(*) as total_records,
        AVG(fuel_percentage) as avg_fuel_level,
        AVG(display_speed) as avg_speed,
        SUM(CASE WHEN ress_power_low_flag = true THEN 1 ELSE 0 END) as low_power_alerts,
        SUM(CASE WHEN charging_time_remain_minute > 0 THEN 1 ELSE 0 END) as charging_sessions
    FROM "s3tablescatalog"."greptime"."canbus01"
    WHERE ts >= DATE_ADD('hour', -24, CURRENT_TIMESTAMP)
    GROUP BY DATE_TRUNC('hour', ts)
)

-- 主仪表板查询
SELECT 
    '车队概览' as dashboard_section,
    
    -- 当前车队状态
    (SELECT COUNT(DISTINCT vin_id) FROM current_status) as total_active_vehicles,
    (SELECT COUNT(*) FROM current_status WHERE ress_power_low_flag = true) as vehicles_with_low_power,
    (SELECT COUNT(*) FROM current_status WHERE charging_time_remain_minute > 0) as vehicles_charging,
    (SELECT COUNT(*) FROM current_status WHERE fuel_percentage < 20) as vehicles_low_fuel,
    
    -- 平均指标
    (SELECT ROUND(AVG(fuel_percentage), 1) FROM current_status) as fleet_avg_fuel_percentage,
    (SELECT ROUND(AVG(display_speed), 1) FROM current_status) as fleet_avg_speed,
    (SELECT ROUND(AVG(target_soc), 1) FROM current_status) as fleet_avg_target_soc,
    (SELECT ROUND(AVG(fuel_cltc_mileage), 1) FROM current_status) as fleet_avg_cltc_mileage,
    
    -- 运营效率指标
    (SELECT ROUND(AVG(fuel_cltc_mileage / NULLIF(fuel_percentage, 0)), 2) FROM current_status) as fleet_efficiency_ratio,
    (SELECT COUNT(*) FROM current_status WHERE extender_starting_point > 30) as high_extender_usage_vehicles,
    
    -- 告警统计
    (SELECT COUNT(*) FROM current_status WHERE fuel_percentage < 15) as critical_fuel_alerts,
    (SELECT COUNT(*) FROM current_status WHERE charging_time_remain_minute > 180) as long_charging_alerts,
    (SELECT COUNT(*) FROM current_status WHERE display_speed > 100) as speed_violation_alerts,
    
    CURRENT_TIMESTAMP as report_timestamp

UNION ALL

-- 24小时趋势数据
SELECT 
    '24小时趋势' as dashboard_section,
    
    -- 时间段标识
    CAST(EXTRACT(hour FROM hour_timestamp) as VARCHAR) as hour_of_day,
    active_vehicles,
    total_records,
    ROUND(avg_fuel_level, 1) as hourly_avg_fuel,
    ROUND(avg_speed, 1) as hourly_avg_speed,
    low_power_alerts,
    charging_sessions,
    
    -- 计算变化趋势
    active_vehicles - LAG(active_vehicles) OVER (ORDER BY hour_timestamp) as vehicle_count_change,
    ROUND(avg_fuel_level - LAG(avg_fuel_level) OVER (ORDER BY hour_timestamp), 2) as fuel_level_change,
    ROUND(avg_speed - LAG(avg_speed) OVER (ORDER BY hour_timestamp), 2) as speed_change,
    
    -- 异常检测
    CASE 
        WHEN low_power_alerts > 5 THEN '高告警'
        WHEN low_power_alerts > 2 THEN '中告警'
        WHEN low_power_alerts > 0 THEN '低告警'
        ELSE '正常'
    END as alert_level,
    
    hour_timestamp as report_timestamp

FROM hourly_stats
ORDER BY dashboard_section, report_timestamp DESC;
