-- 车辆能耗效率分析报表
-- 分析不同驾驶模式下的能耗表现和续航里程

SELECT 
    DATE(ts) as report_date,
    clean_mode,
    road_mode,
    COUNT(*) as record_count,
    AVG(fuel_percentage) as avg_fuel_percentage,
    AVG(fuel_cltc_mileage) as avg_cltc_mileage,
    AVG(fuel_wltc_mileage) as avg_wltc_mileage,
    AVG(display_speed) as avg_speed,
    AVG(target_soc) as avg_target_soc,
    -- 计算能耗效率指标
    ROUND(AVG(fuel_cltc_mileage) / NULLIF(AVG(fuel_percentage), 0), 2) as cltc_efficiency_ratio,
    ROUND(AVG(fuel_wltc_mileage) / NULLIF(AVG(fuel_percentage), 0), 2) as wltc_efficiency_ratio,
    -- 统计低电量警告次数
    SUM(CASE WHEN ress_power_low_flag = true THEN 1 ELSE 0 END) as low_power_warnings,
    ROUND(100.0 * SUM(CASE WHEN ress_power_low_flag = true THEN 1 ELSE 0 END) / COUNT(*), 2) as low_power_warning_rate
FROM "s3tablescatalog"."greptime"."canbus01"
WHERE ts >= DATE_ADD('day', -30, CURRENT_DATE)  -- 最近30天数据
GROUP BY DATE(ts), clean_mode, road_mode
ORDER BY report_date DESC, clean_mode, road_mode;
