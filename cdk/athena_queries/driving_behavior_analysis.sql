-- 驾驶行为分析报表
-- 分析驾驶模式、速度分布和充电行为

WITH speed_categories AS (
    SELECT *,
        CASE 
            WHEN display_speed <= 30 THEN '低速(≤30)'
            WHEN display_speed <= 60 THEN '中速(31-60)'
            WHEN display_speed <= 90 THEN '高速(61-90)'
            ELSE '超高速(>90)'
        END as speed_category,
        CASE 
            WHEN charging_time_remain_minute = 0 THEN '无需充电'
            WHEN charging_time_remain_minute <= 30 THEN '快速充电(≤30分钟)'
            WHEN charging_time_remain_minute <= 120 THEN '标准充电(31-120分钟)'
            ELSE '长时间充电(>120分钟)'
        END as charging_category
    FROM "s3tablescatalog"."greptime"."canbus01"
)

SELECT 
    DATE(ts) as analysis_date,
    vin_id,
    
    -- 驾驶模式分析
    COUNT(*) as total_records,
    COUNT(DISTINCT clean_mode) as clean_modes_used,
    COUNT(DISTINCT road_mode) as road_modes_used,
    
    -- 速度分析
    speed_category,
    COUNT(*) as speed_category_count,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY DATE(ts), vin_id), 2) as speed_category_percentage,
    AVG(display_speed) as avg_speed,
    MIN(display_speed) as min_speed,
    MAX(display_speed) as max_speed,
    
    -- 充电行为分析
    charging_category,
    AVG(charging_time_remain_minute) as avg_charging_time,
    AVG(target_soc) as avg_target_soc,
    
    -- 能耗状态
    AVG(fuel_percentage) as avg_fuel_level,
    COUNT(CASE WHEN ress_power_low_flag = true THEN 1 END) as low_power_incidents,
    
    -- 增程器使用分析
    AVG(extender_starting_point) as avg_extender_point,
    COUNT(CASE WHEN extender_starting_point > 0 THEN 1 END) as extender_usage_count

FROM speed_categories
WHERE ts >= DATE_ADD('day', -7, CURRENT_DATE)  -- 最近7天数据
GROUP BY DATE(ts), vin_id, speed_category, charging_category
ORDER BY analysis_date DESC, vin_id, speed_category;
