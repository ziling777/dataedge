-- 车辆健康状态监控报表
-- 监控车辆关键指标的异常情况和趋势

WITH daily_stats AS (
    SELECT 
        DATE(ts) as monitor_date,
        vin_id,
        COUNT(*) as daily_records,
        
        -- 燃油/电量相关指标
        AVG(fuel_percentage) as avg_fuel_percentage,
        MIN(fuel_percentage) as min_fuel_percentage,
        MAX(fuel_percentage) as max_fuel_percentage,
        STDDEV(fuel_percentage) as fuel_percentage_stddev,
        
        -- 续航里程指标
        AVG(fuel_cltc_mileage) as avg_cltc_mileage,
        AVG(fuel_wltc_mileage) as avg_wltc_mileage,
        
        -- 速度相关指标
        AVG(display_speed) as avg_speed,
        MAX(display_speed) as max_speed,
        STDDEV(display_speed) as speed_stddev,
        
        -- 异常状态统计
        SUM(CASE WHEN ress_power_low_flag = true THEN 1 ELSE 0 END) as low_power_alerts,
        SUM(CASE WHEN fuel_percentage < 20 THEN 1 ELSE 0 END) as low_fuel_incidents,
        SUM(CASE WHEN display_speed > 100 THEN 1 ELSE 0 END) as high_speed_incidents,
        SUM(CASE WHEN charging_time_remain_minute > 180 THEN 1 ELSE 0 END) as long_charging_incidents,
        
        -- 增程器使用情况
        AVG(extender_starting_point) as avg_extender_point,
        COUNT(CASE WHEN extender_starting_point > 50 THEN 1 END) as high_extender_usage
        
    FROM "s3tablescatalog"."greptime"."canbus01"
    WHERE ts >= DATE_ADD('day', -30, CURRENT_DATE)
    GROUP BY DATE(ts), vin_id
),

health_scores AS (
    SELECT *,
        -- 计算健康评分 (0-100分)
        GREATEST(0, 100 - 
            (low_power_alerts * 5) -           -- 低电量警告扣分
            (low_fuel_incidents * 0.1) -       -- 低燃油扣分
            (high_speed_incidents * 0.2) -     -- 超速扣分
            (long_charging_incidents * 2) -    -- 长时间充电扣分
            (CASE WHEN fuel_percentage_stddev > 20 THEN 10 ELSE 0 END) - -- 燃油波动扣分
            (CASE WHEN speed_stddev > 30 THEN 5 ELSE 0 END)  -- 速度波动扣分
        ) as health_score,
        
        -- 趋势分析 (与前一天比较)
        LAG(avg_fuel_percentage) OVER (PARTITION BY vin_id ORDER BY monitor_date) as prev_fuel_percentage,
        LAG(avg_cltc_mileage) OVER (PARTITION BY vin_id ORDER BY monitor_date) as prev_cltc_mileage
        
    FROM daily_stats
)

SELECT 
    monitor_date,
    vin_id,
    daily_records,
    
    -- 当前状态
    ROUND(avg_fuel_percentage, 2) as avg_fuel_percentage,
    ROUND(avg_cltc_mileage, 2) as avg_cltc_mileage,
    ROUND(avg_wltc_mileage, 2) as avg_wltc_mileage,
    ROUND(avg_speed, 2) as avg_speed,
    ROUND(max_speed, 2) as max_speed,
    
    -- 异常指标
    low_power_alerts,
    low_fuel_incidents,
    high_speed_incidents,
    long_charging_incidents,
    high_extender_usage,
    
    -- 健康评分
    ROUND(health_score, 1) as health_score,
    CASE 
        WHEN health_score >= 90 THEN '优秀'
        WHEN health_score >= 80 THEN '良好'
        WHEN health_score >= 70 THEN '一般'
        WHEN health_score >= 60 THEN '需关注'
        ELSE '需维护'
    END as health_status,
    
    -- 趋势分析
    CASE 
        WHEN prev_fuel_percentage IS NULL THEN '无数据'
        WHEN avg_fuel_percentage > prev_fuel_percentage + 5 THEN '燃油上升'
        WHEN avg_fuel_percentage < prev_fuel_percentage - 5 THEN '燃油下降'
        ELSE '燃油稳定'
    END as fuel_trend,
    
    CASE 
        WHEN prev_cltc_mileage IS NULL THEN '无数据'
        WHEN avg_cltc_mileage > prev_cltc_mileage + 10 THEN '续航改善'
        WHEN avg_cltc_mileage < prev_cltc_mileage - 10 THEN '续航下降'
        ELSE '续航稳定'
    END as mileage_trend

FROM health_scores
ORDER BY monitor_date DESC, health_score ASC;
