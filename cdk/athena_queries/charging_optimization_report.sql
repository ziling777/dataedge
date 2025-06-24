-- 充电行为优化建议报表
-- 分析充电模式，提供优化建议

WITH charging_analysis AS (
    SELECT 
        vin_id,
        DATE(ts) as charge_date,
        HOUR(ts) as charge_hour,
        
        -- 充电相关指标
        charging_time_remain_minute,
        target_soc,
        fuel_percentage,
        
        -- 充电效率计算
        CASE 
            WHEN charging_time_remain_minute > 0 THEN 
                ROUND((target_soc - fuel_percentage) / NULLIF(charging_time_remain_minute, 0) * 60, 2)
            ELSE 0 
        END as charging_efficiency_per_hour,
        
        -- 充电时段分类
        CASE 
            WHEN HOUR(ts) BETWEEN 6 AND 10 THEN '早高峰'
            WHEN HOUR(ts) BETWEEN 11 AND 14 THEN '午间'
            WHEN HOUR(ts) BETWEEN 17 AND 20 THEN '晚高峰'
            WHEN HOUR(ts) BETWEEN 21 AND 23 THEN '夜间'
            ELSE '深夜/凌晨'
        END as time_period,
        
        -- 充电紧急程度
        CASE 
            WHEN fuel_percentage < 20 THEN '紧急充电'
            WHEN fuel_percentage < 40 THEN '预防性充电'
            WHEN fuel_percentage < 60 THEN '常规充电'
            ELSE '补充充电'
        END as charging_urgency,
        
        -- 增程器依赖度
        extender_starting_point,
        endurance_type
        
    FROM "s3tablescatalog"."greptime"."canbus01"
    WHERE charging_time_remain_minute > 0  -- 只分析有充电需求的记录
      AND ts >= DATE_ADD('day', -14, CURRENT_DATE)  -- 最近14天
)

SELECT 
    vin_id,
    
    -- 充电行为统计
    COUNT(*) as total_charging_sessions,
    ROUND(AVG(charging_time_remain_minute), 1) as avg_charging_time_minutes,
    ROUND(AVG(target_soc), 1) as avg_target_soc,
    ROUND(AVG(fuel_percentage), 1) as avg_fuel_at_charging,
    ROUND(AVG(charging_efficiency_per_hour), 2) as avg_charging_efficiency,
    
    -- 时段分析
    time_period,
    COUNT(*) as sessions_in_period,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY vin_id), 1) as period_percentage,
    
    -- 充电紧急程度分析
    charging_urgency,
    COUNT(*) as urgency_sessions,
    ROUND(AVG(charging_time_remain_minute), 1) as avg_time_by_urgency,
    
    -- 增程器使用分析
    ROUND(AVG(extender_starting_point), 1) as avg_extender_point,
    COUNT(CASE WHEN extender_starting_point > 30 THEN 1 END) as high_extender_usage,
    
    -- 优化建议生成
    CASE 
        WHEN AVG(fuel_percentage) < 25 THEN '建议提前充电，避免紧急充电'
        WHEN AVG(charging_time_remain_minute) > 120 THEN '建议使用快充设施或调整充电策略'
        WHEN COUNT(CASE WHEN time_period IN ('早高峰', '晚高峰') THEN 1 END) > COUNT(*) * 0.6 
             THEN '建议避开高峰时段充电，可节省时间和成本'
        WHEN AVG(target_soc) > 90 THEN '建议适当降低目标SOC，延长电池寿命'
        WHEN AVG(extender_starting_point) > 40 THEN '增程器使用频繁，建议检查电池状态'
        ELSE '充电行为较为合理'
    END as optimization_suggestion,
    
    -- 成本效益分析
    CASE 
        WHEN time_period = '深夜/凌晨' THEN '低峰电价时段，成本较低'
        WHEN time_period IN ('早高峰', '晚高峰') THEN '高峰电价时段，成本较高'
        ELSE '平峰电价时段，成本适中'
    END as cost_analysis,
    
    -- 充电效率评级
    CASE 
        WHEN AVG(charging_efficiency_per_hour) > 30 THEN '高效充电'
        WHEN AVG(charging_efficiency_per_hour) > 20 THEN '中效充电'
        WHEN AVG(charging_efficiency_per_hour) > 10 THEN '低效充电'
        ELSE '充电效率待改善'
    END as efficiency_rating

FROM charging_analysis
GROUP BY vin_id, time_period, charging_urgency
ORDER BY vin_id, sessions_in_period DESC;
