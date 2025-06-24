-- 车辆性能对比分析报表
-- 对比不同车辆的性能表现，识别最佳和需改进的车辆

WITH vehicle_performance AS (
    SELECT 
        vin_id,
        COUNT(*) as total_records,
        
        -- 燃油效率指标
        AVG(fuel_percentage) as avg_fuel_percentage,
        AVG(fuel_cltc_mileage) as avg_cltc_mileage,
        AVG(fuel_wltc_mileage) as avg_wltc_mileage,
        ROUND(AVG(fuel_cltc_mileage) / NULLIF(AVG(fuel_percentage), 0), 2) as fuel_efficiency_ratio,
        
        -- 驾驶行为指标
        AVG(display_speed) as avg_speed,
        STDDEV(display_speed) as speed_consistency,
        MAX(display_speed) as max_speed,
        
        -- 充电行为指标
        AVG(charging_time_remain_minute) as avg_charging_time,
        AVG(target_soc) as avg_target_soc,
        COUNT(CASE WHEN charging_time_remain_minute > 0 THEN 1 END) as charging_sessions,
        
        -- 增程器使用
        AVG(extender_starting_point) as avg_extender_usage,
        COUNT(CASE WHEN extender_starting_point > 30 THEN 1 END) as high_extender_sessions,
        
        -- 异常情况统计
        SUM(CASE WHEN ress_power_low_flag = true THEN 1 ELSE 0 END) as low_power_incidents,
        SUM(CASE WHEN fuel_percentage < 20 THEN 1 ELSE 0 END) as low_fuel_incidents,
        SUM(CASE WHEN display_speed > 100 THEN 1 ELSE 0 END) as speeding_incidents,
        
        -- 驾驶模式多样性
        COUNT(DISTINCT clean_mode) as clean_modes_used,
        COUNT(DISTINCT road_mode) as road_modes_used,
        COUNT(DISTINCT endurance_type) as endurance_types_used
        
    FROM "s3tablescatalog"."greptime"."canbus01"
    WHERE ts >= DATE_ADD('day', -30, CURRENT_DATE)  -- 最近30天
    GROUP BY vin_id
),

performance_rankings AS (
    SELECT *,
        -- 性能排名
        ROW_NUMBER() OVER (ORDER BY fuel_efficiency_ratio DESC) as efficiency_rank,
        ROW_NUMBER() OVER (ORDER BY avg_speed DESC) as speed_rank,
        ROW_NUMBER() OVER (ORDER BY speed_consistency ASC) as consistency_rank,
        ROW_NUMBER() OVER (ORDER BY low_power_incidents ASC) as reliability_rank,
        ROW_NUMBER() OVER (ORDER BY avg_charging_time ASC) as charging_efficiency_rank,
        
        -- 计算综合评分 (0-100)
        ROUND(
            (100 - (low_power_incidents * 2)) * 0.3 +  -- 可靠性权重30%
            LEAST(100, fuel_efficiency_ratio * 10) * 0.25 +  -- 燃油效率权重25%
            (100 - LEAST(100, speed_consistency)) * 0.2 +  -- 驾驶稳定性权重20%
            LEAST(100, (120 - avg_charging_time) / 120 * 100) * 0.15 +  -- 充电效率权重15%
            LEAST(100, (clean_modes_used + road_modes_used) * 10) * 0.1,  -- 适应性权重10%
            1
        ) as overall_score
        
    FROM vehicle_performance
)

SELECT 
    vin_id,
    total_records,
    
    -- 核心性能指标
    ROUND(avg_fuel_percentage, 1) as avg_fuel_percentage,
    ROUND(avg_cltc_mileage, 1) as avg_cltc_mileage,
    ROUND(avg_wltc_mileage, 1) as avg_wltc_mileage,
    fuel_efficiency_ratio,
    
    -- 驾驶表现
    ROUND(avg_speed, 1) as avg_speed,
    ROUND(speed_consistency, 1) as speed_consistency,
    ROUND(max_speed, 1) as max_speed,
    
    -- 充电表现
    ROUND(avg_charging_time, 1) as avg_charging_time,
    ROUND(avg_target_soc, 1) as avg_target_soc,
    charging_sessions,
    
    -- 增程器使用
    ROUND(avg_extender_usage, 1) as avg_extender_usage,
    high_extender_sessions,
    
    -- 异常统计
    low_power_incidents,
    low_fuel_incidents,
    speeding_incidents,
    
    -- 适应性指标
    clean_modes_used,
    road_modes_used,
    endurance_types_used,
    
    -- 排名和评分
    efficiency_rank,
    consistency_rank,
    reliability_rank,
    charging_efficiency_rank,
    overall_score,
    
    -- 性能等级
    CASE 
        WHEN overall_score >= 90 THEN 'A级 - 优秀'
        WHEN overall_score >= 80 THEN 'B级 - 良好'
        WHEN overall_score >= 70 THEN 'C级 - 一般'
        WHEN overall_score >= 60 THEN 'D级 - 需改进'
        ELSE 'E级 - 需重点关注'
    END as performance_grade,
    
    -- 改进建议
    CASE 
        WHEN low_power_incidents > 10 THEN '建议检查电池系统和充电策略'
        WHEN fuel_efficiency_ratio < 2 THEN '建议优化驾驶习惯，提高燃油效率'
        WHEN speed_consistency > 20 THEN '建议保持稳定驾驶，避免急加速急减速'
        WHEN avg_charging_time > 120 THEN '建议使用快充设施或检查充电系统'
        WHEN high_extender_sessions > total_records * 0.3 THEN '增程器使用频繁，建议检查电池容量'
        ELSE '整体表现良好，继续保持'
    END as improvement_suggestion

FROM performance_rankings
ORDER BY overall_score DESC;
