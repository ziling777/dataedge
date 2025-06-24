# QuickSight 车辆遥测数据看板手动创建指南

## 前置条件
1. 确保 QuickSight 已启用并有适当权限
2. 确保 S3 Tables 数据可通过 Athena 查询

## 步骤1: 创建数据源

1. 登录 AWS QuickSight 控制台
2. 点击 "Manage data" -> "New data set"
3. 选择 "Athena" 作为数据源
4. 配置连接:
   - Data source name: `车辆遥测数据源`
   - Athena workgroup: `primary`
   - 点击 "Create data source"

## 步骤2: 创建数据集

### 数据集1: 车辆概览统计
```sql
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT vin_id) as unique_vehicles,
    ROUND(AVG(fuel_percentage), 2) as avg_fuel_percentage,
    ROUND(AVG(display_speed), 2) as avg_speed,
    SUM(CASE WHEN ress_power_low_flag = true THEN 1 ELSE 0 END) as low_power_alerts,
    ROUND(AVG(fuel_cltc_mileage), 2) as avg_cltc_mileage,
    ROUND(AVG(target_soc), 2) as avg_target_soc
FROM "s3tablescatalog"."greptime"."canbus01"
```

### 数据集2: 驾驶模式分析
```sql
SELECT 
    clean_mode,
    road_mode,
    COUNT(*) as record_count,
    ROUND(AVG(fuel_percentage), 2) as avg_fuel,
    ROUND(AVG(display_speed), 2) as avg_speed,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM "s3tablescatalog"."greptime"."canbus01"
GROUP BY clean_mode, road_mode
ORDER BY record_count DESC
```

### 数据集3: 速度分布分析
```sql
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
    ROUND(AVG(fuel_percentage), 2) as avg_fuel,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM "s3tablescatalog"."greptime"."canbus01"
GROUP BY 
    CASE 
        WHEN display_speed <= 20 THEN '低速(≤20)'
        WHEN display_speed <= 40 THEN '中低速(21-40)'
        WHEN display_speed <= 60 THEN '中速(41-60)'
        WHEN display_speed <= 80 THEN '中高速(61-80)'
        WHEN display_speed <= 100 THEN '高速(81-100)'
        ELSE '超高速(>100)'
    END
ORDER BY record_count DESC
```

### 数据集4: 充电行为分析
```sql
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
FROM "s3tablescatalog"."greptime"."canbus01"
GROUP BY 
    CASE 
        WHEN charging_time_remain_minute = 0 THEN '无需充电'
        WHEN charging_time_remain_minute <= 30 THEN '快速充电(≤30分钟)'
        WHEN charging_time_remain_minute <= 120 THEN '标准充电(31-120分钟)'
        ELSE '长时间充电(>120分钟)'
    END
ORDER BY session_count DESC
```

### 数据集5: 燃油效率分析
```sql
SELECT 
    CASE 
        WHEN fuel_percentage <= 20 THEN '低油量(≤20%)'
        WHEN fuel_percentage <= 40 THEN '中低油量(21-40%)'
        WHEN fuel_percentage <= 60 THEN '中等油量(41-60%)'
        WHEN fuel_percentage <= 80 THEN '中高油量(61-80%)'
        ELSE '高油量(>80%)'
    END as fuel_level,
    COUNT(*) as record_count,
    ROUND(AVG(fuel_cltc_mileage), 2) as avg_cltc_mileage,
    ROUND(AVG(fuel_wltc_mileage), 2) as avg_wltc_mileage,
    ROUND(AVG(fuel_cltc_mileage) / NULLIF(AVG(fuel_percentage), 0), 3) as efficiency_ratio
FROM "s3tablescatalog"."greptime"."canbus01"
GROUP BY 
    CASE 
        WHEN fuel_percentage <= 20 THEN '低油量(≤20%)'
        WHEN fuel_percentage <= 40 THEN '中低油量(21-40%)'
        WHEN fuel_percentage <= 60 THEN '中等油量(41-60%)'
        WHEN fuel_percentage <= 80 THEN '中高油量(61-80%)'
        ELSE '高油量(>80%)'
    END
ORDER BY record_count DESC
```

## 步骤3: 创建可视化图表

### 建议的看板布局

#### 第一行 - KPI 指标卡片
1. **车辆总数** (KPI)
   - 数据集: 车辆概览统计
   - 字段: unique_vehicles

2. **平均燃油百分比** (KPI)
   - 数据集: 车辆概览统计
   - 字段: avg_fuel_percentage

3. **平均速度** (KPI)
   - 数据集: 车辆概览统计
   - 字段: avg_speed

4. **低电量警告** (KPI)
   - 数据集: 车辆概览统计
   - 字段: low_power_alerts

#### 第二行 - 分布分析
1. **驾驶模式分布** (条形图)
   - 数据集: 驾驶模式分析
   - X轴: clean_mode
   - Y轴: record_count
   - 颜色: road_mode

2. **速度分布** (饼图)
   - 数据集: 速度分布分析
   - 类别: speed_category
   - 值: record_count

#### 第三行 - 行为分析
1. **充电行为分布** (环形图)
   - 数据集: 充电行为分析
   - 类别: charging_category
   - 值: session_count

2. **燃油效率对比** (堆叠条形图)
   - 数据集: 燃油效率分析
   - X轴: fuel_level
   - Y轴: avg_cltc_mileage, avg_wltc_mileage

## 步骤4: 配置筛选器

1. **时间范围筛选器**
   - 类型: 日期范围
   - 默认: 最近24小时

2. **车辆ID筛选器**
   - 类型: 下拉列表
   - 允许多选

3. **驾驶模式筛选器**
   - 类型: 下拉列表
   - 允许多选

## 步骤5: 发布仪表板

1. 点击 "Publish" 按钮
2. 设置仪表板名称: "车辆遥测数据分析看板"
3. 配置权限和共享设置
4. 设置自动刷新频率 (建议每小时)

## 预期效果

创建完成后，你将获得一个包含以下功能的交互式看板:

- **实时监控**: 车辆总数、平均燃油、速度等关键指标
- **驾驶行为分析**: 不同驾驶模式的使用情况和效果
- **速度分布**: 车辆速度范围分布，识别驾驶习惯
- **充电模式**: 充电时间分布，优化充电策略
- **燃油效率**: 不同油量水平下的续航表现
- **交互筛选**: 按时间、车辆、模式等维度筛选数据

## 注意事项

1. 确保 Athena 查询权限正确配置
2. 如果查询失败，检查 S3 Tables 的 Glue 联邦目录配置
3. 建议设置合理的查询超时时间
4. 考虑数据量大小，适当使用采样或时间范围限制
5. 定期检查和优化查询性能

## 故障排除

如果遇到 "Schema 'greptime' does not exist" 错误:
1. 检查 Glue 联邦目录是否正确配置
2. 确认 S3 Tables 的命名空间和表名
3. 验证 Athena 工作组权限
4. 尝试直接在 Athena 控制台测试查询
