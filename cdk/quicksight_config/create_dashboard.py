#!/usr/bin/env python3
"""
自动创建 QuickSight 车辆遥测数据看板
"""

import boto3
import json
import time
from datetime import datetime

# AWS 配置
AWS_REGION = 'us-east-1'
AWS_ACCOUNT_ID = '773203029824'
QUICKSIGHT_USER_ARN = f'arn:aws:quicksight:{AWS_REGION}:{AWS_ACCOUNT_ID}:user/default/WSParticipantRole/Participant'

# QuickSight 客户端
quicksight = boto3.client('quicksight', region_name=AWS_REGION)

def create_data_source():
    """创建 Athena 数据源"""
    try:
        response = quicksight.create_data_source(
            AwsAccountId=AWS_ACCOUNT_ID,
            DataSourceId='vehicle-telematics-athena',
            Name='车辆遥测数据源',
            Type='ATHENA',
            DataSourceParameters={
                'AthenaParameters': {
                    'WorkGroup': 'primary'
                }
            },
            Permissions=[
                {
                    'Principal': QUICKSIGHT_USER_ARN,
                    'Actions': [
                        'quicksight:UpdateDataSourcePermissions',
                        'quicksight:DescribeDataSource',
                        'quicksight:DescribeDataSourcePermissions',
                        'quicksight:PassDataSource',
                        'quicksight:UpdateDataSource',
                        'quicksight:DeleteDataSource'
                    ]
                }
            ]
        )
        print(f"✅ 数据源创建成功: {response['DataSourceId']}")
        return response['DataSourceId']
    except Exception as e:
        print(f"❌ 数据源创建失败: {str(e)}")
        return None

def create_datasets():
    """创建多个数据集"""
    datasets = [
        {
            'id': 'vehicle-overview',
            'name': '车辆概览统计',
            'sql': '''
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT vin_id) as unique_vehicles,
                    ROUND(AVG(fuel_percentage), 2) as avg_fuel_percentage,
                    ROUND(AVG(display_speed), 2) as avg_speed,
                    SUM(CASE WHEN ress_power_low_flag = true THEN 1 ELSE 0 END) as low_power_alerts,
                    ROUND(AVG(fuel_cltc_mileage), 2) as avg_cltc_mileage,
                    ROUND(AVG(target_soc), 2) as avg_target_soc
                FROM "s3tablescatalog"."greptime"."canbus01"
            '''
        },
        {
            'id': 'driving-modes',
            'name': '驾驶模式分析',
            'sql': '''
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
            '''
        },
        {
            'id': 'speed-distribution',
            'name': '速度分布',
            'sql': '''
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
            '''
        },
        {
            'id': 'charging-analysis',
            'name': '充电行为分析',
            'sql': '''
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
            '''
        },
        {
            'id': 'fuel-efficiency',
            'name': '燃油效率分析',
            'sql': '''
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
            '''
        }
    ]
    
    created_datasets = []
    
    for dataset_config in datasets:
        try:
            response = quicksight.create_data_set(
                AwsAccountId=AWS_ACCOUNT_ID,
                DataSetId=dataset_config['id'],
                Name=dataset_config['name'],
                ImportMode='DIRECT_QUERY',
                PhysicalTableMap={
                    'query-table': {
                        'CustomSql': {
                            'DataSourceArn': f'arn:aws:quicksight:{AWS_REGION}:{AWS_ACCOUNT_ID}:datasource/vehicle-telematics-athena',
                            'Name': dataset_config['name'],
                            'SqlQuery': dataset_config['sql'],
                            'Columns': [
                                {'Name': 'metric', 'Type': 'STRING'},
                                {'Name': 'value', 'Type': 'DECIMAL'}
                            ]
                        }
                    }
                }
            )
            print(f"✅ 数据集创建成功: {dataset_config['name']}")
            created_datasets.append(dataset_config['id'])
            time.sleep(2)  # 避免API限制
            
        except Exception as e:
            print(f"❌ 数据集创建失败 {dataset_config['name']}: {str(e)}")
    
    return created_datasets

def create_analysis(dataset_ids):
    """创建分析看板"""
    try:
        # 构建数据集声明
        dataset_declarations = []
        for i, dataset_id in enumerate(dataset_ids):
            dataset_declarations.append({
                'DataSetArn': f'arn:aws:quicksight:{AWS_REGION}:{AWS_ACCOUNT_ID}:dataset/{dataset_id}',
                'Identifier': f'dataset-{i}'
            })
        
        # 创建基础的可视化配置
        visuals = [
            {
                'KPIVisual': {
                    'VisualId': 'kpi-total-vehicles',
                    'Title': {
                        'Visibility': 'VISIBLE',
                        'FormatText': {
                            'PlainText': '车辆总数'
                        }
                    },
                    'ChartConfiguration': {
                        'FieldWells': {
                            'Values': [
                                {
                                    'NumericalMeasureField': {
                                        'FieldId': 'unique-vehicles',
                                        'Column': {
                                            'DataSetIdentifier': 'dataset-0',
                                            'ColumnName': 'unique_vehicles'
                                        },
                                        'AggregationFunction': {
                                            'SimpleNumericalAggregation': 'SUM'
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        ]
        
        response = quicksight.create_analysis(
            AwsAccountId=AWS_ACCOUNT_ID,
            AnalysisId='vehicle-telematics-analysis',
            Name='车辆遥测数据分析',
            Definition={
                'DataSetIdentifierDeclarations': dataset_declarations,
                'Sheets': [
                    {
                        'SheetId': 'overview-sheet',
                        'Name': '概览面板',
                        'Visuals': visuals
                    }
                ]
            },
            Permissions=[
                {
                    'Principal': QUICKSIGHT_USER_ARN,
                    'Actions': [
                        'quicksight:RestoreAnalysis',
                        'quicksight:UpdateAnalysisPermissions',
                        'quicksight:DeleteAnalysis',
                        'quicksight:QueryAnalysis',
                        'quicksight:DescribeAnalysis',
                        'quicksight:UpdateAnalysis'
                    ]
                }
            ]
        )
        
        print(f"✅ 分析看板创建成功: {response['AnalysisId']}")
        return response['AnalysisId']
        
    except Exception as e:
        print(f"❌ 分析看板创建失败: {str(e)}")
        return None

def create_dashboard(analysis_id):
    """从分析创建仪表板"""
    try:
        response = quicksight.create_dashboard(
            AwsAccountId=AWS_ACCOUNT_ID,
            DashboardId='vehicle-telematics-dashboard',
            Name='车辆遥测数据看板',
            SourceEntity={
                'SourceTemplate': {
                    'DataSetReferences': [],
                    'Arn': f'arn:aws:quicksight:{AWS_REGION}:{AWS_ACCOUNT_ID}:analysis/{analysis_id}'
                }
            },
            Permissions=[
                {
                    'Principal': QUICKSIGHT_USER_ARN,
                    'Actions': [
                        'quicksight:DescribeDashboard',
                        'quicksight:ListDashboardVersions',
                        'quicksight:UpdateDashboardPermissions',
                        'quicksight:QueryDashboard',
                        'quicksight:UpdateDashboard',
                        'quicksight:DeleteDashboard'
                    ]
                }
            ]
        )
        
        print(f"✅ 仪表板创建成功: {response['DashboardId']}")
        print(f"🔗 仪表板URL: https://{AWS_REGION}.quicksight.aws.amazon.com/sn/dashboards/{response['DashboardId']}")
        return response['DashboardId']
        
    except Exception as e:
        print(f"❌ 仪表板创建失败: {str(e)}")
        return None

def main():
    """主函数"""
    print("🚀 开始创建 QuickSight 车辆遥测数据看板...")
    
    # 步骤1: 创建数据源
    print("\n📊 步骤1: 创建数据源")
    data_source_id = create_data_source()
    if not data_source_id:
        print("❌ 数据源创建失败，退出")
        return
    
    time.sleep(5)  # 等待数据源创建完成
    
    # 步骤2: 创建数据集
    print("\n📈 步骤2: 创建数据集")
    dataset_ids = create_datasets()
    if not dataset_ids:
        print("❌ 数据集创建失败，退出")
        return
    
    time.sleep(10)  # 等待数据集创建完成
    
    # 步骤3: 创建分析
    print("\n🔍 步骤3: 创建分析")
    analysis_id = create_analysis(dataset_ids)
    if not analysis_id:
        print("❌ 分析创建失败，退出")
        return
    
    time.sleep(10)  # 等待分析创建完成
    
    # 步骤4: 创建仪表板
    print("\n📋 步骤4: 创建仪表板")
    dashboard_id = create_dashboard(analysis_id)
    
    if dashboard_id:
        print(f"\n🎉 QuickSight 看板创建完成!")
        print(f"📊 数据源ID: {data_source_id}")
        print(f"📈 数据集数量: {len(dataset_ids)}")
        print(f"🔍 分析ID: {analysis_id}")
        print(f"📋 仪表板ID: {dashboard_id}")
        print(f"\n🔗 访问链接: https://{AWS_REGION}.quicksight.aws.amazon.com/sn/dashboards/{dashboard_id}")
    else:
        print("❌ 看板创建失败")

if __name__ == "__main__":
    main()
