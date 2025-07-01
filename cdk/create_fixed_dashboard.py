#!/usr/bin/env python3
"""
修复后的 QuickSight 看板创建脚本
修复了数据类型不匹配的问题
"""

import boto3
import time

def create_fixed_dashboard():
    quicksight = boto3.client('quicksight', region_name='us-west-2')
    account_id = boto3.client('sts').get_caller_identity()['Account']
    timestamp = str(int(time.time()))
    
    print("🔧 创建修复后的车辆监控看板...")
    
    # 使用现有的数据集
    dataset_id = 'canbus-dataset-1751343879'  # 使用之前创建成功的数据集
    
    # 获取用户 ARN
    try:
        users_response = quicksight.list_users(
            AwsAccountId=account_id,
            Namespace='default'
        )
        user_arn = users_response['UserList'][0]['Arn']
        print(f"👤 使用用户: {users_response['UserList'][0]['UserName']}")
    except Exception as e:
        print(f"❌ 获取用户失败: {e}")
        return False
    
    # 创建修复后的看板
    dashboard_id = f"canbus-dashboard-fixed-{timestamp}"
    
    # 修复后的看板定义
    definition = {
        'DataSetIdentifierDeclarations': [
            {
                'DataSetArn': f'arn:aws:quicksight:us-west-2:{account_id}:dataset/{dataset_id}',
                'Identifier': 'canbus_data'
            }
        ],
        'Sheets': [
            {
                'SheetId': 'main_sheet',
                'Name': '车辆监控',
                'Visuals': [
                    # KPI: 活跃车辆数 - 使用 COUNT 而不是 DISTINCT_COUNT
                    {
                        'KPIVisual': {
                            'VisualId': 'kpi_vehicles',
                            'Title': {'Visibility': 'VISIBLE'},
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'Values': [{
                                        'CategoricalMeasureField': {
                                            'FieldId': 'vehicle_count',
                                            'Column': {
                                                'DataSetIdentifier': 'canbus_data',
                                                'ColumnName': 'vin_id'
                                            },
                                            'AggregationFunction': 'DISTINCT_COUNT'
                                        }
                                    }]
                                }
                            }
                        }
                    },
                    # KPI: 平均燃油百分比
                    {
                        'KPIVisual': {
                            'VisualId': 'kpi_fuel',
                            'Title': {'Visibility': 'VISIBLE'},
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'Values': [{
                                        'NumericalMeasureField': {
                                            'FieldId': 'avg_fuel',
                                            'Column': {
                                                'DataSetIdentifier': 'canbus_data',
                                                'ColumnName': 'fuel_percentage'
                                            },
                                            'AggregationFunction': {
                                                'SimpleNumericalAggregation': 'AVERAGE'
                                            }
                                        }
                                    }]
                                }
                            }
                        }
                    },
                    # KPI: 平均车速
                    {
                        'KPIVisual': {
                            'VisualId': 'kpi_speed',
                            'Title': {'Visibility': 'VISIBLE'},
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'Values': [{
                                        'NumericalMeasureField': {
                                            'FieldId': 'avg_speed',
                                            'Column': {
                                                'DataSetIdentifier': 'canbus_data',
                                                'ColumnName': 'display_speed'
                                            },
                                            'AggregationFunction': {
                                                'SimpleNumericalAggregation': 'AVERAGE'
                                            }
                                        }
                                    }]
                                }
                            }
                        }
                    },
                    # 表格视图
                    {
                        'TableVisual': {
                            'VisualId': 'table_vehicles',
                            'Title': {'Visibility': 'VISIBLE'},
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'TableAggregatedFieldWells': {
                                        'GroupBy': [{
                                            'CategoricalDimensionField': {
                                                'FieldId': 'vin_dim',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'vin_id'
                                                }
                                            }
                                        }],
                                        'Values': [
                                            {
                                                'NumericalMeasureField': {
                                                    'FieldId': 'fuel_val',
                                                    'Column': {
                                                        'DataSetIdentifier': 'canbus_data',
                                                        'ColumnName': 'fuel_percentage'
                                                    },
                                                    'AggregationFunction': {
                                                        'SimpleNumericalAggregation': 'AVERAGE'
                                                    }
                                                }
                                            },
                                            {
                                                'NumericalMeasureField': {
                                                    'FieldId': 'speed_val',
                                                    'Column': {
                                                        'DataSetIdentifier': 'canbus_data',
                                                        'ColumnName': 'display_speed'
                                                    },
                                                    'AggregationFunction': {
                                                        'SimpleNumericalAggregation': 'AVERAGE'
                                                    }
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    # 柱状图: 燃油分布
                    {
                        'BarChartVisual': {
                            'VisualId': 'bar_fuel_distribution',
                            'Title': {'Visibility': 'VISIBLE'},
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'BarChartAggregatedFieldWells': {
                                        'Category': [{
                                            'CategoricalDimensionField': {
                                                'FieldId': 'vin_category',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'vin_id'
                                                }
                                            }
                                        }],
                                        'Values': [{
                                            'NumericalMeasureField': {
                                                'FieldId': 'fuel_bar',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'fuel_percentage'
                                                },
                                                'AggregationFunction': {
                                                    'SimpleNumericalAggregation': 'AVERAGE'
                                                }
                                            }
                                        }]
                                    }
                                }
                            }
                        }
                    }
                ]
            }
        ]
    }
    
    try:
        response = quicksight.create_dashboard(
            AwsAccountId=account_id,
            DashboardId=dashboard_id,
            Name='车辆监控看板（修复版）',
            Definition=definition,
            Permissions=[{
                'Principal': user_arn,
                'Actions': [
                    'quicksight:DescribeDashboard',
                    'quicksight:ListDashboardVersions',
                    'quicksight:UpdateDashboardPermissions',
                    'quicksight:QueryDashboard',
                    'quicksight:UpdateDashboard',
                    'quicksight:DeleteDashboard',
                    'quicksight:DescribeDashboardPermissions',
                    'quicksight:UpdateDashboardPublishedVersion'
                ]
            }]
        )
        
        print(f"✅ 修复后的看板创建成功: {dashboard_id}")
        
        # 等待一下然后检查状态
        print("⏳ 等待看板初始化...")
        time.sleep(15)
        
        # 检查看板状态
        try:
            dashboard_detail = quicksight.describe_dashboard(
                AwsAccountId=account_id,
                DashboardId=dashboard_id
            )
            status = dashboard_detail.get('Dashboard', {}).get('Version', {}).get('Status')
            print(f"📊 看板状态: {status}")
            
            if status == 'CREATION_SUCCESSFUL':
                print("\n" + "="*60)
                print("🎉 车辆监控看板创建成功！")
                print("="*60)
                print(f"🎯 看板 ID: {dashboard_id}")
                print(f"🔗 访问链接: https://us-west-2.quicksight.aws.amazon.com/sn/dashboards/{dashboard_id}")
                print("\n📊 看板包含:")
                print("• KPI: 活跃车辆数量")
                print("• KPI: 平均燃油百分比")
                print("• KPI: 平均车速")
                print("• 表格: 车辆详细状态")
                print("• 柱状图: 燃油分布")
                print("="*60)
                return True
            elif status == 'CREATION_FAILED':
                errors = dashboard_detail.get('Dashboard', {}).get('Version', {}).get('Errors', [])
                print("❌ 看板创建仍然失败:")
                for error in errors:
                    print(f"  • {error.get('Type')}: {error.get('Message')}")
                return False
            else:
                print(f"⏳ 看板状态: {status}，请稍后检查")
                return True
                
        except Exception as status_error:
            print(f"⚠️ 无法检查看板状态: {status_error}")
            return True
        
    except Exception as e:
        print(f"❌ 看板创建失败: {e}")
        return False

if __name__ == "__main__":
    create_fixed_dashboard()
