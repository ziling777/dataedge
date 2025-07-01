#!/usr/bin/env python3
"""
创建基于实际数据字段的电动车监控看板
使用实际存在的字段：fuel_percentage, charging_time_remain_minute, display_speed 等
"""

import boto3
import time

def create_realistic_ev_dashboard():
    quicksight = boto3.client('quicksight', region_name='us-west-2')
    account_id = boto3.client('sts').get_caller_identity()['Account']
    timestamp = str(int(time.time()))
    
    print("🔋 创建基于实际数据的电动车监控看板...")
    
    # 使用现有的数据集
    dataset_id = 'canbus-dataset-1751343879'
    
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
    
    dashboard_id = f"realistic-ev-dashboard-{timestamp}"
    
    # 基于实际数据字段的电动车看板定义
    definition = {
        'DataSetIdentifierDeclarations': [
            {
                'DataSetArn': f'arn:aws:quicksight:us-west-2:{account_id}:dataset/{dataset_id}',
                'Identifier': 'canbus_data'
            }
        ],
        'Sheets': [
            {
                'SheetId': 'realistic_ev_sheet',
                'Name': '电动车实时监控',
                'Visuals': [
                    # 1. KPI: 在线电动车数量
                    {
                        'KPIVisual': {
                            'VisualId': 'kpi_ev_count',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '🚗 在线电动车数量'
                                }
                            },
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'Values': [{
                                        'CategoricalMeasureField': {
                                            'FieldId': 'ev_count',
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
                    
                    # 2. KPI: 平均电池电量 (使用 fuel_percentage 作为电池电量)
                    {
                        'KPIVisual': {
                            'VisualId': 'kpi_battery_level',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '🔋 平均电池电量 (%)'
                                }
                            },
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'Values': [{
                                        'NumericalMeasureField': {
                                            'FieldId': 'avg_battery',
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
                    
                    # 3. KPI: 平均行驶速度
                    {
                        'KPIVisual': {
                            'VisualId': 'kpi_ev_speed',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '⚡ 平均行驶速度 (km/h)'
                                }
                            },
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'Values': [{
                                        'NumericalMeasureField': {
                                            'FieldId': 'avg_ev_speed',
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
                    
                    # 4. KPI: 平均充电剩余时间
                    {
                        'KPIVisual': {
                            'VisualId': 'kpi_charging_time',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '⏰ 平均充电剩余时间 (分钟)'
                                }
                            },
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'Values': [{
                                        'NumericalMeasureField': {
                                            'FieldId': 'avg_charging_time',
                                            'Column': {
                                                'DataSetIdentifier': 'canbus_data',
                                                'ColumnName': 'charging_time_remain_minute'
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
                    
                    # 5. 折线图: 电池电量时间趋势
                    {
                        'LineChartVisual': {
                            'VisualId': 'line_battery_trend',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '🔋 电池电量变化趋势'
                                }
                            },
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'LineChartAggregatedFieldWells': {
                                        'Category': [{
                                            'DateDimensionField': {
                                                'FieldId': 'time_battery_category',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'ts'
                                                },
                                                'DateGranularity': 'HOUR'
                                            }
                                        }],
                                        'Values': [{
                                            'NumericalMeasureField': {
                                                'FieldId': 'battery_trend',
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
                    },
                    
                    # 6. 折线图: 充电时间趋势
                    {
                        'LineChartVisual': {
                            'VisualId': 'line_charging_trend',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '⚡ 充电时间变化趋势'
                                }
                            },
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'LineChartAggregatedFieldWells': {
                                        'Category': [{
                                            'DateDimensionField': {
                                                'FieldId': 'time_charging_category',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'ts'
                                                },
                                                'DateGranularity': 'HOUR'
                                            }
                                        }],
                                        'Values': [{
                                            'NumericalMeasureField': {
                                                'FieldId': 'charging_trend',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'charging_time_remain_minute'
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
                    },
                    
                    # 7. 柱状图: 各电动车电池电量对比
                    {
                        'BarChartVisual': {
                            'VisualId': 'bar_battery_comparison',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '🔋 各电动车电池电量对比'
                                }
                            },
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'BarChartAggregatedFieldWells': {
                                        'Category': [{
                                            'CategoricalDimensionField': {
                                                'FieldId': 'vin_battery_bar',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'vin_id'
                                                }
                                            }
                                        }],
                                        'Values': [{
                                            'NumericalMeasureField': {
                                                'FieldId': 'battery_bar_val',
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
                    },
                    
                    # 8. 柱状图: 各电动车速度对比
                    {
                        'BarChartVisual': {
                            'VisualId': 'bar_speed_comparison',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '⚡ 各电动车速度对比'
                                }
                            },
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'BarChartAggregatedFieldWells': {
                                        'Category': [{
                                            'CategoricalDimensionField': {
                                                'FieldId': 'vin_speed_bar',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'vin_id'
                                                }
                                            }
                                        }],
                                        'Values': [{
                                            'NumericalMeasureField': {
                                                'FieldId': 'speed_bar_val',
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
                        }
                    },
                    
                    # 9. 表格: 电动车详细状态
                    {
                        'TableVisual': {
                            'VisualId': 'table_ev_details',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '📊 电动车详细状态表'
                                }
                            },
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'TableAggregatedFieldWells': {
                                        'GroupBy': [{
                                            'CategoricalDimensionField': {
                                                'FieldId': 'vin_ev_table',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'vin_id'
                                                }
                                            }
                                        }],
                                        'Values': [
                                            {
                                                'NumericalMeasureField': {
                                                    'FieldId': 'battery_table',
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
                                                    'FieldId': 'speed_ev_table',
                                                    'Column': {
                                                        'DataSetIdentifier': 'canbus_data',
                                                        'ColumnName': 'display_speed'
                                                    },
                                                    'AggregationFunction': {
                                                        'SimpleNumericalAggregation': 'AVERAGE'
                                                    }
                                                }
                                            },
                                            {
                                                'NumericalMeasureField': {
                                                    'FieldId': 'charging_ev_table',
                                                    'Column': {
                                                        'DataSetIdentifier': 'canbus_data',
                                                        'ColumnName': 'charging_time_remain_minute'
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
                    }
                ]
            }
        ]
    }
    
    try:
        response = quicksight.create_dashboard(
            AwsAccountId=account_id,
            DashboardId=dashboard_id,
            Name='🔋 电动车实时监控看板',
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
        
        print(f"✅ 电动车实时监控看板创建成功: {dashboard_id}")
        
        # 等待初始化
        print("⏳ 等待看板初始化...")
        time.sleep(20)
        
        # 检查状态
        try:
            dashboard_detail = quicksight.describe_dashboard(
                AwsAccountId=account_id,
                DashboardId=dashboard_id
            )
            status = dashboard_detail.get('Dashboard', {}).get('Version', {}).get('Status')
            print(f"📊 看板状态: {status}")
            
            print("\n" + "="*70)
            print("🔋 电动车实时监控看板创建完成！")
            print("="*70)
            print(f"🎯 看板 ID: {dashboard_id}")
            print(f"🔗 访问链接: https://us-west-2.quicksight.aws.amazon.com/sn/dashboards/{dashboard_id}")
            print("\n📊 电动车专用可视化图表:")
            print("• 🚗 KPI: 在线电动车数量")
            print("• 🔋 KPI: 平均电池电量 (%)")
            print("• ⚡ KPI: 平均行驶速度")
            print("• ⏰ KPI: 平均充电剩余时间")
            print("• 🔋 折线图: 电池电量变化趋势")
            print("• ⚡ 折线图: 充电时间变化趋势")
            print("• 🔋 柱状图: 各电动车电池电量对比")
            print("• ⚡ 柱状图: 各电动车速度对比")
            print("• 📊 数据表: 电动车详细状态")
            
            if status == 'CREATION_FAILED':
                errors = dashboard_detail.get('Dashboard', {}).get('Version', {}).get('Errors', [])
                print("\n⚠️ 创建过程中的错误:")
                for error in errors:
                    print(f"  • {error.get('Type')}: {error.get('Message')}")
            
            print("\n🔋 电动车监控特色:")
            print("• 电池电量实时监控和历史趋势")
            print("• 充电状态和剩余时间跟踪")
            print("• 电动车性能指标对比分析")
            print("• 多车辆充电进度同时监控")
            print("• 基于实际 CAN 总线数据的准确显示")
            print("="*70)
            return True
                
        except Exception as status_error:
            print(f"⚠️ 无法检查看板状态: {status_error}")
            print(f"🔗 请直接访问: https://us-west-2.quicksight.aws.amazon.com/sn/dashboards/{dashboard_id}")
            return True
        
    except Exception as e:
        print(f"❌ 看板创建失败: {e}")
        return False

if __name__ == "__main__":
    create_realistic_ev_dashboard()
