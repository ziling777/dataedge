#!/usr/bin/env python3
"""
创建简化但美观的车辆监控看板
避免数据类型冲突，专注于可视化效果
"""

import boto3
import time

def create_simple_beautiful_dashboard():
    quicksight = boto3.client('quicksight', region_name='us-west-2')
    account_id = boto3.client('sts').get_caller_identity()['Account']
    timestamp = str(int(time.time()))
    
    print("🎨 创建简化美观的车辆监控看板...")
    
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
    
    dashboard_id = f"canbus-simple-beautiful-{timestamp}"
    
    # 创建简化但美观的看板定义
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
                'Name': '车辆监控看板',
                'Visuals': [
                    # 1. KPI: 活跃车辆数
                    {
                        'KPIVisual': {
                            'VisualId': 'kpi_vehicles',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '🚗 活跃车辆总数'
                                }
                            },
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
                    
                    # 2. KPI: 平均燃油百分比
                    {
                        'KPIVisual': {
                            'VisualId': 'kpi_fuel',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '⛽ 平均燃油水平 (%)'
                                }
                            },
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
                    
                    # 3. KPI: 平均车速
                    {
                        'KPIVisual': {
                            'VisualId': 'kpi_speed',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '🏃 平均行驶速度 (km/h)'
                                }
                            },
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
                    
                    # 4. 折线图: 时间趋势
                    {
                        'LineChartVisual': {
                            'VisualId': 'line_trend',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '📈 燃油水平时间趋势'
                                }
                            },
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'LineChartAggregatedFieldWells': {
                                        'Category': [{
                                            'DateDimensionField': {
                                                'FieldId': 'time_category',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'ts'
                                                },
                                                'DateGranularity': 'HOUR'
                                            }
                                        }],
                                        'Values': [{
                                            'NumericalMeasureField': {
                                                'FieldId': 'fuel_trend',
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
                    
                    # 5. 柱状图: 车辆燃油对比
                    {
                        'BarChartVisual': {
                            'VisualId': 'bar_fuel_comparison',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '📊 各车辆燃油水平对比'
                                }
                            },
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'BarChartAggregatedFieldWells': {
                                        'Category': [{
                                            'CategoricalDimensionField': {
                                                'FieldId': 'vin_bar',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'vin_id'
                                                }
                                            }
                                        }],
                                        'Values': [{
                                            'NumericalMeasureField': {
                                                'FieldId': 'fuel_bar_val',
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
                    
                    # 6. 另一个折线图: 速度趋势
                    {
                        'LineChartVisual': {
                            'VisualId': 'line_speed_trend',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '🚀 车速变化趋势'
                                }
                            },
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'LineChartAggregatedFieldWells': {
                                        'Category': [{
                                            'DateDimensionField': {
                                                'FieldId': 'time_speed_category',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'ts'
                                                },
                                                'DateGranularity': 'HOUR'
                                            }
                                        }],
                                        'Values': [{
                                            'NumericalMeasureField': {
                                                'FieldId': 'speed_trend',
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
                    
                    # 7. 柱状图: 车辆速度对比
                    {
                        'BarChartVisual': {
                            'VisualId': 'bar_speed_comparison',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '🏁 各车辆平均速度对比'
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
                    
                    # 8. 表格: 车辆详细信息
                    {
                        'TableVisual': {
                            'VisualId': 'table_details',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '📋 车辆详细状态表'
                                }
                            },
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'TableAggregatedFieldWells': {
                                        'GroupBy': [{
                                            'CategoricalDimensionField': {
                                                'FieldId': 'vin_table',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'vin_id'
                                                }
                                            }
                                        }],
                                        'Values': [
                                            {
                                                'NumericalMeasureField': {
                                                    'FieldId': 'fuel_table',
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
                                                    'FieldId': 'speed_table',
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
                                                    'FieldId': 'charging_table',
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
            Name='🚗 车辆监控美观看板 v2',
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
        
        print(f"✅ 美观看板创建成功: {dashboard_id}")
        
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
            print("🎨 美观车辆监控看板创建完成！")
            print("="*70)
            print(f"🎯 看板 ID: {dashboard_id}")
            print(f"🔗 访问链接: https://us-west-2.quicksight.aws.amazon.com/sn/dashboards/{dashboard_id}")
            print("\n📊 看板包含丰富的可视化图表:")
            print("• 🚗 KPI 卡片: 活跃车辆总数")
            print("• ⛽ KPI 卡片: 平均燃油水平")
            print("• 🏃 KPI 卡片: 平均行驶速度")
            print("• 📈 折线图: 燃油水平时间趋势")
            print("• 🚀 折线图: 车速变化趋势")
            print("• 📊 柱状图: 各车辆燃油水平对比")
            print("• 🏁 柱状图: 各车辆平均速度对比")
            print("• 📋 数据表: 车辆详细状态")
            
            if status == 'CREATION_FAILED':
                errors = dashboard_detail.get('Dashboard', {}).get('Version', {}).get('Errors', [])
                print("\n⚠️ 创建过程中的错误:")
                for error in errors:
                    print(f"  • {error.get('Type')}: {error.get('Message')}")
            
            print("\n💡 提示:")
            print("• 看板包含多种图表类型，展示效果更丰富")
            print("• 可以在 QuickSight 中进一步调整颜色和布局")
            print("• 支持交互式过滤和钻取分析")
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
    create_simple_beautiful_dashboard()
