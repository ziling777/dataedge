#!/usr/bin/env python3
"""
创建美观的车辆监控看板
包含多种图表类型：折线图、饼图、散点图、热力图等
"""

import boto3
import time

def create_beautiful_dashboard():
    quicksight = boto3.client('quicksight', region_name='us-west-2')
    account_id = boto3.client('sts').get_caller_identity()['Account']
    timestamp = str(int(time.time()))
    
    print("🎨 创建美观的车辆监控看板...")
    
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
    
    dashboard_id = f"canbus-beautiful-{timestamp}"
    
    # 创建包含多种图表的美观看板
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
                                    'PlainText': '🚗 活跃车辆数'
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
                                    'PlainText': '⛽ 平均燃油百分比'
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
                                    'PlainText': '🏃 平均车速 (km/h)'
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
                                    'PlainText': '📈 燃油和速度趋势'
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
                                        'Values': [
                                            {
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
                                            },
                                            {
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
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    },
                    
                    # 5. 饼图: 驾驶模式分布
                    {
                        'PieChartVisual': {
                            'VisualId': 'pie_modes',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '🎯 驾驶模式分布'
                                }
                            },
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'PieChartAggregatedFieldWells': {
                                        'Category': [{
                                            'CategoricalDimensionField': {
                                                'FieldId': 'clean_mode_dim',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'clean_mode'
                                                }
                                            }
                                        }],
                                        'Values': [{
                                            'CategoricalMeasureField': {
                                                'FieldId': 'mode_count',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'vin_id'
                                                },
                                                'AggregationFunction': 'COUNT'
                                            }
                                        }]
                                    }
                                }
                            }
                        }
                    },
                    
                    # 6. 散点图: 速度 vs 燃油
                    {
                        'ScatterPlotVisual': {
                            'VisualId': 'scatter_speed_fuel',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '💨 速度与燃油关系'
                                }
                            },
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'ScatterPlotCategoricallyAggregatedFieldWells': {
                                        'XAxis': [{
                                            'NumericalMeasureField': {
                                                'FieldId': 'speed_x',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'display_speed'
                                                },
                                                'AggregationFunction': {
                                                    'SimpleNumericalAggregation': 'AVERAGE'
                                                }
                                            }
                                        }],
                                        'YAxis': [{
                                            'NumericalMeasureField': {
                                                'FieldId': 'fuel_y',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'fuel_percentage'
                                                },
                                                'AggregationFunction': {
                                                    'SimpleNumericalAggregation': 'AVERAGE'
                                                }
                                            }
                                        }],
                                        'Category': [{
                                            'CategoricalDimensionField': {
                                                'FieldId': 'vin_scatter',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'vin_id'
                                                }
                                            }
                                        }]
                                    }
                                }
                            }
                        }
                    },
                    
                    # 7. 柱状图: 车辆燃油对比
                    {
                        'BarChartVisual': {
                            'VisualId': 'bar_fuel_comparison',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '📊 车辆燃油对比'
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
                    
                    # 8. 仪表盘: 平均充电时间
                    {
                        'GaugeChartVisual': {
                            'VisualId': 'gauge_charging',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '🔋 平均充电时间'
                                }
                            },
                            'ChartConfiguration': {
                                'FieldWells': {
                                    'Values': [{
                                        'NumericalMeasureField': {
                                            'FieldId': 'charging_time',
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
                    
                    # 9. 表格: 车辆详细信息
                    {
                        'TableVisual': {
                            'VisualId': 'table_details',
                            'Title': {
                                'Visibility': 'VISIBLE',
                                'FormatText': {
                                    'PlainText': '📋 车辆详细状态'
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
            Name='🚗 车辆监控美观看板',
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
            
            if status == 'CREATION_SUCCESSFUL':
                print("\n" + "="*70)
                print("🎨 美观车辆监控看板创建成功！")
                print("="*70)
                print(f"🎯 看板 ID: {dashboard_id}")
                print(f"🔗 访问链接: https://us-west-2.quicksight.aws.amazon.com/sn/dashboards/{dashboard_id}")
                print("\n📊 看板包含丰富的图表:")
                print("• 🚗 KPI: 活跃车辆数")
                print("• ⛽ KPI: 平均燃油百分比")
                print("• 🏃 KPI: 平均车速")
                print("• 📈 折线图: 燃油和速度趋势")
                print("• 🎯 饼图: 驾驶模式分布")
                print("• 💨 散点图: 速度与燃油关系")
                print("• 📊 柱状图: 车辆燃油对比")
                print("• 🔋 仪表盘: 平均充电时间")
                print("• 📋 表格: 车辆详细状态")
                print("="*70)
                return True
            elif status == 'CREATION_FAILED':
                errors = dashboard_detail.get('Dashboard', {}).get('Version', {}).get('Errors', [])
                print("❌ 看板创建失败:")
                for error in errors:
                    print(f"  • {error.get('Type')}: {error.get('Message')}")
                return False
            else:
                print(f"⏳ 看板状态: {status}")
                print(f"🔗 访问链接: https://us-west-2.quicksight.aws.amazon.com/sn/dashboards/{dashboard_id}")
                return True
                
        except Exception as status_error:
            print(f"⚠️ 无法检查看板状态: {status_error}")
            print(f"🔗 请直接访问: https://us-west-2.quicksight.aws.amazon.com/sn/dashboards/{dashboard_id}")
            return True
        
    except Exception as e:
        print(f"❌ 看板创建失败: {e}")
        return False

if __name__ == "__main__":
    create_beautiful_dashboard()
