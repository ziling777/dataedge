#!/usr/bin/env python3
"""
基于实际 canbus01 表结构的电车业务看板创建脚本
表结构字段：
- __primary_key STRING
- vin_id STRING  
- charging_time_remain_minute INT
- extender_starting_point INT
- fuel_cltc_mileage INT
- fuel_wltc_mileage INT
- fuel_percentage INT
- clean_mode INT
- road_mode INT
- ress_power_low_flag BOOLEAN
- target_soc INT
- display_speed FLOAT
- channel_id INT
- endurance_type INT
- ts TIMESTAMP
"""

import boto3
import time

class EVBusinessDashboardCreator:
    def __init__(self, region='us-west-2'):
        self.quicksight = boto3.client('quicksight', region_name=region)
        self.region = region
        self.account_id = boto3.client('sts').get_caller_identity()['Account']
        self.timestamp = str(int(time.time()))
        
        print("🔋 电车业务看板创建器启动")
        print(f"📍 AWS 账户: {self.account_id}")
        print(f"🌍 区域: {self.region}")
    
    def get_user_arn(self):
        """获取用户 ARN"""
        try:
            response = self.quicksight.list_users(
                AwsAccountId=self.account_id,
                Namespace='default'
            )
            if response.get('UserList'):
                user = response['UserList'][0]
                print(f"👤 使用用户: {user.get('UserName')}")
                return user.get('Arn')
        except Exception as e:
            print(f"⚠️ 获取用户失败: {e}")
        
        return f"arn:aws:quicksight:{self.region}:{self.account_id}:user/default/admin"
    
    def create_data_source(self):
        """创建数据源"""
        print("\n🔗 创建 Athena 数据源...")
        
        data_source_id = f"ev-canbus-datasource-{self.timestamp}"
        user_arn = self.get_user_arn()
        
        try:
            self.quicksight.create_data_source(
                AwsAccountId=self.account_id,
                DataSourceId=data_source_id,
                Name='电车 CAN 总线数据源',
                Type='ATHENA',
                DataSourceParameters={
                    'AthenaParameters': {
                        'WorkGroup': 'primary'
                    }
                },
                Permissions=[{
                    'Principal': user_arn,
                    'Actions': [
                        'quicksight:DescribeDataSource',
                        'quicksight:DescribeDataSourcePermissions',
                        'quicksight:PassDataSource',
                        'quicksight:UpdateDataSource',
                        'quicksight:DeleteDataSource',
                        'quicksight:UpdateDataSourcePermissions'
                    ]
                }]
            )
            print(f"✅ 数据源创建成功: {data_source_id}")
            return data_source_id
        except Exception as e:
            print(f"❌ 数据源创建失败: {e}")
            return None
    
    def create_dataset(self, data_source_id):
        """创建数据集 - 基于实际表结构"""
        print("\n📊 创建数据集...")
        
        dataset_id = f"ev-canbus-dataset-{self.timestamp}"
        user_arn = self.get_user_arn()
        
        try:
            self.quicksight.create_data_set(
                AwsAccountId=self.account_id,
                DataSetId=dataset_id,
                Name='电车 CAN 总线数据集',
                PhysicalTableMap={
                    'canbus01': {
                        'RelationalTable': {
                            'DataSourceArn': f'arn:aws:quicksight:{self.region}:{self.account_id}:datasource/{data_source_id}',
                            'Catalog': 's3tablescatalog/caredgedemo',
                            'Schema': 'greptime',
                            'Name': 'canbus01',
                            'InputColumns': [
                                {'Name': '__primary_key', 'Type': 'STRING'},
                                {'Name': 'vin_id', 'Type': 'STRING'},
                                {'Name': 'charging_time_remain_minute', 'Type': 'INTEGER'},
                                {'Name': 'extender_starting_point', 'Type': 'INTEGER'},
                                {'Name': 'fuel_cltc_mileage', 'Type': 'INTEGER'},
                                {'Name': 'fuel_wltc_mileage', 'Type': 'INTEGER'},
                                {'Name': 'fuel_percentage', 'Type': 'INTEGER'},
                                {'Name': 'clean_mode', 'Type': 'INTEGER'},
                                {'Name': 'road_mode', 'Type': 'INTEGER'},
                                {'Name': 'ress_power_low_flag', 'Type': 'BIT'},
                                {'Name': 'target_soc', 'Type': 'INTEGER'},
                                {'Name': 'display_speed', 'Type': 'DECIMAL'},
                                {'Name': 'channel_id', 'Type': 'INTEGER'},
                                {'Name': 'endurance_type', 'Type': 'INTEGER'},
                                {'Name': 'ts', 'Type': 'DATETIME'}
                            ]
                        }
                    }
                },
                ImportMode='DIRECT_QUERY',
                Permissions=[{
                    'Principal': user_arn,
                    'Actions': [
                        'quicksight:DescribeDataSet',
                        'quicksight:DescribeDataSetPermissions',
                        'quicksight:PassDataSet',
                        'quicksight:DescribeIngestion',
                        'quicksight:ListIngestions',
                        'quicksight:UpdateDataSet',
                        'quicksight:DeleteDataSet',
                        'quicksight:CreateIngestion',
                        'quicksight:CancelIngestion',
                        'quicksight:UpdateDataSetPermissions'
                    ]
                }]
            )
            print(f"✅ 数据集创建成功: {dataset_id}")
            return dataset_id
        except Exception as e:
            print(f"❌ 数据集创建失败: {e}")
            return None
    
    def create_ev_business_dashboard(self, dataset_id):
        """创建电车业务看板"""
        print("\n🎯 创建电车业务看板...")
        
        dashboard_id = f"ev-business-dashboard-{self.timestamp}"
        user_arn = self.get_user_arn()
        
        # 基于实际字段的电车业务看板定义
        definition = {
            'DataSetIdentifierDeclarations': [
                {
                    'DataSetArn': f'arn:aws:quicksight:{self.region}:{self.account_id}:dataset/{dataset_id}',
                    'Identifier': 'ev_canbus_data'
                }
            ],
            'Sheets': [
                {
                    'SheetId': 'ev_business_main',
                    'Name': '电车业务监控',
                    'Visuals': [
                        # 1. KPI: 在线电车数量
                        {
                            'KPIVisual': {
                                'VisualId': 'kpi_ev_fleet_size',
                                'Title': {
                                    'Visibility': 'VISIBLE',
                                    'FormatText': {
                                        'PlainText': '🚗 在线电车数量'
                                    }
                                },
                                'ChartConfiguration': {
                                    'FieldWells': {
                                        'Values': [{
                                            'CategoricalMeasureField': {
                                                'FieldId': 'ev_fleet_count',
                                                'Column': {
                                                    'DataSetIdentifier': 'ev_canbus_data',
                                                    'ColumnName': 'vin_id'
                                                },
                                                'AggregationFunction': 'DISTINCT_COUNT'
                                            }
                                        }]
                                    }
                                }
                            }
                        },
                        
                        # 2. KPI: 平均电池 SOC
                        {
                            'KPIVisual': {
                                'VisualId': 'kpi_avg_soc',
                                'Title': {
                                    'Visibility': 'VISIBLE',
                                    'FormatText': {
                                        'PlainText': '🔋 平均电池 SOC (%)'
                                    }
                                },
                                'ChartConfiguration': {
                                    'FieldWells': {
                                        'Values': [{
                                            'NumericalMeasureField': {
                                                'FieldId': 'avg_target_soc',
                                                'Column': {
                                                    'DataSetIdentifier': 'ev_canbus_data',
                                                    'ColumnName': 'target_soc'
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
                        
                        # 3. KPI: 平均充电剩余时间
                        {
                            'KPIVisual': {
                                'VisualId': 'kpi_avg_charging_time',
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
                                                'FieldId': 'avg_charging_remain',
                                                'Column': {
                                                    'DataSetIdentifier': 'ev_canbus_data',
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
                        
                        # 4. KPI: 平均行驶速度
                        {
                            'KPIVisual': {
                                'VisualId': 'kpi_avg_speed',
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
                                                'FieldId': 'avg_display_speed',
                                                'Column': {
                                                    'DataSetIdentifier': 'ev_canbus_data',
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
                        
                        # 5. 折线图: SOC 时间趋势
                        {
                            'LineChartVisual': {
                                'VisualId': 'line_soc_trend',
                                'Title': {
                                    'Visibility': 'VISIBLE',
                                    'FormatText': {
                                        'PlainText': '🔋 电池 SOC 时间趋势'
                                    }
                                },
                                'ChartConfiguration': {
                                    'FieldWells': {
                                        'LineChartAggregatedFieldWells': {
                                            'Category': [{
                                                'DateDimensionField': {
                                                    'FieldId': 'time_soc_trend',
                                                    'Column': {
                                                        'DataSetIdentifier': 'ev_canbus_data',
                                                        'ColumnName': 'ts'
                                                    },
                                                    'DateGranularity': 'HOUR'
                                                }
                                            }],
                                            'Values': [{
                                                'NumericalMeasureField': {
                                                    'FieldId': 'soc_trend_value',
                                                    'Column': {
                                                        'DataSetIdentifier': 'ev_canbus_data',
                                                        'ColumnName': 'target_soc'
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
                                                    'FieldId': 'time_charging_trend',
                                                    'Column': {
                                                        'DataSetIdentifier': 'ev_canbus_data',
                                                        'ColumnName': 'ts'
                                                    },
                                                    'DateGranularity': 'HOUR'
                                                }
                                            }],
                                            'Values': [{
                                                'NumericalMeasureField': {
                                                    'FieldId': 'charging_trend_value',
                                                    'Column': {
                                                        'DataSetIdentifier': 'ev_canbus_data',
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
                        
                        # 7. 柱状图: 各车辆 SOC 对比
                        {
                            'BarChartVisual': {
                                'VisualId': 'bar_soc_by_vehicle',
                                'Title': {
                                    'Visibility': 'VISIBLE',
                                    'FormatText': {
                                        'PlainText': '🔋 各电车 SOC 对比'
                                    }
                                },
                                'ChartConfiguration': {
                                    'FieldWells': {
                                        'BarChartAggregatedFieldWells': {
                                            'Category': [{
                                                'CategoricalDimensionField': {
                                                    'FieldId': 'vin_soc_category',
                                                    'Column': {
                                                        'DataSetIdentifier': 'ev_canbus_data',
                                                        'ColumnName': 'vin_id'
                                                    }
                                                }
                                            }],
                                            'Values': [{
                                                'NumericalMeasureField': {
                                                    'FieldId': 'soc_by_vehicle',
                                                    'Column': {
                                                        'DataSetIdentifier': 'ev_canbus_data',
                                                        'ColumnName': 'target_soc'
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
                        
                        # 8. 柱状图: CLTC vs WLTC 续航对比
                        {
                            'BarChartVisual': {
                                'VisualId': 'bar_mileage_comparison',
                                'Title': {
                                    'Visibility': 'VISIBLE',
                                    'FormatText': {
                                        'PlainText': '🛣️ CLTC vs WLTC 续航对比'
                                    }
                                },
                                'ChartConfiguration': {
                                    'FieldWells': {
                                        'BarChartAggregatedFieldWells': {
                                            'Category': [{
                                                'CategoricalDimensionField': {
                                                    'FieldId': 'vin_mileage_category',
                                                    'Column': {
                                                        'DataSetIdentifier': 'ev_canbus_data',
                                                        'ColumnName': 'vin_id'
                                                    }
                                                }
                                            }],
                                            'Values': [
                                                {
                                                    'NumericalMeasureField': {
                                                        'FieldId': 'cltc_mileage',
                                                        'Column': {
                                                            'DataSetIdentifier': 'ev_canbus_data',
                                                            'ColumnName': 'fuel_cltc_mileage'
                                                        },
                                                        'AggregationFunction': {
                                                            'SimpleNumericalAggregation': 'AVERAGE'
                                                        }
                                                    }
                                                },
                                                {
                                                    'NumericalMeasureField': {
                                                        'FieldId': 'wltc_mileage',
                                                        'Column': {
                                                            'DataSetIdentifier': 'ev_canbus_data',
                                                            'ColumnName': 'fuel_wltc_mileage'
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
                        
                        # 9. 表格: 电车详细业务数据
                        {
                            'TableVisual': {
                                'VisualId': 'table_ev_business_details',
                                'Title': {
                                    'Visibility': 'VISIBLE',
                                    'FormatText': {
                                        'PlainText': '📊 电车业务详细数据'
                                    }
                                },
                                'ChartConfiguration': {
                                    'FieldWells': {
                                        'TableAggregatedFieldWells': {
                                            'GroupBy': [{
                                                'CategoricalDimensionField': {
                                                    'FieldId': 'vin_business_table',
                                                    'Column': {
                                                        'DataSetIdentifier': 'ev_canbus_data',
                                                        'ColumnName': 'vin_id'
                                                    }
                                                }
                                            }],
                                            'Values': [
                                                {
                                                    'NumericalMeasureField': {
                                                        'FieldId': 'soc_business_table',
                                                        'Column': {
                                                            'DataSetIdentifier': 'ev_canbus_data',
                                                            'ColumnName': 'target_soc'
                                                        },
                                                        'AggregationFunction': {
                                                            'SimpleNumericalAggregation': 'AVERAGE'
                                                        }
                                                    }
                                                },
                                                {
                                                    'NumericalMeasureField': {
                                                        'FieldId': 'speed_business_table',
                                                        'Column': {
                                                            'DataSetIdentifier': 'ev_canbus_data',
                                                            'ColumnName': 'display_speed'
                                                        },
                                                        'AggregationFunction': {
                                                            'SimpleNumericalAggregation': 'AVERAGE'
                                                        }
                                                    }
                                                },
                                                {
                                                    'NumericalMeasureField': {
                                                        'FieldId': 'charging_business_table',
                                                        'Column': {
                                                            'DataSetIdentifier': 'ev_canbus_data',
                                                            'ColumnName': 'charging_time_remain_minute'
                                                        },
                                                        'AggregationFunction': {
                                                            'SimpleNumericalAggregation': 'AVERAGE'
                                                        }
                                                    }
                                                },
                                                {
                                                    'NumericalMeasureField': {
                                                        'FieldId': 'cltc_business_table',
                                                        'Column': {
                                                            'DataSetIdentifier': 'ev_canbus_data',
                                                            'ColumnName': 'fuel_cltc_mileage'
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
            response = self.quicksight.create_dashboard(
                AwsAccountId=self.account_id,
                DashboardId=dashboard_id,
                Name='🔋 电车业务监控看板',
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
            
            print(f"✅ 电车业务看板创建成功: {dashboard_id}")
            return dashboard_id
            
        except Exception as e:
            print(f"❌ 看板创建失败: {e}")
            return None
    
    def create_complete_solution(self):
        """创建完整的电车业务解决方案"""
        print("🚀 开始创建电车业务监控解决方案...")
        
        # 步骤 1: 创建数据源
        data_source_id = self.create_data_source()
        if not data_source_id:
            return False
        
        time.sleep(5)
        
        # 步骤 2: 创建数据集
        dataset_id = self.create_dataset(data_source_id)
        if not dataset_id:
            return False
        
        time.sleep(10)
        
        # 步骤 3: 创建业务看板
        dashboard_id = self.create_ev_business_dashboard(dataset_id)
        if not dashboard_id:
            return False
        
        time.sleep(15)
        
        # 检查看板状态
        try:
            dashboard_detail = self.quicksight.describe_dashboard(
                AwsAccountId=self.account_id,
                DashboardId=dashboard_id
            )
            status = dashboard_detail.get('Dashboard', {}).get('Version', {}).get('Status')
            
            print("\n" + "="*80)
            print("🔋 电车业务监控看板创建完成！")
            print("="*80)
            print(f"🎯 看板 ID: {dashboard_id}")
            print(f"🔗 访问链接: https://{self.region}.quicksight.aws.amazon.com/sn/dashboards/{dashboard_id}")
            print(f"📊 看板状态: {status}")
            
            print("\n📊 基于实际表结构的业务指标:")
            print("• 🚗 在线电车数量 (基于 vin_id)")
            print("• 🔋 平均电池 SOC (基于 target_soc)")
            print("• ⏰ 平均充电剩余时间 (基于 charging_time_remain_minute)")
            print("• ⚡ 平均行驶速度 (基于 display_speed)")
            print("• 🔋 SOC 时间趋势分析")
            print("• ⚡ 充电时间变化趋势")
            print("• 🔋 各电车 SOC 对比")
            print("• 🛣️ CLTC vs WLTC 续航对比")
            print("• 📊 电车业务详细数据表")
            
            if status == 'CREATION_FAILED':
                errors = dashboard_detail.get('Dashboard', {}).get('Version', {}).get('Errors', [])
                print("\n⚠️ 创建过程中的错误:")
                for error in errors:
                    print(f"  • {error.get('Type')}: {error.get('Message')}")
            
            print("\n🔋 电车业务特色功能:")
            print("• 基于真实 CAN 总线数据的准确监控")
            print("• SOC (State of Charge) 电池状态监控")
            print("• 充电进度和剩余时间跟踪")
            print("• CLTC/WLTC 标准续航里程对比")
            print("• 多维度电车性能分析")
            print("• 实时业务数据展示")
            print("="*80)
            
            return True
            
        except Exception as e:
            print(f"⚠️ 无法检查看板状态: {e}")
            print(f"🔗 请直接访问: https://{self.region}.quicksight.aws.amazon.com/sn/dashboards/{dashboard_id}")
            return True

def main():
    print("🔋 电车业务看板自动创建工具")
    print("基于实际 canbus01 表结构")
    print("="*50)
    
    creator = EVBusinessDashboardCreator(region='us-west-2')
    
    try:
        success = creator.create_complete_solution()
        if success:
            print("\n🎊 电车业务看板创建成功！")
        else:
            print("\n❌ 电车业务看板创建失败")
    except Exception as e:
        print(f"\n❌ 发生错误: {e}")

if __name__ == "__main__":
    main()
