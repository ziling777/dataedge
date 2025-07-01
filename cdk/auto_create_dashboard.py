#!/usr/bin/env python3
"""
完全自动化的 QuickSight 看板创建脚本
通过 Python 直接创建完整的车辆监控看板
"""

import boto3
import json
import time
import uuid

class AutoDashboardCreator:
    def __init__(self, region='us-west-2'):
        self.quicksight = boto3.client('quicksight', region_name=region)
        self.region = region
        self.account_id = boto3.client('sts').get_caller_identity()['Account']
        self.timestamp = str(int(time.time()))
        
        print(f"🚀 自动化看板创建器启动")
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
        
        data_source_id = f"s3tables-canbus-{self.timestamp}"
        user_arn = self.get_user_arn()
        
        try:
            self.quicksight.create_data_source(
                AwsAccountId=self.account_id,
                DataSourceId=data_source_id,
                Name='S3Tables CAN Bus Data',
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
        """创建数据集"""
        print("\n📊 创建数据集...")
        
        dataset_id = f"canbus-dataset-{self.timestamp}"
        user_arn = self.get_user_arn()
        
        try:
            self.quicksight.create_data_set(
                AwsAccountId=self.account_id,
                DataSetId=dataset_id,
                Name='CAN Bus Vehicle Data',
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
                                {'Name': 'fuel_percentage', 'Type': 'INTEGER'},
                                {'Name': 'display_speed', 'Type': 'DECIMAL'},
                                {'Name': 'clean_mode', 'Type': 'INTEGER'},
                                {'Name': 'road_mode', 'Type': 'INTEGER'},
                                {'Name': 'ress_power_low_flag', 'Type': 'BIT'},
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
    
    def create_dashboard_directly(self, dataset_id):
        """直接创建看板（不通过分析）"""
        print("\n🎯 直接创建看板...")
        
        dashboard_id = f"canbus-dashboard-{self.timestamp}"
        user_arn = self.get_user_arn()
        
        # 使用简化的看板定义
        definition = {
            'DataSetIdentifierDeclarations': [
                {
                    'DataSetArn': f'arn:aws:quicksight:{self.region}:{self.account_id}:dataset/{dataset_id}',
                    'Identifier': 'canbus_data'
                }
            ],
            'Sheets': [
                {
                    'SheetId': 'main_sheet',
                    'Name': '车辆监控',
                    'Visuals': [
                        # KPI: 活跃车辆数
                        {
                            'KPIVisual': {
                                'VisualId': 'kpi_vehicles',
                                'Title': {'Visibility': 'VISIBLE'},
                                'ChartConfiguration': {
                                    'FieldWells': {
                                        'Values': [{
                                            'NumericalMeasureField': {
                                                'FieldId': 'vehicle_count',
                                                'Column': {
                                                    'DataSetIdentifier': 'canbus_data',
                                                    'ColumnName': 'vin_id'
                                                },
                                                'AggregationFunction': {
                                                    'SimpleNumericalAggregation': 'DISTINCT_COUNT'
                                                }
                                            }
                                        }]
                                    }
                                }
                            }
                        },
                        # KPI: 平均燃油
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
                        }
                    ]
                }
            ]
        }
        
        try:
            response = self.quicksight.create_dashboard(
                AwsAccountId=self.account_id,
                DashboardId=dashboard_id,
                Name='车辆监控看板',
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
            print(f"✅ 看板创建成功: {dashboard_id}")
            return dashboard_id
        except Exception as e:
            print(f"❌ 看板创建失败: {e}")
            print(f"详细错误: {str(e)}")
            return None
    
    def create_complete_solution(self):
        """创建完整解决方案"""
        print("🚀 开始自动创建完整的车辆监控看板...")
        
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
        
        # 步骤 3: 直接创建看板
        dashboard_id = self.create_dashboard_directly(dataset_id)
        if not dashboard_id:
            return False
        
        # 显示结果
        print("\n" + "="*60)
        print("🎉 车辆监控看板自动创建完成！")
        print("="*60)
        print(f"📊 数据源 ID: {data_source_id}")
        print(f"📈 数据集 ID: {dataset_id}")
        print(f"🎯 看板 ID: {dashboard_id}")
        print(f"🔗 访问链接: https://{self.region}.quicksight.aws.amazon.com/sn/dashboards/{dashboard_id}")
        
        print("\n📊 看板包含:")
        print("• KPI: 活跃车辆数量")
        print("• KPI: 平均燃油百分比")
        print("• 表格: 车辆详细状态")
        
        print("\n💡 后续操作:")
        print("1. 访问上面的链接查看看板")
        print("2. 在 QuickSight 中可以进一步编辑和美化")
        print("3. 添加更多图表类型（折线图、饼图等）")
        print("4. 设置自动刷新和权限")
        print("="*60)
        
        return True

def main():
    print("🚗 QuickSight 车辆监控看板自动创建工具")
    print("="*50)
    
    creator = AutoDashboardCreator(region='us-west-2')
    
    try:
        success = creator.create_complete_solution()
        if success:
            print("\n🎊 自动创建成功！")
        else:
            print("\n❌ 自动创建失败")
    except Exception as e:
        print(f"\n❌ 发生错误: {e}")

if __name__ == "__main__":
    main()
