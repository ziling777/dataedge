#!/usr/bin/env python3
"""
简化版 QuickSight 看板创建脚本
用途: 基于 S3 Tables 数据创建基础看板
"""

import boto3
import json
import time
import sys
from datetime import datetime

class SimpleQuickSightCreator:
    def __init__(self, region='us-west-2'):
        """初始化 QuickSight 客户端"""
        self.quicksight = boto3.client('quicksight', region_name=region)
        self.region = region
        self.account_id = boto3.client('sts').get_caller_identity()['Account']
        self.timestamp = str(int(time.time()))
        
        print(f"🚀 初始化 QuickSight 看板创建器")
        print(f"📍 AWS 账户: {self.account_id}")
        print(f"🌍 区域: {self.region}")
        
    def check_quicksight_subscription(self):
        """检查 QuickSight 订阅状态"""
        print("\n🔍 检查 QuickSight 订阅状态...")
        
        try:
            response = self.quicksight.describe_account_settings(
                AwsAccountId=self.account_id
            )
            print(f"✅ QuickSight 已启用: {response['AccountSettings']['AccountName']}")
            return True
        except Exception as e:
            print(f"❌ QuickSight 未启用: {str(e)}")
            return False
    
    def get_user_arn(self):
        """获取当前用户的 ARN"""
        try:
            response = self.quicksight.list_users(
                AwsAccountId=self.account_id,
                Namespace='default'
            )
            
            if response.get('UserList'):
                user = response['UserList'][0]  # 使用第一个用户
                print(f"🔍 使用用户: {user.get('UserName')}")
                return user.get('Arn')
            
            return f"arn:aws:quicksight:{self.region}:{self.account_id}:user/default/admin"
            
        except Exception as e:
            print(f"⚠️ 获取用户信息失败: {str(e)}")
            return f"arn:aws:quicksight:{self.region}:{self.account_id}:user/default/admin"
    
    def create_data_source(self):
        """创建 Athena 数据源"""
        print("\n🔗 创建 Athena 数据源...")
        
        data_source_id = f"s3tables-canbus-{self.timestamp}"
        user_arn = self.get_user_arn()
        
        try:
            response = self.quicksight.create_data_source(
                AwsAccountId=self.account_id,
                DataSourceId=data_source_id,
                Name='S3Tables CAN Bus Data Source',
                Type='ATHENA',
                DataSourceParameters={
                    'AthenaParameters': {
                        'WorkGroup': 'primary'
                    }
                },
                Permissions=[
                    {
                        'Principal': user_arn,
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
            print(f"✅ 数据源创建成功: {data_source_id}")
            return data_source_id
            
        except Exception as e:
            print(f"❌ 数据源创建失败: {str(e)}")
            return None
    
    def create_dataset(self, data_source_id):
        """创建数据集"""
        print("\n📊 创建数据集...")
        
        dataset_id = f"canbus-dataset-{self.timestamp}"
        user_arn = self.get_user_arn()
        
        try:
            response = self.quicksight.create_data_set(
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
                Permissions=[
                    {
                        'Principal': user_arn,
                        'Actions': [
                            'quicksight:UpdateDataSetPermissions',
                            'quicksight:DescribeDataSet',
                            'quicksight:DescribeDataSetPermissions',
                            'quicksight:PassDataSet',
                            'quicksight:DescribeIngestion',
                            'quicksight:ListIngestions',
                            'quicksight:UpdateDataSet',
                            'quicksight:DeleteDataSet',
                            'quicksight:CreateIngestion',
                            'quicksight:CancelIngestion'
                        ]
                    }
                ]
            )
            print(f"✅ 数据集创建成功: {dataset_id}")
            return dataset_id
            
        except Exception as e:
            print(f"❌ 数据集创建失败: {str(e)}")
            return None
    
    def create_simple_analysis(self, dataset_id):
        """创建简化的分析"""
        print("\n📈 创建分析...")
        
        analysis_id = f"canbus-analysis-{self.timestamp}"
        user_arn = self.get_user_arn()
        
        # 简化的定义，只包含基本的可视化
        definition = {
            'DataSetIdentifierDeclarations': [
                {
                    'DataSetArn': f'arn:aws:quicksight:{self.region}:{self.account_id}:dataset/{dataset_id}',
                    'Identifier': 'canbus_data'
                }
            ],
            'Sheets': [
                {
                    'SheetId': 'main_dashboard',
                    'Name': '车辆监控看板',
                    'Visuals': [
                        # KPI: 活跃车辆数
                        {
                            'KPIVisual': {
                                'VisualId': 'active_vehicles_kpi',
                                'Title': {
                                    'Visibility': 'VISIBLE'
                                },
                                'ChartConfiguration': {
                                    'FieldWells': {
                                        'Values': [
                                            {
                                                'NumericalMeasureField': {
                                                    'FieldId': 'active_vehicles',
                                                    'Column': {
                                                        'DataSetIdentifier': 'canbus_data',
                                                        'ColumnName': 'vin_id'
                                                    },
                                                    'AggregationFunction': {
                                                        'SimpleNumericalAggregation': 'DISTINCT_COUNT'
                                                    }
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        },
                        # 表格: 车辆状态
                        {
                            'TableVisual': {
                                'VisualId': 'vehicle_table',
                                'Title': {
                                    'Visibility': 'VISIBLE'
                                },
                                'ChartConfiguration': {
                                    'FieldWells': {
                                        'TableAggregatedFieldWells': {
                                            'GroupBy': [
                                                {
                                                    'CategoricalDimensionField': {
                                                        'FieldId': 'vin_id_dim',
                                                        'Column': {
                                                            'DataSetIdentifier': 'canbus_data',
                                                            'ColumnName': 'vin_id'
                                                        }
                                                    }
                                                }
                                            ],
                                            'Values': [
                                                {
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
                                                },
                                                {
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
            response = self.quicksight.create_analysis(
                AwsAccountId=self.account_id,
                AnalysisId=analysis_id,
                Name='车辆监控分析',
                Definition=definition,
                Permissions=[
                    {
                        'Principal': user_arn,
                        'Actions': [
                            'quicksight:RestoreAnalysis',
                            'quicksight:UpdateAnalysisPermissions',
                            'quicksight:DeleteAnalysis',
                            'quicksight:DescribeAnalysisPermissions',
                            'quicksight:QueryAnalysis',
                            'quicksight:DescribeAnalysis',
                            'quicksight:UpdateAnalysis'
                        ]
                    }
                ]
            )
            print(f"✅ 分析创建成功: {analysis_id}")
            return analysis_id
            
        except Exception as e:
            print(f"❌ 分析创建失败: {str(e)}")
            return None
    
    def create_dashboard(self, analysis_id, dataset_id):
        """创建看板"""
        print("\n🎯 创建看板...")
        
        dashboard_id = f"canbus-dashboard-{self.timestamp}"
        user_arn = self.get_user_arn()
        
        try:
            response = self.quicksight.create_dashboard(
                AwsAccountId=self.account_id,
                DashboardId=dashboard_id,
                Name='车辆监控看板',
                SourceEntity={
                    'SourceTemplate': {
                        'DataSetReferences': [
                            {
                                'DataSetArn': f'arn:aws:quicksight:{self.region}:{self.account_id}:dataset/{dataset_id}',
                                'DataSetPlaceholder': 'canbus_data'
                            }
                        ],
                        'Arn': f'arn:aws:quicksight:{self.region}:{self.account_id}:analysis/{analysis_id}'
                    }
                },
                Permissions=[
                    {
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
                    }
                ]
            )
            print(f"✅ 看板创建成功: {dashboard_id}")
            return dashboard_id
            
        except Exception as e:
            print(f"❌ 看板创建失败: {str(e)}")
            return None
    
    def create_complete_dashboard(self):
        """创建完整的看板"""
        print("🚀 开始创建车辆监控看板...")
        
        # 检查 QuickSight
        if not self.check_quicksight_subscription():
            return False
        
        # 创建数据源
        data_source_id = self.create_data_source()
        if not data_source_id:
            return False
        
        time.sleep(5)
        
        # 创建数据集
        dataset_id = self.create_dataset(data_source_id)
        if not dataset_id:
            return False
        
        time.sleep(10)
        
        # 创建分析
        analysis_id = self.create_simple_analysis(dataset_id)
        if not analysis_id:
            return False
        
        time.sleep(10)
        
        # 创建看板
        dashboard_id = self.create_dashboard(analysis_id, dataset_id)
        if not dashboard_id:
            return False
        
        # 显示结果
        print("\n" + "="*60)
        print("🎉 车辆监控看板创建完成！")
        print("="*60)
        print(f"📊 数据源 ID: {data_source_id}")
        print(f"📈 数据集 ID: {dataset_id}")
        print(f"🔍 分析 ID: {analysis_id}")
        print(f"🎯 看板 ID: {dashboard_id}")
        print(f"🔗 访问链接: https://{self.region}.quicksight.aws.amazon.com/sn/dashboards/{dashboard_id}")
        print("="*60)
        
        return True

def main():
    print("🚗 QuickSight 车辆监控看板创建工具（简化版）")
    print("="*50)
    
    region = 'us-west-2'  # 使用您当前的区域
    print(f"🌍 使用 AWS 区域: {region}")
    
    response = input("\n是否开始创建看板？(y/N): ").strip().lower()
    if response != 'y':
        print("❌ 操作已取消")
        return
    
    creator = SimpleQuickSightCreator(region=region)
    
    try:
        success = creator.create_complete_dashboard()
        if success:
            print("\n🎊 看板创建成功！")
        else:
            print("\n❌ 看板创建失败")
            sys.exit(1)
    except Exception as e:
        print(f"\n❌ 发生错误: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
