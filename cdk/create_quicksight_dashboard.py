#!/usr/bin/env python3
"""
QuickSight 看板自动创建脚本
用途: 自动创建车辆监控综合看板
"""

import boto3
import json
import time
import sys
from datetime import datetime

class QuickSightDashboardCreator:
    def __init__(self, region='us-east-1'):
        self.quicksight = boto3.client('quicksight', region_name=region)
        self.region = region
        self.account_id = boto3.client('sts').get_caller_identity()['Account']
        
    def create_data_source(self):
        """创建 Athena 数据源"""
        print("🔗 创建 Athena 数据源...")
        
        data_source_id = f"s3tables-canbus-{int(time.time())}"
        
        try:
            response = self.quicksight.create_data_source(
                AwsAccountId=self.account_id,
                DataSourceId=data_source_id,
                Name='S3Tables CAN Bus Data',
                Type='ATHENA',
                DataSourceParameters={
                    'AthenaParameters': {
                        'WorkGroup': 'primary'
                    }
                },
                Permissions=[
                    {
                        'Principal': f'arn:aws:quicksight:{self.region}:{self.account_id}:user/default/{boto3.client("sts").get_caller_identity()["UserId"]}',
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
        print("📊 创建数据集...")
        
        dataset_id = f"canbus-dataset-{int(time.time())}"
        
        try:
            response = self.quicksight.create_data_set(
                AwsAccountId=self.account_id,
                DataSetId=dataset_id,
                Name='CAN Bus Dataset',
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
                        'Principal': f'arn:aws:quicksight:{self.region}:{self.account_id}:user/default/{boto3.client("sts").get_caller_identity()["UserId"]}',
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
    
    def create_analysis(self, dataset_id):
        """创建分析"""
        print("📈 创建分析...")
        
        analysis_id = f"canbus-analysis-{int(time.time())}"
        
        # 定义看板布局
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
                    'Visuals': self._create_visuals(),
                    'FilterControls': self._create_filters(),
                    'ParameterControls': self._create_parameters()
                }
            ]
        }
        
        try:
            response = self.quicksight.create_analysis(
                AwsAccountId=self.account_id,
                AnalysisId=analysis_id,
                Name='车辆监控综合分析',
                Definition=definition,
                Permissions=[
                    {
                        'Principal': f'arn:aws:quicksight:{self.region}:{self.account_id}:user/default/{boto3.client("sts").get_caller_identity()["UserId"]}',
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
    
    def _create_visuals(self):
        """创建可视化组件"""
        visuals = []
        
        # KPI 1: 活跃车辆数
        visuals.append({
            'KPIVisual': {
                'VisualId': 'active_vehicles_kpi',
                'Title': {'Visibility': 'VISIBLE', 'Label': '活跃车辆数'},
                'Subtitle': {'Visibility': 'VISIBLE'},
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
                                    'AggregationFunction': {'SimpleNumericalAggregation': 'DISTINCT_COUNT'}
                                }
                            }
                        ]
                    },
                    'KPIOptions': {
                        'Comparison': {
                            'ComparisonMethod': 'DIFFERENCE'
                        }
                    }
                }
            }
        })
        
        # KPI 2: 平均燃油百分比
        visuals.append({
            'KPIVisual': {
                'VisualId': 'avg_fuel_kpi',
                'Title': {'Visibility': 'VISIBLE', 'Label': '平均燃油百分比'},
                'Subtitle': {'Visibility': 'VISIBLE'},
                'ChartConfiguration': {
                    'FieldWells': {
                        'Values': [
                            {
                                'NumericalMeasureField': {
                                    'FieldId': 'avg_fuel',
                                    'Column': {
                                        'DataSetIdentifier': 'canbus_data',
                                        'ColumnName': 'fuel_percentage'
                                    },
                                    'AggregationFunction': {'SimpleNumericalAggregation': 'AVERAGE'}
                                }
                            }
                        ]
                    }
                }
            }
        })
        
        # 折线图: 24小时趋势
        visuals.append({
            'LineChartVisual': {
                'VisualId': 'trend_line_chart',
                'Title': {'Visibility': 'VISIBLE', 'Label': '24小时燃油和速度趋势'},
                'Subtitle': {'Visibility': 'VISIBLE'},
                'ChartConfiguration': {
                    'FieldWells': {
                        'LineChartAggregatedFieldWells': {
                            'Category': [
                                {
                                    'DateDimensionField': {
                                        'FieldId': 'timestamp_hour',
                                        'Column': {
                                            'DataSetIdentifier': 'canbus_data',
                                            'ColumnName': 'ts'
                                        },
                                        'DateGranularity': 'HOUR'
                                    }
                                }
                            ],
                            'Values': [
                                {
                                    'NumericalMeasureField': {
                                        'FieldId': 'avg_fuel_trend',
                                        'Column': {
                                            'DataSetIdentifier': 'canbus_data',
                                            'ColumnName': 'fuel_percentage'
                                        },
                                        'AggregationFunction': {'SimpleNumericalAggregation': 'AVERAGE'}
                                    }
                                },
                                {
                                    'NumericalMeasureField': {
                                        'FieldId': 'avg_speed_trend',
                                        'Column': {
                                            'DataSetIdentifier': 'canbus_data',
                                            'ColumnName': 'display_speed'
                                        },
                                        'AggregationFunction': {'SimpleNumericalAggregation': 'AVERAGE'}
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        })
        
        # 饼图: 驾驶模式分布
        visuals.append({
            'PieChartVisual': {
                'VisualId': 'driving_mode_pie',
                'Title': {'Visibility': 'VISIBLE', 'Label': '驾驶模式分布'},
                'Subtitle': {'Visibility': 'VISIBLE'},
                'ChartConfiguration': {
                    'FieldWells': {
                        'PieChartAggregatedFieldWells': {
                            'Category': [
                                {
                                    'CategoricalDimensionField': {
                                        'FieldId': 'clean_mode_cat',
                                        'Column': {
                                            'DataSetIdentifier': 'canbus_data',
                                            'ColumnName': 'clean_mode'
                                        }
                                    }
                                }
                            ],
                            'Values': [
                                {
                                    'NumericalMeasureField': {
                                        'FieldId': 'mode_count',
                                        'Column': {
                                            'DataSetIdentifier': 'canbus_data',
                                            'ColumnName': 'vin_id'
                                        },
                                        'AggregationFunction': {'SimpleNumericalAggregation': 'COUNT'}
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        })
        
        return visuals
    
    def _create_filters(self):
        """创建过滤器"""
        return [
            {
                'DateTimePickerFilter': {
                    'FilterId': 'time_filter',
                    'Title': '时间范围',
                    'Column': {
                        'DataSetIdentifier': 'canbus_data',
                        'ColumnName': 'ts'
                    }
                }
            }
        ]
    
    def _create_parameters(self):
        """创建参数控件"""
        return []
    
    def create_dashboard(self, analysis_id):
        """从分析创建看板"""
        print("🎯 创建看板...")
        
        dashboard_id = f"canbus-dashboard-{int(time.time())}"
        
        try:
            response = self.quicksight.create_dashboard(
                AwsAccountId=self.account_id,
                DashboardId=dashboard_id,
                Name='车辆监控综合看板',
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
                        'Principal': f'arn:aws:quicksight:{self.region}:{self.account_id}:user/default/{boto3.client("sts").get_caller_identity()["UserId"]}',
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
    
    def get_dashboard_url(self, dashboard_id):
        """获取看板 URL"""
        try:
            response = self.quicksight.get_dashboard_embed_url(
                AwsAccountId=self.account_id,
                DashboardId=dashboard_id,
                IdentityType='IAM'
            )
            return response['EmbedUrl']
        except Exception as e:
            print(f"❌ 获取看板 URL 失败: {str(e)}")
            return None

def main():
    print("🚀 开始创建 QuickSight 车辆监控看板...")
    
    # 检查 QuickSight 订阅
    try:
        quicksight = boto3.client('quicksight')
        account_id = boto3.client('sts').get_caller_identity()['Account']
        
        # 检查 QuickSight 账户状态
        response = quicksight.describe_account_settings(AwsAccountId=account_id)
        print(f"✅ QuickSight 账户状态: {response['AccountSettings']['AccountName']}")
        
    except Exception as e:
        print(f"❌ QuickSight 未启用或配置错误: {str(e)}")
        print("请先在 AWS 控制台中启用 QuickSight 服务")
        sys.exit(1)
    
    # 创建看板
    creator = QuickSightDashboardCreator()
    
    # 步骤 1: 创建数据源
    data_source_id = creator.create_data_source()
    if not data_source_id:
        sys.exit(1)
    
    # 等待数据源创建完成
    time.sleep(10)
    
    # 步骤 2: 创建数据集
    dataset_id = creator.create_dataset(data_source_id)
    if not dataset_id:
        sys.exit(1)
    
    # 等待数据集创建完成
    time.sleep(10)
    
    # 步骤 3: 创建分析
    analysis_id = creator.create_analysis(dataset_id)
    if not analysis_id:
        sys.exit(1)
    
    # 等待分析创建完成
    time.sleep(15)
    
    # 步骤 4: 创建看板
    dashboard_id = creator.create_dashboard(analysis_id)
    if not dashboard_id:
        sys.exit(1)
    
    # 获取看板 URL
    dashboard_url = creator.get_dashboard_url(dashboard_id)
    
    print("\n🎉 看板创建完成！")
    print("=" * 50)
    print(f"数据源 ID: {data_source_id}")
    print(f"数据集 ID: {dataset_id}")
    print(f"分析 ID: {analysis_id}")
    print(f"看板 ID: {dashboard_id}")
    if dashboard_url:
        print(f"看板 URL: {dashboard_url}")
    print("=" * 50)
    print("\n📝 后续步骤:")
    print("1. 登录 QuickSight 控制台")
    print("2. 找到创建的看板并进行个性化配置")
    print("3. 添加更多可视化组件")
    print("4. 设置自动刷新和告警")

if __name__ == "__main__":
    main()
