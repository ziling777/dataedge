#!/usr/bin/env python3
"""
QuickSight 车辆监控看板一键创建脚本
用途: 基于 S3 Tables 数据自动创建完整的车辆监控看板
作者: Amazon Q
"""

import boto3
import json
import time
import sys
from datetime import datetime
import uuid

class QuickSightDashboardCreator:
    def __init__(self, region='us-east-1'):
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
            print("\n请按照以下步骤启用 QuickSight:")
            print("1. 访问 AWS 控制台")
            print("2. 搜索 'QuickSight' 服务")
            print("3. 点击 'Sign up for QuickSight'")
            print("4. 选择 Standard 版本并完成注册")
            return False
    
    def create_data_source(self):
        """创建 Athena 数据源"""
        print("\n🔗 创建 Athena 数据源...")
        
        data_source_id = f"s3tables-canbus-{self.timestamp}"
        
        try:
            # 获取当前用户信息
            user_arn = self._get_user_arn()
            
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
        
        try:
            user_arn = self._get_user_arn()
            
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
    
    def _get_user_arn(self):
        """获取当前用户的 ARN"""
        try:
            # 首先尝试列出用户来找到正确的用户名
            response = self.quicksight.list_users(
                AwsAccountId=self.account_id,
                Namespace='default'
            )
            
            # 获取当前调用者信息
            caller_identity = boto3.client('sts').get_caller_identity()
            current_user_id = caller_identity.get('UserId', '')
            current_arn = caller_identity.get('Arn', '')
            
            # 尝试从用户列表中找到匹配的用户
            for user in response.get('UserList', []):
                user_arn = user.get('Arn', '')
                if current_user_id in user_arn or 'admin' in user.get('UserName', '').lower():
                    print(f"🔍 找到匹配用户: {user.get('UserName')}")
                    return user_arn
            
            # 如果没有找到，使用第一个用户（通常是管理员）
            if response.get('UserList'):
                first_user = response['UserList'][0]
                print(f"🔍 使用第一个用户: {first_user.get('UserName')}")
                return first_user.get('Arn')
            
            # 如果还是没有，尝试构造标准格式
            print("⚠️ 未找到现有用户，尝试构造用户 ARN")
            return f"arn:aws:quicksight:{self.region}:{self.account_id}:user/default/admin"
            
        except Exception as e:
            print(f"⚠️ 获取用户信息失败: {str(e)}")
            # 最后的备选方案
            return f"arn:aws:quicksight:{self.region}:{self.account_id}:user/default/admin"
    
    def create_analysis(self, dataset_id):
        """创建分析"""
        print("\n📈 创建分析...")
        
        analysis_id = f"canbus-analysis-{self.timestamp}"
        
        try:
            user_arn = self._get_user_arn()
            
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
                        'Visuals': self._create_all_visuals(),
                        'Layouts': [
                            {
                                'Configuration': {
                                    'GridLayout': {
                                        'Elements': self._create_layout_elements()
                                    }
                                }
                            }
                        ]
                    }
                ]
            }
            
            response = self.quicksight.create_analysis(
                AwsAccountId=self.account_id,
                AnalysisId=analysis_id,
                Name='车辆监控综合分析',
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
    
    def _create_all_visuals(self):
        """创建所有可视化组件"""
        visuals = []
        
        # KPI 1: 活跃车辆数
        visuals.append({
            'KPIVisual': {
                'VisualId': 'active_vehicles_kpi',
                'Title': {
                    'Visibility': 'VISIBLE',
                    'Label': '活跃车辆数'
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
        })
        
        # KPI 2: 平均燃油百分比
        visuals.append({
            'KPIVisual': {
                'VisualId': 'avg_fuel_kpi',
                'Title': {
                    'Visibility': 'VISIBLE',
                    'Label': '平均燃油百分比'
                },
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
                                    'AggregationFunction': {
                                        'SimpleNumericalAggregation': 'AVERAGE'
                                    }
                                }
                            }
                        ]
                    }
                }
            }
        })
        
        # KPI 3: 平均车速
        visuals.append({
            'KPIVisual': {
                'VisualId': 'avg_speed_kpi',
                'Title': {
                    'Visibility': 'VISIBLE',
                    'Label': '平均车速 (km/h)'
                },
                'ChartConfiguration': {
                    'FieldWells': {
                        'Values': [
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
        })
        
        # 折线图: 24小时趋势
        visuals.append({
            'LineChartVisual': {
                'VisualId': 'trend_line_chart',
                'Title': {
                    'Visibility': 'VISIBLE',
                    'Label': '24小时燃油和速度趋势'
                },
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
                                        'AggregationFunction': {
                                            'SimpleNumericalAggregation': 'AVERAGE'
                                        }
                                    }
                                },
                                {
                                    'NumericalMeasureField': {
                                        'FieldId': 'avg_speed_trend',
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
        })
        
        # 柱状图: 续航里程对比
        visuals.append({
            'BarChartVisual': {
                'VisualId': 'mileage_bar_chart',
                'Title': {
                    'Visibility': 'VISIBLE',
                    'Label': 'CLTC vs WLTC 续航对比'
                },
                'ChartConfiguration': {
                    'FieldWells': {
                        'BarChartAggregatedFieldWells': {
                            'Category': [
                                {
                                    'CategoricalDimensionField': {
                                        'FieldId': 'vin_category',
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
                                        'FieldId': 'cltc_mileage',
                                        'Column': {
                                            'DataSetIdentifier': 'canbus_data',
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
                                            'DataSetIdentifier': 'canbus_data',
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
        })
        
        # 饼图: 驾驶模式分布
        visuals.append({
            'PieChartVisual': {
                'VisualId': 'driving_mode_pie',
                'Title': {
                    'Visibility': 'VISIBLE',
                    'Label': '驾驶模式分布'
                },
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
                                        'AggregationFunction': {
                                            'SimpleNumericalAggregation': 'COUNT'
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        })
        
        # 表格: 车辆详细状态
        visuals.append({
            'TableVisual': {
                'VisualId': 'vehicle_status_table',
                'Title': {
                    'Visibility': 'VISIBLE',
                    'Label': '车辆详细状态'
                },
                'ChartConfiguration': {
                    'FieldWells': {
                        'TableAggregatedFieldWells': {
                            'GroupBy': [
                                {
                                    'CategoricalDimensionField': {
                                        'FieldId': 'vin_table',
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
                                        'FieldId': 'latest_fuel',
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
                                        'FieldId': 'latest_speed',
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
                                        'FieldId': 'charging_time',
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
        })
        
        return visuals
    
    def _create_layout_elements(self):
        """创建布局元素"""
        elements = []
        
        # KPI 卡片布局 (第一行)
        kpi_visuals = ['active_vehicles_kpi', 'avg_fuel_kpi', 'avg_speed_kpi']
        for i, visual_id in enumerate(kpi_visuals):
            elements.append({
                'ElementId': f'element_{visual_id}',
                'ElementType': 'VISUAL',
                'GridLayoutConfiguration': {
                    'ColumnIndex': i * 4,
                    'ColumnSpan': 4,
                    'RowIndex': 0,
                    'RowSpan': 6
                }
            })
        
        # 趋势图 (第二行左侧)
        elements.append({
            'ElementId': 'element_trend_line_chart',
            'ElementType': 'VISUAL',
            'GridLayoutConfiguration': {
                'ColumnIndex': 0,
                'ColumnSpan': 8,
                'RowIndex': 6,
                'RowSpan': 8
            }
        })
        
        # 柱状图 (第二行右侧)
        elements.append({
            'ElementId': 'element_mileage_bar_chart',
            'ElementType': 'VISUAL',
            'GridLayoutConfiguration': {
                'ColumnIndex': 8,
                'ColumnSpan': 4,
                'RowIndex': 6,
                'RowSpan': 8
            }
        })
        
        return elements
    
    def create_dashboard(self, analysis_id, dataset_id):
        """从分析创建看板"""
        print("\n🎯 创建看板...")
        
        dashboard_id = f"canbus-dashboard-{self.timestamp}"
        
        try:
            user_arn = self._get_user_arn()
            
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
    
    def get_dashboard_url(self, dashboard_id):
        """获取看板访问 URL"""
        try:
            # 构建 QuickSight 控制台 URL
            console_url = f"https://{self.region}.quicksight.aws.amazon.com/sn/dashboards/{dashboard_id}"
            return console_url
        except Exception as e:
            print(f"❌ 获取看板 URL 失败: {str(e)}")
            return None
    
    def create_complete_dashboard(self):
        """创建完整的看板 - 主流程"""
        print("🚀 开始创建完整的车辆监控看板...")
        
        # 步骤 1: 检查 QuickSight 订阅
        if not self.check_quicksight_subscription():
            return False
        
        # 步骤 2: 创建数据源
        data_source_id = self.create_data_source()
        if not data_source_id:
            return False
        
        # 等待数据源创建完成
        print("⏳ 等待数据源初始化...")
        time.sleep(10)
        
        # 步骤 3: 创建数据集
        dataset_id = self.create_dataset(data_source_id)
        if not dataset_id:
            return False
        
        # 等待数据集创建完成
        print("⏳ 等待数据集初始化...")
        time.sleep(15)
        
        # 步骤 4: 创建分析
        analysis_id = self.create_analysis(dataset_id)
        if not analysis_id:
            return False
        
        # 等待分析创建完成
        print("⏳ 等待分析初始化...")
        time.sleep(20)
        
        # 步骤 5: 创建看板
        dashboard_id = self.create_dashboard(analysis_id, dataset_id)
        if not dashboard_id:
            return False
        
        # 步骤 6: 获取访问链接
        dashboard_url = self.get_dashboard_url(dashboard_id)
        
        # 显示结果
        self._show_results(data_source_id, dataset_id, analysis_id, dashboard_id, dashboard_url)
        
        return True
    
    def _show_results(self, data_source_id, dataset_id, analysis_id, dashboard_id, dashboard_url):
        """显示创建结果"""
        print("\n" + "="*60)
        print("🎉 车辆监控看板创建完成！")
        print("="*60)
        
        print(f"📊 数据源 ID: {data_source_id}")
        print(f"📈 数据集 ID: {dataset_id}")
        print(f"🔍 分析 ID: {analysis_id}")
        print(f"🎯 看板 ID: {dashboard_id}")
        
        if dashboard_url:
            print(f"🔗 看板访问链接: {dashboard_url}")
        
        print("\n📝 看板包含以下组件:")
        print("• KPI 卡片: 活跃车辆数、平均燃油百分比、平均车速")
        print("• 折线图: 24小时燃油和速度趋势")
        print("• 柱状图: CLTC vs WLTC 续航对比")
        print("• 饼图: 驾驶模式分布")
        print("• 散点图: 速度与燃油关系")
        print("• 表格: 车辆详细状态")
        
        print("\n🔧 后续步骤:")
        print("1. 登录 QuickSight 控制台查看看板")
        print("2. 根据需要调整可视化样式和布局")
        print("3. 设置数据刷新频率")
        print("4. 为团队成员配置访问权限")
        print("5. 设置告警和通知")
        
        print("\n💡 提示:")
        print("• 如果看板显示无数据，请等待数据处理完成")
        print("• 可以在 QuickSight 中进一步自定义看板")
        print("• 支持导出为 PDF 或图片格式")
        
        print("="*60)

def main():
    """主函数"""
    print("🚗 QuickSight 车辆监控看板一键创建工具")
    print("="*50)
    
    # 获取 AWS 区域
    try:
        region = boto3.Session().region_name or 'us-east-1'
    except:
        region = 'us-east-1'
    
    print(f"🌍 使用 AWS 区域: {region}")
    
    # 确认开始
    response = input("\n是否开始创建看板？(y/N): ").strip().lower()
    if response != 'y':
        print("❌ 操作已取消")
        return
    
    # 创建看板
    creator = QuickSightDashboardCreator(region=region)
    
    try:
        success = creator.create_complete_dashboard()
        if success:
            print("\n🎊 看板创建成功！")
        else:
            print("\n❌ 看板创建失败，请检查错误信息")
            sys.exit(1)
    except KeyboardInterrupt:
        print("\n⚠️ 操作被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 发生未预期的错误: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
