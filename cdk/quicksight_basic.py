#!/usr/bin/env python3
"""
最基础的 QuickSight 看板创建脚本
只创建数据源和数据集，然后提供手动创建看板的指导
"""

import boto3
import time

def main():
    print("🚗 QuickSight 基础设置工具")
    print("="*40)
    
    # 初始化客户端
    quicksight = boto3.client('quicksight', region_name='us-west-2')
    account_id = boto3.client('sts').get_caller_identity()['Account']
    timestamp = str(int(time.time()))
    
    print(f"📍 AWS 账户: {account_id}")
    print(f"🌍 区域: us-west-2")
    
    # 获取用户信息
    try:
        users_response = quicksight.list_users(
            AwsAccountId=account_id,
            Namespace='default'
        )
        user_arn = users_response['UserList'][0]['Arn']
        user_name = users_response['UserList'][0]['UserName']
        print(f"👤 用户: {user_name}")
    except Exception as e:
        print(f"❌ 获取用户信息失败: {e}")
        return
    
    # 创建数据源
    print("\n🔗 创建 Athena 数据源...")
    data_source_id = f"s3tables-canbus-{timestamp}"
    
    try:
        quicksight.create_data_source(
            AwsAccountId=account_id,
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
    except Exception as e:
        print(f"❌ 数据源创建失败: {e}")
        return
    
    time.sleep(5)
    
    # 创建数据集
    print("\n📊 创建数据集...")
    dataset_id = f"canbus-dataset-{timestamp}"
    
    try:
        quicksight.create_data_set(
            AwsAccountId=account_id,
            DataSetId=dataset_id,
            Name='CAN Bus Data',
            PhysicalTableMap={
                'canbus01': {
                    'RelationalTable': {
                        'DataSourceArn': f'arn:aws:quicksight:us-west-2:{account_id}:datasource/{data_source_id}',
                        'Catalog': 's3tablescatalog/caredgedemo',
                        'Schema': 'greptime',
                        'Name': 'canbus01',
                        'InputColumns': [
                            {'Name': 'vin_id', 'Type': 'STRING'},
                            {'Name': 'fuel_percentage', 'Type': 'INTEGER'},
                            {'Name': 'display_speed', 'Type': 'DECIMAL'},
                            {'Name': 'charging_time_remain_minute', 'Type': 'INTEGER'},
                            {'Name': 'clean_mode', 'Type': 'INTEGER'},
                            {'Name': 'road_mode', 'Type': 'INTEGER'},
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
    except Exception as e:
        print(f"❌ 数据集创建失败: {e}")
        return
    
    # 提供手动创建看板的指导
    print("\n" + "="*60)
    print("🎉 基础设置完成！")
    print("="*60)
    print(f"📊 数据源 ID: {data_source_id}")
    print(f"📈 数据集 ID: {dataset_id}")
    print(f"🔗 QuickSight 控制台: https://us-west-2.quicksight.aws.amazon.com/")
    
    print("\n📝 手动创建看板步骤:")
    print("1. 访问上面的 QuickSight 控制台链接")
    print("2. 点击左侧菜单的 'Analyses'")
    print("3. 点击 'New analysis'")
    print(f"4. 选择数据集: 'CAN Bus Data' (ID: {dataset_id})")
    print("5. 点击 'Create analysis'")
    print("6. 在分析中添加以下可视化:")
    print("   • KPI: 活跃车辆数 (vin_id 的 Distinct Count)")
    print("   • KPI: 平均燃油百分比 (fuel_percentage 的 Average)")
    print("   • KPI: 平均车速 (display_speed 的 Average)")
    print("   • 表格: 显示 vin_id, fuel_percentage, display_speed")
    print("   • 折线图: 时间 (ts) vs 燃油百分比")
    print("7. 完成后点击 'Share' -> 'Publish dashboard'")
    
    print("\n💡 提示:")
    print("• 如果数据集显示无数据，请确保 S3 Tables 中有数据")
    print("• 可以在 Athena 中先测试查询: SELECT * FROM \"s3tablescatalog/caredgedemo\".\"greptime\".\"canbus01\" LIMIT 10")
    print("="*60)

if __name__ == "__main__":
    main()
