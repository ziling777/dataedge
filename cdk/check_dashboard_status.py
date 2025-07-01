#!/usr/bin/env python3
"""
检查 QuickSight 看板状态的脚本
"""

import boto3
import json

def check_dashboard_status():
    quicksight = boto3.client('quicksight', region_name='us-west-2')
    account_id = boto3.client('sts').get_caller_identity()['Account']
    
    print("🔍 检查 QuickSight 资源状态...")
    print(f"📍 AWS 账户: {account_id}")
    print(f"🌍 区域: us-west-2")
    
    # 检查数据源
    print("\n📊 检查数据源...")
    try:
        data_sources = quicksight.list_data_sources(AwsAccountId=account_id)
        print(f"找到 {len(data_sources.get('DataSources', []))} 个数据源:")
        for ds in data_sources.get('DataSources', []):
            print(f"  • {ds.get('Name')} (ID: {ds.get('DataSourceId')}) - 状态: {ds.get('Status')}")
    except Exception as e:
        print(f"❌ 获取数据源失败: {e}")
    
    # 检查数据集
    print("\n📈 检查数据集...")
    try:
        datasets = quicksight.list_data_sets(AwsAccountId=account_id)
        print(f"找到 {len(datasets.get('DataSetSummaries', []))} 个数据集:")
        for ds in datasets.get('DataSetSummaries', []):
            print(f"  • {ds.get('Name')} (ID: {ds.get('DataSetId')}) - 导入模式: {ds.get('ImportMode')}")
    except Exception as e:
        print(f"❌ 获取数据集失败: {e}")
    
    # 检查看板
    print("\n🎯 检查看板...")
    try:
        dashboards = quicksight.list_dashboards(AwsAccountId=account_id)
        print(f"找到 {len(dashboards.get('DashboardSummaryList', []))} 个看板:")
        for db in dashboards.get('DashboardSummaryList', []):
            dashboard_id = db.get('DashboardId')
            print(f"  • {db.get('Name')} (ID: {dashboard_id})")
            print(f"    创建时间: {db.get('CreatedTime')}")
            print(f"    最后更新: {db.get('LastUpdatedTime')}")
            print(f"    版本号: {db.get('VersionNumber')}")
            print(f"    访问链接: https://us-west-2.quicksight.aws.amazon.com/sn/dashboards/{dashboard_id}")
            
            # 获取看板详细信息
            try:
                dashboard_detail = quicksight.describe_dashboard(
                    AwsAccountId=account_id,
                    DashboardId=dashboard_id
                )
                status = dashboard_detail.get('Dashboard', {}).get('Version', {}).get('Status')
                print(f"    详细状态: {status}")
                
                # 检查是否已发布
                if status == 'CREATION_SUCCESSFUL':
                    print(f"    ✅ 看板创建成功，可以访问")
                elif status == 'CREATION_IN_PROGRESS':
                    print(f"    ⏳ 看板正在创建中，请稍等...")
                elif status == 'CREATION_FAILED':
                    print(f"    ❌ 看板创建失败")
                    errors = dashboard_detail.get('Dashboard', {}).get('Version', {}).get('Errors', [])
                    for error in errors:
                        print(f"      错误: {error}")
                
            except Exception as detail_error:
                print(f"    ⚠️ 无法获取详细状态: {detail_error}")
            
            print()
    except Exception as e:
        print(f"❌ 获取看板失败: {e}")
    
    # 检查分析
    print("\n📋 检查分析...")
    try:
        analyses = quicksight.list_analyses(AwsAccountId=account_id)
        print(f"找到 {len(analyses.get('AnalysisSummaryList', []))} 个分析:")
        for analysis in analyses.get('AnalysisSummaryList', []):
            print(f"  • {analysis.get('Name')} (ID: {analysis.get('AnalysisId')})")
            print(f"    状态: {analysis.get('Status')}")
    except Exception as e:
        print(f"❌ 获取分析失败: {e}")

if __name__ == "__main__":
    check_dashboard_status()
