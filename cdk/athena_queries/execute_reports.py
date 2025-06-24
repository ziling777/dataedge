#!/usr/bin/env python3
"""
执行 Athena 报表查询的脚本
使用方法: python execute_reports.py [report_name]
"""

import boto3
import time
import sys
import os
from pathlib import Path

# Athena 配置
ATHENA_DATABASE = 's3tablescatalog'
ATHENA_WORKGROUP = 'primary'
S3_OUTPUT_LOCATION = 's3://s3tablecdkstack-telematicsdatauploadbucketb14f9ff3-8u9khojxu5sa/athena-results/'

# 报表配置
REPORTS = {
    'efficiency': {
        'name': '车辆能耗效率分析报表',
        'file': 'vehicle_efficiency_report.sql'
    },
    'behavior': {
        'name': '驾驶行为分析报表', 
        'file': 'driving_behavior_analysis.sql'
    },
    'health': {
        'name': '车辆健康状态监控报表',
        'file': 'vehicle_health_monitoring.sql'
    },
    'charging': {
        'name': '充电行为优化建议报表',
        'file': 'charging_optimization_report.sql'
    },
    'dashboard': {
        'name': '实时运营监控仪表板',
        'file': 'realtime_operations_dashboard.sql'
    },
    'comparison': {
        'name': '车辆性能对比分析报表',
        'file': 'vehicle_performance_comparison.sql'
    }
}

def read_sql_file(file_path):
    """读取SQL文件内容"""
    with open(file_path, 'r', encoding='utf-8') as f:
        return f.read()

def execute_athena_query(athena_client, query, query_name):
    """执行Athena查询"""
    print(f"正在执行查询: {query_name}")
    
    try:
        # 启动查询
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': ATHENA_DATABASE},
            ResultConfiguration={'OutputLocation': S3_OUTPUT_LOCATION},
            WorkGroup=ATHENA_WORKGROUP
        )
        
        query_execution_id = response['QueryExecutionId']
        print(f"查询ID: {query_execution_id}")
        
        # 等待查询完成
        while True:
            response = athena_client.get_query_execution(
                QueryExecutionId=query_execution_id
            )
            
            status = response['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED']:
                print(f"✅ 查询成功完成!")
                result_location = response['QueryExecution']['ResultConfiguration']['OutputLocation']
                print(f"结果位置: {result_location}")
                return query_execution_id, result_location
                
            elif status in ['FAILED', 'CANCELLED']:
                error_message = response['QueryExecution']['Status'].get('StateChangeReason', '未知错误')
                print(f"❌ 查询失败: {error_message}")
                return None, None
                
            else:
                print(f"⏳ 查询状态: {status}, 等待中...")
                time.sleep(5)
                
    except Exception as e:
        print(f"❌ 执行查询时出错: {str(e)}")
        return None, None

def get_query_results(athena_client, query_execution_id, max_rows=10):
    """获取查询结果的前几行"""
    try:
        response = athena_client.get_query_results(
            QueryExecutionId=query_execution_id,
            MaxResults=max_rows
        )
        
        # 解析结果
        columns = [col['Label'] for col in response['ResultSet']['ResultSetMetadata']['ColumnInfo']]
        rows = []
        
        for row_data in response['ResultSet']['Rows'][1:]:  # 跳过标题行
            row = [field.get('VarCharValue', '') for field in row_data['Data']]
            rows.append(row)
        
        return columns, rows
        
    except Exception as e:
        print(f"❌ 获取查询结果时出错: {str(e)}")
        return None, None

def print_results(columns, rows, max_display_rows=5):
    """打印查询结果"""
    if not columns or not rows:
        print("无结果数据")
        return
    
    print(f"\n📊 查询结果预览 (显示前{min(len(rows), max_display_rows)}行):")
    print("-" * 100)
    
    # 打印列标题
    header = " | ".join([col[:15] for col in columns])
    print(header)
    print("-" * len(header))
    
    # 打印数据行
    for i, row in enumerate(rows[:max_display_rows]):
        row_str = " | ".join([str(cell)[:15] for cell in row])
        print(row_str)
    
    if len(rows) > max_display_rows:
        print(f"... 还有 {len(rows) - max_display_rows} 行数据")
    
    print("-" * 100)

def main():
    # 检查参数
    if len(sys.argv) < 2:
        print("使用方法: python execute_reports.py [report_name]")
        print("可用的报表:")
        for key, report in REPORTS.items():
            print(f"  {key}: {report['name']}")
        print("  all: 执行所有报表")
        return
    
    report_name = sys.argv[1].lower()
    
    # 初始化Athena客户端
    athena_client = boto3.client('athena', region_name='us-east-1')
    
    # 获取当前脚本目录
    script_dir = Path(__file__).parent
    
    if report_name == 'all':
        # 执行所有报表
        print("🚀 开始执行所有报表...")
        for key, report in REPORTS.items():
            print(f"\n{'='*60}")
            print(f"执行报表: {report['name']}")
            print(f"{'='*60}")
            
            sql_file = script_dir / report['file']
            if not sql_file.exists():
                print(f"❌ SQL文件不存在: {sql_file}")
                continue
            
            query = read_sql_file(sql_file)
            query_id, result_location = execute_athena_query(athena_client, query, report['name'])
            
            if query_id:
                columns, rows = get_query_results(athena_client, query_id)
                print_results(columns, rows)
            
            print(f"\n完成报表: {report['name']}")
            time.sleep(2)  # 避免请求过于频繁
            
    elif report_name in REPORTS:
        # 执行指定报表
        report = REPORTS[report_name]
        print(f"🚀 开始执行报表: {report['name']}")
        
        sql_file = script_dir / report['file']
        if not sql_file.exists():
            print(f"❌ SQL文件不存在: {sql_file}")
            return
        
        query = read_sql_file(sql_file)
        query_id, result_location = execute_athena_query(athena_client, query, report['name'])
        
        if query_id:
            columns, rows = get_query_results(athena_client, query_id)
            print_results(columns, rows)
        
    else:
        print(f"❌ 未知的报表名称: {report_name}")
        print("可用的报表:")
        for key, report in REPORTS.items():
            print(f"  {key}: {report['name']}")

if __name__ == "__main__":
    main()
