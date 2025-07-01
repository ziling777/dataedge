# QuickSight 车辆监控看板一键创建工具

## 🎯 功能说明

这个 Python 脚本可以自动创建一个完整的 QuickSight 车辆监控看板，基于您的 S3 Tables 数据源。

## 📋 前置条件

### 1. AWS 服务要求
- ✅ AWS 账户已配置（具有管理员权限）
- ✅ QuickSight 服务已启用
- ✅ S3 Tables 数据已就绪（canbus01 表）
- ✅ Athena 可以查询 S3 Tables 数据

### 2. Python 环境
```bash
# 安装必要的 Python 包
pip install boto3
```

### 3. AWS 凭证配置
```bash
# 确保 AWS CLI 已配置
aws configure
# 或者设置环境变量
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

## 🚀 使用方法

### 一键运行
```bash
# 进入项目目录
cd /Users/xiezili/Downloads/greptime/cdk

# 运行脚本
python3 quicksight_dashboard_creator.py
```

### 交互式运行
脚本会引导您完成以下步骤：
1. 检查 QuickSight 订阅状态
2. 创建 Athena 数据源
3. 创建数据集（连接到 S3 Tables）
4. 创建分析（包含多个可视化组件）
5. 发布看板

## 📊 看板内容

创建的看板包含以下组件：

### KPI 卡片
- **活跃车辆数**: 统计不重复的车辆 ID 数量
- **平均燃油百分比**: 所有车辆的平均燃油水平
- **平均车速**: 所有车辆的平均行驶速度

### 图表组件
- **折线图**: 24小时燃油和速度趋势分析
- **柱状图**: CLTC vs WLTC 续航里程对比
- **饼图**: 驾驶模式分布统计
- **表格**: 车辆详细状态信息

## 🔧 自定义配置

### 修改数据源
如果您的数据源不同，请修改脚本中的以下参数：
```python
# 在 create_dataset 方法中修改
'Catalog': 's3tablescatalog/caredgedemo',  # 您的 catalog 名称
'Schema': 'greptime',                      # 您的 schema 名称
'Name': 'canbus01',                        # 您的表名称
```

### 修改区域
```python
# 在主函数中修改默认区域
creator = QuickSightDashboardCreator(region='your-region')
```

## 📝 运行示例

```bash
$ python3 quicksight_dashboard_creator.py

🚗 QuickSight 车辆监控看板一键创建工具
==================================================
🌍 使用 AWS 区域: us-east-1

是否开始创建看板？(y/N): y

🚀 初始化 QuickSight 看板创建器
📍 AWS 账户: 123456789012
🌍 区域: us-east-1

🔍 检查 QuickSight 订阅状态...
✅ QuickSight 已启用: MyQuickSightAccount

🔗 创建 Athena 数据源...
✅ 数据源创建成功: s3tables-canbus-1672531200

📊 创建数据集...
✅ 数据集创建成功: canbus-dataset-1672531200

📈 创建分析...
✅ 分析创建成功: canbus-analysis-1672531200

🎯 创建看板...
✅ 看板创建成功: canbus-dashboard-1672531200

============================================================
🎉 车辆监控看板创建完成！
============================================================
📊 数据源 ID: s3tables-canbus-1672531200
📈 数据集 ID: canbus-dataset-1672531200
🔍 分析 ID: canbus-analysis-1672531200
🎯 看板 ID: canbus-dashboard-1672531200
🔗 看板访问链接: https://us-east-1.quicksight.aws.amazon.com/sn/dashboards/canbus-dashboard-1672531200
```

## 🛠️ 故障排除

### 常见问题

**1. QuickSight 未启用**
```
❌ QuickSight 未启用: User: arn:aws:iam::123456789012:user/username is not authorized to perform: quicksight:DescribeAccountSettings
```
**解决方案**: 在 AWS 控制台中启用 QuickSight 服务

**2. 权限不足**
```
❌ 数据源创建失败: AccessDenied
```
**解决方案**: 确保 AWS 凭证具有 QuickSight 和 Athena 的完整权限

**3. 数据表不存在**
```
❌ 数据集创建失败: Table 'canbus01' doesn't exist
```
**解决方案**: 确保 S3 Tables 中的数据表已创建并包含数据

**4. 网络连接问题**
```
❌ 发生未预期的错误: EndpointConnectionError
```
**解决方案**: 检查网络连接和 AWS 区域设置

### 调试模式
如需查看详细错误信息，可以修改脚本中的异常处理：
```python
except Exception as e:
    print(f"❌ 详细错误: {str(e)}")
    import traceback
    traceback.print_exc()  # 添加这行查看完整错误堆栈
```

## 📞 支持

如果遇到问题，请检查：
1. AWS 凭证配置是否正确
2. QuickSight 服务是否已启用
3. S3 Tables 数据是否可通过 Athena 查询
4. 网络连接是否正常

## 🎉 完成后的步骤

看板创建成功后，您可以：
1. 访问提供的 QuickSight 链接查看看板
2. 在 QuickSight 中进一步自定义样式和布局
3. 设置数据刷新频率
4. 为团队成员配置访问权限
5. 设置告警和通知
