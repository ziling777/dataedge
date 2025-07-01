# S3 Tables CDK Project

这个 CDK 项目用于部署 S3 Tables 相关的基础设施和 QuickSight 车辆监控看板。

## 🚀 一键部署（推荐）

### 快速开始
```bash
# 1. 进入项目目录
cd /Users/xiezili/Downloads/greptime/cdk

# 2. 执行一键部署
chmod +x one_click_deploy.sh
./one_click_deploy.sh
```

一键部署脚本将自动完成：
- ✅ 检查和安装所有依赖
- 🏗️ 部署 S3 Tables 基础设施
- 📊 创建 QuickSight 数据源和数据集
- 🎯 自动生成车辆监控看板
- 🔧 配置所有必要的权限

### 部署时间
- 总耗时：约 15-20 分钟
- 基础设施部署：10-15 分钟
- QuickSight 看板创建：2-5 分钟

## 📋 前置条件

### 必需工具
- Python 3.8 或更高版本
- Node.js 和 npm
- AWS CLI (已配置凭证)

### AWS 服务要求
- AWS 账户具有管理员权限
- QuickSight 服务已启用（脚本会引导您完成）
- 确保在支持 S3 Tables 的区域（如 us-east-1）

### 快速检查
```bash
# 检查 Python
python3 --version

# 检查 Node.js
node --version

# 检查 AWS CLI
aws --version
aws sts get-caller-identity
```

## 🎯 部署后的资源

### 基础设施组件
- **S3 存储桶**: 存储原始和处理后的数据
- **Lambda 函数**: 数据处理和转换
- **EMR Serverless**: 大数据处理作业
- **S3 Tables**: Iceberg 格式的分析表
- **VPC 和网络**: 安全的网络环境

### QuickSight 看板
- **数据源**: Athena 连接到 S3 Tables
- **数据集**: CAN 总线数据集
- **看板组件**:
  - KPI 卡片：活跃车辆数、平均燃油、平均车速
  - 折线图：24小时趋势分析
  - 饼图：驾驶模式分布
  - 散点图：速度与燃油关系
  - 表格：车辆详细状态

## 📊 看板功能

### 实时监控
- 车辆实时状态
- 燃油水平监控
- 速度和性能指标
- 电池状态告警

### 趋势分析
- 历史数据趋势
- 驾驶行为分析
- 能源效率评估
- 异常检测和告警

### 交互功能
- 时间范围过滤
- 车辆 ID 筛选
- 钻取分析
- 数据导出

## 🛠️ 手动部署（高级用户）

如果您需要自定义部署或遇到问题，可以分步执行：

### 1. 环境准备
```bash
# 创建并激活虚拟环境
python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux
# .venv\Scripts\activate   # Windows

# 安装依赖
pip install --upgrade aws-cdk-lib
pip install -r requirements.txt
```

### 2. CDK 部署
```bash
# CDK Bootstrap（首次部署）
cdk bootstrap

# 查看变更
cdk diff

# 部署基础设施
cdk deploy --all
```

### 3. QuickSight 配置
```bash
# 创建 QuickSight 看板
python3 create_quicksight_dashboard.py
```

## 📁 项目结构

```
.
├── lambda/                     # Lambda 函数代码
├── lambda_layers/             # Lambda layers
├── emr_job/                   # EMR Spark 作业
├── s3table_cdk/              # CDK 应用代码
├── tests/                    # 测试代码
├── one_click_deploy.sh       # 一键部署脚本
├── deploy.sh                 # 基础设施部署脚本
├── create_quicksight_dashboard.py  # QuickSight 看板创建脚本
└── README.md                 # 本文件
```

## 🔧 故障排除

### 常见问题

**1. CDK CLI 版本不匹配**
```bash
npm install -g aws-cdk@latest
```

**2. QuickSight 未启用**
- 访问 AWS 控制台
- 搜索 QuickSight 服务
- 完成服务注册

**3. 权限问题**
- 确保 AWS 凭证具有管理员权限
- 检查 Lake Formation 权限设置

**4. 数据未显示**
- 等待 10-15 分钟让数据处理完成
- 检查 Lambda 和 EMR 作业状态
- 查看 CloudWatch 日志

### 日志查看
```bash
# 查看 CDK 部署日志
cdk deploy --verbose

# 查看 Lambda 日志
aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/"

# 查看 EMR 日志
# 在 S3 存储桶的 logs/ 目录中
```

### 清理资源
```bash
# 销毁所有资源
cdk destroy --all

# 清理 QuickSight 资源（需要手动在控制台操作）
```

## 📞 支持和文档

### AWS 文档
- [S3 Tables 用户指南](https://docs.aws.amazon.com/s3/latest/userguide/s3-tables.html)
- [QuickSight 用户指南](https://docs.aws.amazon.com/quicksight/)
- [CDK 开发者指南](https://docs.aws.amazon.com/cdk/)

### 社区资源
- [AWS CDK GitHub](https://github.com/aws/aws-cdk)
- [QuickSight 社区](https://repost.aws/tags/TA4IvCeRdxT_2-bEPKZGzg/amazon-quick-sight)

## 📄 许可证

This project is licensed under the MIT License - see the LICENSE file for details

---

## 🎉 快速体验

想要快速体验完整的车辆监控系统？只需运行：

```bash
./one_click_deploy.sh
```

15-20 分钟后，您将拥有一个功能完整的车辆数据分析平台！

