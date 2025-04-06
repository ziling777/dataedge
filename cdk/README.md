# S3 Tables CDK Project

这个 CDK 项目用于部署 S3 Tables 相关的基础设施。

## 前置条件

- Python 3.8 或更高版本
- AWS CDK CLI (`npm install -g aws-cdk`)
- AWS 凭证已配置

## 快速开始

1. **创建并激活虚拟环境**

```bash
# 创建虚拟环境
python -m venv .venv

# 激活虚拟环境
# Windows
.venv\Scripts\activate
# macOS/Linux
source .venv/bin/activate
```

2. **安装依赖**

```bash
pip install --upgrade aws-cdk-lib
pip install -r requirements.txt
```

3. **创建 Lambda Layer**

```bash
rm -rf lambda_layers
python create_layer.py
```

4. **部署 CDK Stack**

```bash
# 首次部署前需要引导
cdk bootstrap

# 查看变更
cdk diff

# 部署
cdk deploy
```

## 项目结构

```
.
├── lambda/                  # Lambda 函数代码
├── lambda_layers/          # Lambda layers
├── s3table_cdk/           # CDK 应用代码
└── tests/                 # 测试代码
```

## 常见问题

1. **CDK CLI 版本不匹配**
   - 更新 CDK CLI: `npm install -g aws-cdk`
   - 或修改 requirements.txt 中的 aws-cdk-lib 版本

2. **Lambda Layer 创建失败**
   - 确保有网络连接
   - 检查 Python 版本兼容性

## 注意事项

- 确保 `.gitignore` 正确配置，避免提交不必要的文件
- 部署前请检查 AWS 配置是否正确
- 使用 `cdk destroy` 清理资源

## 许可证

This project is licensed under the MIT License - see the LICENSE file for details

## 常见问题

如果见到下面的问题：
```
This CDK CLI is not compatible with the CDK library used by your application. Please upgrade the CLI to the latest version.
```
请升级 CDK CLI：
```
npm install -g aws-cdk
```

