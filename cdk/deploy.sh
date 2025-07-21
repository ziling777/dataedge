#!/bin/bash

# 一键部署脚本 - S3 Tables CDK 项目
# 作者: Amazon Q
# 用途: 自动化部署整个 S3 Tables 基础设施

set -e  # 遇到错误立即退出

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日志函数
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查必要的工具
check_prerequisites() {
    log_info "检查部署前置条件..."
    
    # 检查 Python
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 未安装，请先安装 Python 3.8+"
        exit 1
    fi
    
    # 检查 Node.js 和 npm
    if ! command -v npm &> /dev/null; then
        log_error "npm 未安装，请先安装 Node.js 和 npm"
        exit 1
    fi
    
    # 检查 AWS CLI
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI 未安装，请先安装并配置 AWS CLI"
        exit 1
    fi
    
    # 检查 AWS 凭证
    if ! aws sts get-caller-identity &> /dev/null; then
        log_error "AWS 凭证未配置或已过期，请运行 'aws configure'"
        exit 1
    fi
    
    log_success "前置条件检查通过"
}

# 安装和更新 CDK CLI
install_cdk_cli() {
    log_info "检查并更新 CDK CLI..."
    
    if ! command -v cdk &> /dev/null; then
        log_info "安装 AWS CDK CLI..."
        npm install -g aws-cdk
    else
        log_info "更新 AWS CDK CLI 到最新版本..."
        npm install -g aws-cdk@latest
    fi
    
    log_success "CDK CLI 已就绪: $(cdk --version)"
}

# 设置 Python 虚拟环境
setup_python_env() {
    log_info "设置 Python 虚拟环境..."
    
    # 创建虚拟环境（如果不存在）
    if [ ! -d ".venv" ]; then
        log_info "创建 Python 虚拟环境..."
        python3 -m venv .venv
    fi
    
    # 激活虚拟环境
    log_info "激活虚拟环境..."
    source .venv/bin/activate
    
    # 升级 pip
    log_info "升级 pip..."
    pip install --upgrade pip
    
    # 安装依赖
    log_info "安装 Python 依赖..."
    pip install --upgrade aws-cdk-lib
    if [ -f "requirements.txt" ]; then
        pip install -r requirements.txt
    fi
    if [ -f "requirements-dev.txt" ]; then
        pip install -r requirements-dev.txt
    fi
    
    log_success "Python 环境设置完成"
}

# CDK Bootstrap
bootstrap_cdk() {
    log_info "执行 CDK Bootstrap..."
    
    # 获取当前 AWS 账户和区域
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    REGION=$(aws configure get region)
    
    if [ -z "$REGION" ]; then
        REGION="us-east-1"
        log_warning "未设置默认区域，使用 us-east-1"
    fi
    
    log_info "AWS 账户: $ACCOUNT_ID"
    log_info "AWS 区域: $REGION"
    
    # 检查是否已经 bootstrap
    if aws cloudformation describe-stacks --stack-name CDKToolkit --region $REGION &> /dev/null; then
        log_info "CDK 已经 bootstrap，跳过..."
    else
        log_info "执行 CDK bootstrap..."
        cdk bootstrap aws://$ACCOUNT_ID/$REGION
        log_success "CDK bootstrap 完成"
    fi
}

# 构建 Lambda Layers
build_lambda_layers() {
    log_info "构建 Lambda Layers..."
    
    if [ -d "lambda_layers" ]; then
        cd lambda_layers
        
        # 如果存在构建脚本，执行它
        if [ -f "build.sh" ]; then
            log_info "执行 Lambda Layer 构建脚本..."
            chmod +x build.sh
            ./build.sh
        else
            log_info "创建 Lambda Layer 目录结构..."
            mkdir -p python/lib/python3.13/site-packages
            
            # 安装必要的包（如果有 requirements.txt）
            if [ -f "requirements.txt" ]; then
                pip install -r requirements.txt -t python/lib/python3.13/site-packages/
            fi
        fi
        
        cd ..
        log_success "Lambda Layers 构建完成"
    else
        log_warning "未找到 lambda_layers 目录，跳过..."
    fi
}

# 验证 CDK 应用
validate_cdk_app() {
    log_info "验证 CDK 应用..."
    
    # 检查 CDK 应用语法
    if cdk synth > /dev/null 2>&1; then
        log_success "CDK 应用语法验证通过"
    else
        log_error "CDK 应用语法验证失败"
        cdk synth
        exit 1
    fi
}

# 显示部署计划
show_deployment_plan() {
    log_info "显示部署计划..."
    
    echo "=========================================="
    echo "部署计划概览:"
    echo "=========================================="
    
    # 显示将要创建的资源
    cdk diff --no-color | head -50
    
    echo "=========================================="
    
    # 询问用户确认
    read -p "是否继续部署？(y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_info "部署已取消"
        exit 0
    fi
}

# 执行部署
deploy_stack() {
    log_info "开始部署 CDK Stack..."
    
    # 部署所有 stacks
    cdk deploy --all --require-approval never --progress events
    
    if [ $? -eq 0 ]; then
        log_success "CDK Stack 部署成功！"
    else
        log_error "CDK Stack 部署失败"
        exit 1
    fi
}

# 显示部署结果
show_deployment_results() {
    log_info "获取部署结果..."
    
    echo "=========================================="
    echo "部署完成！以下是重要的输出信息："
    echo "=========================================="
    
    # 获取 CloudFormation 输出
    STACK_NAME=$(cdk list | head -1)
    if [ ! -z "$STACK_NAME" ]; then
        aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
            --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' \
            --output table
    fi
    
    echo "=========================================="
    echo "后续步骤："
    echo "1. 检查 S3 存储桶中的数据"
    echo "2. 验证 Lambda 函数运行状态"
    echo "3. 查看 EMR Serverless 应用状态"
    echo "=========================================="
}

# 清理函数（可选）
cleanup_on_failure() {
    log_warning "检测到部署失败，是否需要清理资源？"
    read -p "是否销毁已创建的资源？(y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "正在清理资源..."
        cdk destroy --all --force
        log_success "资源清理完成"
    fi
}

# 主函数
main() {
    echo "=========================================="
    echo "S3 Tables CDK 项目一键部署脚本"
    echo "=========================================="
    
    # 设置错误处理
    trap cleanup_on_failure ERR
    
    # 执行部署步骤
    check_prerequisites
    install_cdk_cli
    setup_python_env
    bootstrap_cdk
    build_lambda_layers
    validate_cdk_app
    show_deployment_plan
    deploy_stack
    show_deployment_results
    
    log_success "🎉 部署完成！您的 S3 Tables 基础设施已就绪。"
}

# 执行主函数
main "$@"
