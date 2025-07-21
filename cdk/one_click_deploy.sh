#!/bin/bash

# 完整一键部署脚本 - S3 Tables
# 用途: 自动化部署整个解决方案

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
NC='\033[0m'

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

log_step() {
    echo -e "${PURPLE}[STEP]${NC} $1"
}

# 显示欢迎信息
show_welcome() {
    clear
    echo "=================================================================="
    echo "🚀 S3 Tables 车辆监控系统一键部署"
    echo "=================================================================="
    echo "此脚本将自动完成以下步骤:"
    echo "1. ✅ 部署 S3 Tables 基础设施 (CDK)"
    echo "2. 🔧 配置所有必要的权限和连接"
    echo "=================================================================="
    echo ""
    
    read -p "按 Enter 键开始部署，或 Ctrl+C 取消: "
}

# 部署基础设施
deploy_infrastructure() {
    log_step "部署 S3 Tables 基础设施..."
    
    # 确保部署脚本可执行
    chmod +x deploy.sh
    
    # 执行基础设施部署
    if ./deploy.sh; then
        log_success "基础设施部署完成"
        return 0
    else
        log_error "基础设施部署失败"
        return 1
    fi
}

# 等待数据生成
wait_for_data() {
    log_step "等待数据生成和处理..."
    
    echo "正在等待以下组件完成初始化:"
    echo "- Lambda 函数处理数据"
    echo "- EMR Serverless 作业执行"
    echo "- S3 Tables 数据写入"
    
    # 等待 5 分钟让数据处理完成
    for i in {1..30}; do
        echo -n "."
        sleep 10
    done
    echo ""
    
    log_success "数据处理等待完成"
}

# 显示部署结果和后续步骤
show_final_results() {
    log_step "收集部署结果..."
    
    echo ""
    echo "=================================================================="
    echo "🎉 部署完成！系统已就绪"
    echo "=================================================================="
    
    # 获取 CloudFormation 输出
    STACK_NAME=$(cdk list 2>/dev/null | head -1)
    if [ ! -z "$STACK_NAME" ]; then
        echo ""
        echo "📋 基础设施信息:"
        echo "------------------------------------------------------------------"
        aws cloudformation describe-stacks --stack-name "$STACK_NAME" \
            --query 'Stacks[0].Outputs[*].[OutputKey,OutputValue]' \
            --output table 2>/dev/null || echo "无法获取 CloudFormation 输出"
    fi
    
    echo ""
    echo "🔗 访问链接:"
    echo "------------------------------------------------------------------"
    REGION=$(aws configure get region || echo "us-east-1")
    ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
    
    echo "• S3 控制台: https://console.aws.amazon.com/s3/"
    echo "• CloudFormation: https://console.aws.amazon.com/cloudformation/"
    echo "• Lambda 函数: https://console.aws.amazon.com/lambda/"
    
    echo ""
    echo "📝 后续步骤:"
    echo "------------------------------------------------------------------"
    echo "1. 🔍 验证数据: 检查 S3 存储桶中是否有处理后的数据"
    
    echo ""
    echo "🛠️ 故障排除:"
    echo "------------------------------------------------------------------"
    echo "• 如果遇到权限问题，检查 IAM 角色和 Lake Formation 设置"
    echo "• 查看 CloudWatch 日志了解详细错误信息"
    
    echo ""
    echo "📞 支持:"
    echo "------------------------------------------------------------------"
    echo "• S3 Tables 文档: https://docs.aws.amazon.com/s3/latest/userguide/s3-tables.html"
    
    echo "=================================================================="
}

# 清理函数
cleanup_on_error() {
    log_error "部署过程中发生错误"
    
    read -p "是否需要清理已创建的资源？(y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "正在清理资源..."
        
        # 清理 CDK 资源
        if command -v cdk &> /dev/null; then
            cdk destroy --all --force 2>/dev/null || true
        fi
        
        log_success "资源清理完成"
    fi
}

# 主函数
main() {
    # 设置错误处理
    trap cleanup_on_error ERR
    
    # 记录开始时间
    START_TIME=$(date +%s)
    
    # 执行部署步骤
    show_welcome
    deploy_infrastructure
    wait_for_data
    
    # 计算总耗时
    END_TIME=$(date +%s)
    DURATION=$((END_TIME - START_TIME))
    MINUTES=$((DURATION / 60))
    SECONDS=$((DURATION % 60))
    
    show_final_results
    
    echo ""
    log_success "🎊 全部完成！总耗时: ${MINUTES}分${SECONDS}秒"
    echo ""
}

# 检查是否在正确的目录中
if [ ! -f "app.py" ] || [ ! -f "cdk.json" ]; then
    log_error "请在 CDK 项目根目录中运行此脚本"
    exit 1
fi

# 执行主函数
main "$@"
