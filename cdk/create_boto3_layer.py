import os
import subprocess
import sys

def create_boto3_layer():
    """创建 boto3 Lambda Layer"""
    print("开始创建 boto3 Lambda Layer...")
    
    # 创建目录结构
    layer_dir = "lambda_layers/boto3/python"
    os.makedirs(layer_dir, exist_ok=True)
    
    # 安装 boto3
    print(f"在 {layer_dir} 中安装最新版本的 boto3...")
    
    # 使用 pip 安装 boto3 到目标目录
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", 
            "boto3", "--target", layer_dir, "--upgrade"
        ])
        print("boto3 安装成功！")
    except subprocess.CalledProcessError as e:
        print(f"安装 boto3 时出错: {e}")
        sys.exit(1)
    
    print("boto3 Lambda Layer 创建完成！")

if __name__ == "__main__":
    create_boto3_layer() 