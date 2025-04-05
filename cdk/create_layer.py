import os
import subprocess
import sys

def create_py7zr_layer():
    """创建 py7zr Lambda Layer"""
    print("开始创建 py7zr Lambda Layer...")
    
    # 创建目录结构
    layer_dir = "lambda_layers/python"
    os.makedirs(layer_dir, exist_ok=True)
    
    # 安装 py7zr
    print(f"在 {layer_dir} 中安装最新版本的 py7zr...")
    
    # 使用 pip 安装 py7zr 到目标目录
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", 
            "py7zr", "--target", layer_dir, "--upgrade"
        ])
        print("py7zr 安装成功！")
    except subprocess.CalledProcessError as e:
        print(f"安装 py7zr 时出错: {e}")
        sys.exit(1)
    
    print("py7zr Lambda Layer 创建完成！")

if __name__ == "__main__":
    create_py7zr_layer() 