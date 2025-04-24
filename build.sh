#!/bin/bash
# 设置脚本在遇到错误时退出
set -e

echo "Activating Python virtual environment..."
# 激活 Python 虚拟环境 (Linux/macOS)
source .venv/bin/activate

echo "Setting TA-Lib environment variables for build..."
# 获取脚本所在的目录
current_path=$(dirname "$0")
# 将相对路径转换为绝对路径 (更健壮)
current_path=$(cd "$current_path" && pwd)

echo "Current path: $current_path"
export TA_INCLUDE_PATH="$current_path/ta-lib/include"
export TA_LIBRARY_PATH="$current_path/ta-lib/lib"
echo "TA_INCLUDE_PATH set to: $TA_INCLUDE_PATH"
echo "TA_LIBRARY_PATH set to: $TA_LIBRARY_PATH"


echo "Running hatch build..."
# 运行 hatch build，并将所有传递给脚本的参数传递给 hatch
hatch build "$@"

echo "Cleaning up TA-Lib environment variables..."
# 清理环境变量
unset TA_INCLUDE_PATH
unset TA_LIBRARY_PATH

echo "Build process finished."

# 如果需要暂停效果，可以取消下面一行的注释
# read -p "Press Enter to continue..."

# 脚本执行完毕后，虚拟环境仍然是激活状态，
# 直到您手动运行 deactivate 或关闭当前 shell 