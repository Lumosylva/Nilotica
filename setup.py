# setup.py
import os
import platform
import shutil
import subprocess
from pathlib import Path
from setuptools import setup, Extension, find_packages
from setuptools.command.build_ext import build_ext


# 设置pyd和pyi文件存放路径，前面是项目绝对路径，改成自己的
target_dir = Path("D:/Project/PycharmProjects/Nilotica/vnpy_ctp/api")
# 设置pybind11头文件目录，前面是项目绝对路径，改成自己的
pybind11_include = "D:/Project/PycharmProjects/Nilotica/.venv/Lib/site-packages/pybind11/include"
print("定义pyd目标目录:", target_dir)
print("pybind11_include:", pybind11_include)

# 自定义构建命令：编译扩展后生成存根
class CustomBuildExt(build_ext):

    def run(self):
        # 1. 先执行原始编译逻辑
        super().run()

        # 2. 仅处理 Windows/Linux 平台
        if platform.system() not in ["Windows", "Linux"]:
            return

        # 3. 获取编译产物路径
        build_dir = Path(self.build_lib).resolve() # 处理绝对路径，避免相对路径歧义
        print(f"编译产物目录: {build_dir}")

        # 4. 遍历所有扩展模块，复制 .pyd/.so 文件
        for ext in self.extensions:
            # 构建模块文件名（如 vnctpmd.cp312-win_amd64.pyd）
            ext_name = ext.name.split('.')[-1]  # 如 'vnctpmd'
            # 使用通配符 pattern = f"{ext_name}.*.pyd" 适配不同平台和
            # Python 版本的文件名（如 vnctpmd.cp312-win_amd64.pyd）
            if platform.system() == "Windows":
                pattern = f"{ext_name}.*.pyd"
            else:
                pattern = f"{ext_name}.*.so"

            # 在构建目录中搜索匹配文件
            for src_file in build_dir.glob(f"**/{pattern}"):
                dest_file = target_dir / src_file.name
                print(f"复制: {src_file} -> {dest_file}")
                shutil.copy(src_file, dest_file)

        # 6. 生成存根文件（需确保模块已可导入）
        print("\n生成存根文件...")
        for ext in self.extensions:
            module_name = ext.name.split('.')[-1]
            cmd = [
                "pybind11-stubgen",
                module_name,
                "--output-dir", str(target_dir)
            ]
            # 添加当前目录到 PYTHONPATH
            env = os.environ.copy()
            env["PYTHONPATH"] = str(target_dir) + os.pathsep + env.get("PYTHONPATH", "")

            try:
                subprocess.run(cmd, check=True, env=env, cwd=Path(__file__).parent)
                print(f"成功生成存根: {module_name}.pyi")
            except subprocess.CalledProcessError as e:
                print(f"存根生成失败: {e}")
                raise


def get_ext_modules() -> list:
    """
    获取三方模块
    Linux和Windows需要编译封装接口
    """
    # 明确指定编译目标，并添加调试输出
    system = platform.system()
    if system not in ["Windows", "Linux"]:
        print(f"当前系统 {system} 不支持编译，跳过扩展模块。")
        return []

    libraries = ["thostmduserapi_se", "thosttraderapi_se"]

    # Linux
    if platform.system() == "Linux":
        include_dirs = ["vnpy_ctp/api/include", pybind11_include, "vnpy_ctp/api/vnctp"]
        library_dirs = ["vnpy_ctp/api"]
        extra_compile_flags = [
            "-std=c++17",
            "-O3",
            "-Wno-delete-incomplete",
            "-Wno-sign-compare",
        ]
        extra_link_args = ["-lstdc++"]
        runtime_library_dirs = ["$ORIGIN"]
    # Windows
    elif platform.system() == "Windows":
        include_dirs = ["vnpy_ctp/api/include", pybind11_include, "vnpy_ctp/api/vnctp"]
        library_dirs = ["vnpy_ctp/api/libs", "vnpy_ctp/api"]
        extra_compile_flags = ["-O2", "-MT"]
        extra_link_args = []
        runtime_library_dirs = []
    else: # 去掉了Mac支持
        return []

    # 定义扩展模块
    extensions = [
        Extension(
            name="vnpy_ctp.api.vnctpmd",
            sources=["vnpy_ctp/api/vnctp/vnctpmd/vnctpmd.cpp"],
            include_dirs=include_dirs,
            library_dirs=library_dirs,
            libraries=libraries,
            extra_compile_args=extra_compile_flags,
            extra_link_args=extra_link_args,
            runtime_library_dirs=runtime_library_dirs,
            language="cpp",
        ),

        Extension(
            name="vnpy_ctp.api.vnctptd",
            sources=["vnpy_ctp/api/vnctp/vnctptd/vnctptd.cpp"],
            include_dirs=include_dirs,
            library_dirs=library_dirs,
            libraries=libraries,
            extra_compile_args=extra_compile_flags,
            extra_link_args=extra_link_args,
            runtime_library_dirs=runtime_library_dirs,
            language="cpp",
        )
    ]

    print("\n扩展模块配置：")
    for ext in extensions:
        print(f"- 模块名: {ext.name}")
        print(f"  源文件: {ext.sources}")
        print(f"  包含目录: {ext.include_dirs}")
        print(f"  库目录: {ext.library_dirs}")
        print(f"  链接库: {ext.libraries}\n")

    return extensions


# 配置 package_data 确保存根文件被打包
package_data = {
    "vnpy_ctp.api": ["*.dll", "*.so", "*.pyd", "*.pyi"],
    "vnpy_ctp.api.libs": ["*.a", "*.lib"],
}


setup(
    packages=find_packages(include=['vnpy_ctp.api', 'vnpy_ctp.api.*']),
    package_dir={'vnpy_ctp': 'vnpy_ctp'},
    ext_modules=get_ext_modules(),
    cmdclass = {'build_ext': CustomBuildExt},  # 注册自定义构建命令
    package_data = package_data,  # 包含存根和二进制文件
    include_package_data=True,
    setup_requires = ["pybind11-stubgen"],  # 确保生成工具可用
)