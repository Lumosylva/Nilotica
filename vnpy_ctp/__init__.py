# The MIT License (MIT)
#
# Copyright (c) 2015-present, Xiaoyou Chen
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation project_files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.


import os
from pathlib import Path

from .gateway import CtpGateway


# CTP DLL 文件位于 vnpy_ctp.api 目录下

# 添加 API 路径到环境变量，方便 CTP 接口动态载入底层 DLL
api_path: Path = Path(__file__).parent.joinpath("api")
os.add_dll_directory(str(api_path))
os.add_dll_directory(str(api_path.joinpath("libs")))

# 尝试导入 metadata 模块
try:
    # For Python 3.8 and later
    import importlib.metadata as importlib_metadata
except ImportError:
    # For Python 3.7 and earlier
    import importlib_metadata # type: ignore

# 从入口加载模块版本号
# Get the version of the package
# Note: Add fallback logic for cases where the package isn't installed,
# like during development or CI/CD builds without full installation.
try:
    __version__: str = importlib_metadata.version("vnpy_ctp")
except importlib_metadata.PackageNotFoundError:
    # Fallback version if the package is not installed
    __version__ = "0.0.0-dev"
