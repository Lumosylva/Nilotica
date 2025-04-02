# 基于vnpy的量化交易系统——Nilotica

### **1. 前言**

项目基于[vnpy](https://github.com/vnpy/vnpy)及[vnpy_ctp](https://github.com/vnpy/vnpy_ctp)，目的是简化量化交易的上手程度，让手动交易者更容易转向量化交易，让交易者更加专注于量化策略的开发。

### **2. 基础环境**

- **Python** ：`3.12.9`
- **工具链**：`uv` + `setuptools` + `wheel`
- **vnpy** ：`3.9.4`
- **vnpy_ctp**： `6.7.2.1`（基于**CTP 6.7.2**接口封装，接口中自带的是穿透式环境的dll文件）
- 需要进行C++编译，因此在执行下述命令之前请确保已经安装了Visual Studio（Windows）、GCC（Linux）

### **3. 环境配置**

项目使用uv来管理Python虚拟环境及软件包，以及软件包分发。

uv的安装

```bash
# On macOS and Linux.
curl -LsSf https://astral.sh/uv/install.sh | sh

# On Windows.
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# With pip.
pip install uv
```

使用 `uv sync `命令可以让uv 工具根据pyproject.toml 文件中的配置进行虚拟环境的创建和依赖的下载，当从github 上下载一个基于uv的python 项目时，这个命令可以很方便的创建好虚拟环境和安装好依赖。

```
uv sync
```

或者不使用sync 命令，手动的去创建虚拟环境， 使用 `uv venv `命令创建虚拟环境。

```bash
# 环境指定Python版本3.12.9
uv venv --python 3.12.9 .venv
# 激活虚拟环境
.venv\Scripts\activate
```

### **4. 构建流程**

项目利用`setup.py`在`vnpy_ctp\api\`路径下编译出Python可调用的行情和交易文件`.pyd`以及利用`pybind11-stubgen`生成它们对应的存根文件`.pyi`。

#### **(1) 清理旧的构建**

打开终端进入项目根目录，删除dist、*.egg-info目录

PowerShell 

```bash
Remove-Item -Path ".\dist", ".\*.egg-info" -Recurse -Force -ErrorAction SilentlyContinue
```

CMD

```bash
rmdir /s /q ".\dist" ".\*.egg-info"
```

Bash

```bash
rm -rf .\dist .\*.egg-info
```

#### **(2) 执行构建**

```bash
uv build . 或者
uv build . -v  # 使用 -v 查看详细日志
```

### **5. 项目结构**

```reStructuredText
demo - 示例代码

vnpy - vnpy 官方核心库，版本3.9.4

vnpy_ctp - vnpy官方 vnpy-ctp 库，使用pybind11包装CTP C++接口为Python可调用的接口。

.python-version - 使用的Python版本，uv自动管理的文件

LICENSE.txt - license文件。

MANIFEST.in - Python 包管理工具（setuptools）用来指定哪些文件应该包含在生成的分发包（如 .tar.gz 或 .whl文件）中的配置文件，recursive-include表示递归的包含目录下匹配的文件。

pyproject.toml -  Python 项目配置文件，用于定义项目的主要依赖、元数据、构建系统等信息。

setup.py - 自动化编译文件

uv.lock - 记录项目的所有依赖，包括依赖的依赖，且跨平台，确保在不同环境下安装的一致性。这个文件由 uv 自动管理，不要手动编辑
```



### **6. 项目进度**

■□□□□□□□□□(开发中......)

### **7. 交流**

QQ交流群：`446042777`(澄明期货研究)