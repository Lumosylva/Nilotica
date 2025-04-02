# 基于vnpy的量化交易系统——Nilotica

### **1. 前言**

- 使用`whl` 格式和 `pyproject.toml` 配置。
- **工具链**：uv + `setuptools` + `wheel`。

### **2. 环境配置**

在工程根目录创建Python虚拟环境

```bash
# 环境指定Python版本3.12.9
uv venv --python 3.12.9 .venv
# 激活虚拟环境
.venv\Scripts\activate
```

### **3. 构建流程**

#### **(1) 清理旧构建**

PowerShell 删除dist、*.egg-info目录

```bash
Remove-Item -Path ".\dist", ".\*.egg-info" -Recurse -Force -ErrorAction SilentlyContinue
```

CMD 删除dist、*.egg-info目录

```bash
rmdir /s /q ".\dist" ".\*.egg-info"
```

#### **(2) 执行构建**

```bash
uv build . 或者
uv build . -v  # 使用 -v 查看详细日志
```

### **4. 项目结构**

`Nilotica` - 根目录

		- demo - 示例代码
		- vnpy - vnpy 官方核心库，版本3.9.4
		- vnpy_ctp - vnpy官方 vnpy-ctp 库，使用pybind11包装CTP C++接口为Python可调用的接口。
		- .python-version - 使用的Python版本，uv自动管理的文件
		- LICENSE.txt - license文件。
		- MANIFEST.in - Python 包管理工具（setuptools）用来指定哪些文件应该包含在生成的分发包（如 .tar.gz 或 .whl文件）中的配置文件，recursive-include表示递归的包含目录下匹配的文件。
		- pyproject.toml -  Python 项目配置文件，用于定义项目的元数据、构建系统、依赖管理等信息。
		- setup.py - 自动化编译文件
		- uv.lock - uv自动管理的文件

### **5. 交流**

QQ交流群：`446042777`(澄明期货研究)