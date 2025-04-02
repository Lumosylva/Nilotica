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

### 3. 构建流程**

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

QQ交流群：446042777(澄明期货研究)