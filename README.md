### **1. 说明**

- 使用 `whl` 格式和 `pyproject.toml` 配置。
- **工具链**：uv + `setuptools` + `wheel`。

### 2. 构建流程**

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