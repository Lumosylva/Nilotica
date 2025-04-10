# 基于vnpy的期货量化交易系统——Nilotica

### **1. 前言**

项目基于 [vnpy](https://github.com/vnpy/vnpy) 及 [vnpy_ctp](https://github.com/vnpy/vnpy_ctp) ，目的是简化国内期货量化交易的上手程度，让手动交易者更容易转向期货量化交易，让交易者更加专注于量化策略的开发。

### **2. 基础环境**

- **Python** ：`3.12.9`
- **工具链**：`uv` + `setuptools` + `wheel`
- **vnpy** ：`3.9.4`
- **vnpy_ctp**： `6.7.2.1`（基于**CTP 6.7.2**接口封装，接口中自带的是穿透式环境的dll文件）
- 需要进行`C++`编译，因此在执行下述命令之前请确保已经安装了`Visual Studio`（`Windows`）、`GCC`（`Linux`）
- 目前仅在`Windows`环境开发测试，`Linux`未测试

### **3. 环境配置**

项目使用`uv`来管理Python虚拟环境及软件包，以及软件包分发。

uv的安装

```bash
# On macOS and Linux.
curl -LsSf https://astral.sh/uv/install.sh | sh

# On Windows.
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"

# With pip.
pip install uv
```

使用 `uv sync `命令可以让`uv`根据`pyproject.toml`文件中的配置，自动进行虚拟环境的创建和依赖的下载。

```
uv sync
```

或不使用sync 命令，手动创建虚拟环境。

```bash
# 指定虚拟环境Python版本
uv venv --python 3.12.9 .venv
# 激活虚拟环境
.venv\Scripts\activate
```

### **4. 构建流程**

项目利用`setup.py`在`vnpy_ctp\api\`路径下编译出Python可调用的行情和交易文件`.pyd`以及利用`pybind11-stubgen`生成它们对应的存根文件`.pyi`。

#### **(1) 清理旧的构建**

打开终端进入项目根目录，删除`dist`、`*.egg-info`目录

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
uv build .
或
uv build . -v  # 使用 -v 查看详细日志
```

### **5. 项目结构**

```reStructuredText
vnpy - vnpy官方的核心库，主要功能是实现事件驱动引擎，版本3.9.4。

vnpy_ctp - vnpy官方的ctp库，使用pybind11包装CTP C++接口为Python可调用的接口，主要功能是与交易所行情和交易服务器打交道。

vnpy_rpcservice - vnpy官方的RPC库，实现了RPC服务。

zmq_services - 本系统的核心，包括行情网关、订单执行网关、策略订阅器、风控管理、数据记录、策略回测。实现了行情转发、报单、策略执行、简单的风控监控、数据记录、策略回测等功能。

.python-version - 包含Python版本号，uv 自动生成的文件，不用手动编辑。

CHANGELOG.md - 本系统版本更新日志。

LICENSE.txt - license文件。

README.md - 项目说明。

main.py - 项目入口，暂时无定义

pyproject.toml -  项目配置文件，由uv自动生成，用于定义项目的主要依赖、元数据、构建系统等信息。

setup.py - 自动化编译脚本，实现在vnpy_ctp\api\路径下自动编译出行情和交易pyd文件及对应pyi存根文件。

uv.lock - 记录项目的所有依赖，由uv自动管理，不用手动编辑。
```



### **6. 项目进度**

- [x] 行情网关

  连接vnpy中CTP网关，将行情以订阅方式发送出去。

- [x] 订单执行网关

  处理来自策略订阅器的订单请求，将请求发送到vnpy中CTP网关。

- [x] 策略订阅器

  连接到行情网关，将策略订单发送到订单执行网关，打印订单回报和成交回报。

- [x] 风控管理

  连接行情发布器(行情网关)和订单/成交回报发布器(订单执行网关)，维护持仓更新，持仓限制相关警报。

- [x] 数据记录

  连接行情发布器(行情网关)和订单/成交回报发布器(订单执行网关)，记录tick、order、trade数据到本地。

- [x] 策略回测

  1. 回放今天的数据，以最大速度。

     ```bash
     python zmq_services/backtester/run_data_player.py
     ```

  2. 对指定日期 20250409 运行回测

     ```bash
     python zmq_services/backtester/run_backtest.py --date 20250409
     ```

  3. 回放指定日期 (例如 20250409) 的数据，以接近实时的速度 (1x)

     ```bash
     python zmq_services/backtester/run_data_player.py --date 20250409 --speed 1
     ```

  4. 回放指定日期的数据，以 10 倍速度

     ```bash
     python zmq_services/backtester/run_data_player.py --date 20250409 --speed 10
     ```

  5. 从不同路径加载数据回放

     ```bash
     python zmq_services/backtester/run_data_player.py --date 20250409 --path /path/to/other/data
     ```

- [ ] 更精确的成本计算：如果需要，可以实现更复杂的成本计算方法，如 FIFO 或 LIFO。

- [ ] 可视化：将权益曲线、回撤等用web绘制出来。

- [ ] 参数化配置：将初始资金、无风险利率、年化天数等参数移到配置文件或命令行参数中。

- [ ] 统计检验：对策略收益进行更严格的统计检验。

- [ ] 与其他模块集成：将性能报告与策略优化、风险管理等模块结合。

- [ ] 优化性能报告的计算或显示

开发中......

### **7. 交流**

QQ交流群：`446042777`(澄明期货研究)