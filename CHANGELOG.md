# 0.1.3版本

1. 加入国际化，支持中英文语句输出。
2. 



Version 0.1.3

1. Added internationalization to support Chinese and English sentence output.
2. 



# 0.1.2版本

1. 优化风险管理器 (RiskManagerService) 和策略引擎 (StrategyEngine) 与订单执行网关 (OrderExecutionGatewayService) 连接状态的机制，到订单执行网关的连接，优化了心跳检测，实现了更健壮的超时和重试逻辑。策略引擎使用了两种机制结合的方式来判断与网关的连接状态，1. 监听 PUB/SUB 心跳 (主要检测超时)，2. 主动 RPC Ping (检查/恢复 RPC 连接)。风险管理器使用了单一机制来判断连接状态，主动 RPC Ping。
2. 全部的服务及启动脚本替换了更好的日志打印模块，提供 --log-level 参数加载不同的日志级别打印，用户目录下 (例如：C:\Users\donny\\.nilotica) 从nt_setting.json 中读取日志打印级别，默认日志级别为 INFO。
3. 各网关启动脚本增加 --env 可选参数，从用户目录下的 connect_ctp.json 中选择对应的环境配置进行连接，默认环境为 simnow。
4. 增加策略基类 (strategy_base.py)，改进类型提示并解决持仓 (self.pos) 使用浮点数可能带来的精度问题。添加跟踪平均入场价的功能，这对于计算浮动盈亏或设置动态止损非常有用。
5. 优化数据记录器 (data_recorder.py) 中的错误恢复机制，使其在遇到解码或序列化错误时能够更好地处理数据。采用批量写入来缓解高流量下同步写入可能带来的性能瓶颈。
6. 优化策略引擎 (strategy_engine.py) 的策略映射，使一个 vt_symbol 可以映射到多个策略实例。事件分发，将相应的事件分发给所有监听该合约的策略。策略引擎使用配置文件路径用户目录下strategies_setting.json。
7. 解耦示例策略 (threshold_strategy.py) 与对全局 config.zmq_config 的依赖，策略应该只依赖于通过字典传递给它的参数。
8. 区分在回测模式和实盘模式下，策略引擎 (StrategyEngine) 与订单执行网关 (OrderExecutionGatewayService) 通信的处理。采用不同的订单发送逻辑，回测模式下将订单请求发送给模拟引擎 (SimulationEngineService)。
9. 优化数据记录器 (data_player.py)，修改 DataPlayerService.load_data 方法，让它加载按合约分开存储的 Tick 文件，并使用 msgpack 进行序列化。
10. 全局使用 msgpack 进行序列化和反序列化代替 Pickle。
11. 配置管理的统一性优化，全局配置从 global_config.yaml 文件读取，包含了从 config/zmq_config.py 迁移过来的所有的全局配置，例如 ZMQ 地址、路径、风险管理参数、引擎通信参数以及默认订阅合约列表。
12. 对各个服务启动模块加入环境隔离 (dev/prod/backtest)，例如当使用 --config-env dev 启动时将会使用config/dev_config.yaml中的配置。
13. 增强系统的类型安全与校验，定义配置的预期结构和类型（使用 Pydantic 模型）。ConfigManager 在加载和合并配置后，使用这些模型进行校验。如果校验失败，则记录详细错误并可能阻止服务启动或回落到安全的默认值。各个服务模块在访问配置项时，可以假设类型基本正确。
14. 优化回测和回放模块回测策略配置加载，从 ConfigManager 获取回测参数，包括：回测数据源路径、回测 ZMQ 地址、产品信息文件路径、策略配置、回测特定参数。
15. 强化回测框架与结果分析，提升订单执行模拟的真实性，可配置的手续费模型、不同类型的滑点模型、标准化性能指标。



Version 0.1.2

1. Optimize the mechanism of the connection status between RiskManagerService and StrategyEngine and OrderExecutionGatewayService. The connection to the OrderExecutionGateway has been optimized, and the heartbeat detection has been optimized to implement a more robust timeout and retry logic. The Strategy Engine uses a combination of two mechanisms to determine the connection status with the gateway: 1. Listening to PUB/SUB heartbeats (mainly detecting timeouts), 2. Active RPC Ping (checking/restoring RPC connections). The Risk Manager uses a single mechanism to determine the connection status, active RPC Ping.
2. All services and startup scripts have been replaced with a better log printing module, providing the --log-level parameter to load different log level printing. The log printing level is read from nt_setting.json in the user directory (for example: C:\Users\donny\\.nilotica), and the default log level is INFO.
3. Each gateway startup script adds the --env optional parameter, selects the corresponding environment configuration from the connect_ctp.json in the user directory to connect, and the default environment is simnow.
4. Add the strategy base class (strategy_base.py), improve the type prompt and solve the precision problem that may be caused by the use of floating point numbers in the position (self.pos). Add the function of tracking the average entry price, which is very useful for calculating floating profit and loss or setting dynamic stop loss.
5. Optimize the error recovery mechanism in the data recorder (data_recorder.py) so that it can better handle data when encountering decoding or serialization errors. Use batch writing to alleviate the performance bottleneck that may be caused by synchronous writing under high traffic.
6. Optimize the strategy mapping of the strategy engine (strategy_engine.py) so that one vt_symbol can be mapped to multiple strategy instances. Event distribution, distribute the corresponding events to all strategies that listen to the contract. The strategy engine uses the configuration file path strategies_setting.json in the user directory.
7. Decouple the sample strategy (threshold_strategy.py) from the global config.zmq_config. The strategy should only depend on the parameters passed to it through the dictionary.
8. Differentiate the handling of the communication between the StrategyEngine and the OrderExecutionGatewayService in backtest mode and real trading mode. Use different order sending logic, and send order requests to the SimulationEngineService in backtest mode.
9. Optimize the data recorder (data_player.py), modify the DataPlayerService.load_data method, let it load the Tick file stored separately by contract, and use msgpack for serialization.
10. Use msgpack for serialization and deserialization globally instead of Pickle.
11. Unified configuration management optimization. Global configuration is read from the global_config.yaml file, which contains all global configurations migrated from config/zmq_config.py, such as ZMQ addresses, paths, risk management parameters, engine communication parameters, and default subscription contract lists.
12. Add environment isolation (dev/prod/backtest) to each service startup module. For example, when starting with --config-env dev, the configuration in config/dev_config.yaml will be used.
13. Enhance the type safety and verification of the system, and define the expected structure and type of the configuration (using Pydantic models). ConfigManager uses these models for verification after loading and merging the configuration. If the verification fails, detailed errors are recorded and the service may be prevented from starting or fall back to safe default values. When accessing configuration items, each service module can assume that the type is basically correct.
14. Optimize the backtest strategy configuration loading of the backtest and replay modules, and obtain the backtest parameters from ConfigManager, including: backtest data source path, backtest ZMQ address, product information file path, strategy configuration, and backtest specific parameters.
15. Strengthen the backtest framework and result analysis, improve the authenticity of order execution simulation, configurable fee model, different types of slippage models, and standardized performance indicators.

------

# 0.1.1版本

1. 使用新的`vnpy`版本4.0.0。
2. 使用新的`vnpy_ctp`版本6.7.7.1，基于CTP期货版的6.7.7接口封装开发，接口中自带的是【穿透式实盘环境】的dll文件。
3. 修改pyproject.toml文件。



Version 0.1.1

1. Use the new `vnpy` version 4.0.0.
2. Use the new `vnpy_ctp` version 6.7.7.1, which is developed based on the 6.7.7 interface package of the CTP futures version. The interface comes with the dll file of the [penetrating real-time environment].
3. Modify the pyproject.toml file.

------

# 0.1.0版本

1. 使用`vnpy`版本3.9.4。
2. 使用`vnpy_ctp`版本6.7.2.1，基于CTP期货版的6.7.2接口封装开发，接口中自带的是【穿透式实盘环境】的dll文件。
3. 对`vnpy_ctp`中CTP编译的支持修改。
4. 行情网关、订单执行网关、策略订阅器、风控管理、数据记录、策略回测、行情回放的实现。
5. 使用uv管理Python虚拟环境及依赖包。
6. 使用hatch构建项目。



Version 0.1.0

1. Use `vnpy` version 3.9.4.

2. Use `vnpy_ctp` version 6.7.2.1, based on the 6.7.2 interface package development of CTP futures version, the interface comes with the dll file of [penetrating real-time environment].
3. Modify the support of CTP compilation in `vnpy_ctp`.
4. Implementation of market gateway, order execution gateway, strategy subscriber, risk control management, data recording, strategy backtesting, and market playback.
5. Use uv to manage Python virtual environment and dependent packages.
6. Use hatch to build the project.



