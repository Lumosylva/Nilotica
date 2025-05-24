import os
import sys

from utils.service_common import runner_args

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.config_manager import ConfigManager
from utils.i18n import _
from utils.logger import logger, setup_logging
from zmq_services.strategy_engine import StrategyEngine


def main():
    """
    运行策略引擎服务。

    Runs the strategy engine service.
    :return:
    """
    args = runner_args(arg_desc="Run the Strategy Engine Service.")

    setup_logging(service_name=f"StrategyEngineRunner[{args.ctp_env}]", level=args.log_level.upper(), config_env=args.config_env)

    # +++ 使用 config_env 初始化 ConfigManager(Initialize ConfigManager with config_env) +++
    config_service = ConfigManager(environment=args.config_env)

    logger.info(_("正在初始化策略引擎..."))

    # +++ 从 ConfigManage 获取连接 URL 和策略配置(Get connection URLs and strategy config from ConfigManager) +++
    md_pub_addr_raw = config_service.get_global_config("zmq_addresses.market_data_pub")
    order_gw_rep_addr_raw = config_service.get_global_config("zmq_addresses.order_gateway_rep")
    order_gw_pub_addr_raw = config_service.get_global_config("zmq_addresses.order_gateway_pub")
    strategies_config = config_service.get_strategies_config()

    # 检查是否已加载所需配置(Check if required configurations were loaded)
    if not all([md_pub_addr_raw, order_gw_rep_addr_raw, order_gw_pub_addr_raw]):
        logger.critical(_("错误：未能从 global_config.yaml 中获取必要的 ZMQ 地址。请检查配置。"))
        sys.exit(1)
    if not strategies_config:
        logger.critical(_("错误：未能加载策略配置。请检查策略配置是否存在且格式正确。")) # 使用内部路径来显示错误消息(Use internal path for error message)
        sys.exit(1)

    # 将通配符或 0.0.0.0 替换为 localhost 以进行本地连接(Replace wildcard or 0.0.0.0 with localhost for connecting locally)
    md_pub_addr_connect = md_pub_addr_raw.replace("*", "localhost").replace("0.0.0.0", "localhost")
    order_gw_rep_addr_connect = order_gw_rep_addr_raw.replace("*", "localhost").replace("0.0.0.0", "localhost")
    order_gw_pub_addr_connect = order_gw_pub_addr_raw.replace("*", "localhost").replace("0.0.0.0", "localhost")

    logger.info(_("连接行情发布器地址: {}").format(md_pub_addr_connect))
    logger.info(_("连接订单网关请求地址: {}").format(order_gw_rep_addr_connect))
    logger.info(_("连接订单回报发布器地址: {}").format(order_gw_pub_addr_connect))
    logger.info(_("加载的策略配置: {}").format(strategies_config)) # Log loaded strategies

    # 创建 StrategyEngine 实例，传递获取的配置(Create the StrategyEngine instance, passing the fetched config)
    engine = None # 初始化 finally 块(Initialize for finally block)
    try:
        engine = StrategyEngine(
            config_manager=config_service, # 传递 config_service 实例(Pass config_service instance)
            gateway_pub_url=md_pub_addr_connect,      # 使用连接地址(Use connect address)
            order_gw_rep_url=order_gw_rep_addr_connect, # <-- CORRECT KEYWORD
            order_report_url=order_gw_pub_addr_connect, # 使用连接地址(Use connect address)
            strategies_config=strategies_config # 使用获取的策略配置(Use fetched strategies config)
        )
    except Exception as init_err:
         logger.exception(_("初始化策略引擎时发生严重错误: {}").format(init_err))
         sys.exit(1)

    if not engine.strategies:
         logger.error(_("未能成功加载任何策略。引擎将退出。"))
         sys.exit(1)

    logger.info(_("尝试启动策略引擎..."))
    try:
        # start 方法包含主循环(The start method contains the main loop)
        engine.start()
    except KeyboardInterrupt:
        logger.info(_("主程序检测到 Ctrl+C，正在停止引擎..."))
        # engine.start() 应该处理 KeyboardInterrupt 并调用 stop
        # 添加显式 stop 函数，以防循环在调用 stop 函数之前异常退出
        # engine.start() should handle KeyboardInterrupt and call stop
        # Add explicit stop just in case loop exited abnormally before stop was called
        if engine and hasattr(engine, 'running') and engine.running:
            engine.stop()
    except Exception as e:
        logger.exception(_("策略引擎运行时发生意外错误: {}").format(e))
        if engine and hasattr(engine, 'running') and engine.running:
            engine.stop()
    finally:
        # 如果尚未完成，请确保调用停止函数
        # 在尝试停止之前，检查引擎对象是否存在且是否具有正在运行的属性
        # Ensure stop is called if not already done
        # Check if engine object exists and has running attribute before attempting stop
        if 'engine' in locals() and hasattr(engine, 'running') and engine.running:
             logger.info(_("执行最终停止清理..."))
             engine.stop()
        logger.info(_("策略引擎运行结束。"))

if __name__ == "__main__":
    main()
