import sys
import os
import argparse

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.config_manager import ConfigManager

from zmq_services.strategy_engine import StrategyEngine
from utils.logger import setup_logging, logger


def main():
    """
    运行策略引擎服务。

    Runs the strategy engine service.
    :return:
    """
    parser = argparse.ArgumentParser(description="Run the Strategy Engine Service.")
    parser.add_argument(
        "--ctp-env", # Renamed from --env
        default="simnow", 
        help="The CTP environment name (e.g., 'simnow'). Currently informational for Strategy Engine."
    )
    parser.add_argument(
        "--config-env",
        default=None,
        type=str,
        help="The configuration environment to load (e.g., 'dev', 'prod', 'backtest'). Overrides global_config.yaml."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the minimum logging level."
    )
    args = parser.parse_args()

    setup_logging(service_name=f"StrategyEngineRunner[{args.ctp_env}]", level=args.log_level.upper(), config_env=args.config_env)

    # +++ 使用 config_env 初始化 ConfigManager(Initialize ConfigManager with config_env) +++
    config_service = ConfigManager(environment=args.config_env)

    # 正在使用的日志环境(Log environments being used)
    if args.ctp_env == "simnow" and '--ctp-env' not in sys.argv and '--env' not in sys.argv: # Check both old and new name for default message
        logger.info(f"No --ctp-env specified, using default CTP environment: {args.ctp_env}")
    else:
        logger.info(f"Strategy Engine CTP environment (informational): {args.ctp_env}")
    
    if args.config_env:
        logger.info(f"Using configuration environment: '{args.config_env}'")
    else:
        # 如果默认值为 dev，则不会发生这种情况
        # This case should not happen if default is "dev"
        logger.info("No --config-env specified, using base global_config.yaml only.") 

    logger.info("正在初始化策略引擎...")

    # +++ 从 ConfigManage 获取连接 URL 和策略配置(Get connection URLs and strategy config from ConfigManager) +++
    md_pub_addr_raw = config_service.get_global_config("zmq_addresses.market_data_pub")
    order_gw_rep_addr_raw = config_service.get_global_config("zmq_addresses.order_gateway_rep")
    order_gw_pub_addr_raw = config_service.get_global_config("zmq_addresses.order_gateway_pub")
    strategies_config = config_service.get_strategies_config()

    # 检查是否已加载所需配置(Check if required configurations were loaded)
    if not all([md_pub_addr_raw, order_gw_rep_addr_raw, order_gw_pub_addr_raw]):
        logger.critical("错误：未能从 global_config.yaml 中获取必要的 ZMQ 地址。请检查配置。")
        sys.exit(1)
    if not strategies_config:
        logger.critical(f"错误：未能加载策略配置。请检查策略配置是否存在且格式正确。") # Use internal path for error message
        sys.exit(1)

    # 将通配符或 0.0.0.0 替换为 localhost 以进行本地连接(Replace wildcard or 0.0.0.0 with localhost for connecting locally)
    md_pub_addr_connect = md_pub_addr_raw.replace("*", "localhost").replace("0.0.0.0", "localhost")
    order_gw_rep_addr_connect = order_gw_rep_addr_raw.replace("*", "localhost").replace("0.0.0.0", "localhost")
    order_gw_pub_addr_connect = order_gw_pub_addr_raw.replace("*", "localhost").replace("0.0.0.0", "localhost")

    logger.info(f"连接行情发布器地址: {md_pub_addr_connect}")
    logger.info(f"连接订单网关请求地址: {order_gw_rep_addr_connect}")
    logger.info(f"连接订单回报发布器地址: {order_gw_pub_addr_connect}")
    logger.info(f"加载的策略配置: {strategies_config}") # Log loaded strategies

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
         logger.exception(f"初始化 StrategyEngine 时发生严重错误: {init_err}")
         sys.exit(1)

    if not engine.strategies:
         logger.error("未能成功加载任何策略。引擎将退出。")
         sys.exit(1)

    logger.info("尝试启动策略引擎...")
    try:
        # start 方法包含主循环(The start method contains the main loop)
        engine.start()
    except KeyboardInterrupt:
        logger.info("主程序检测到 Ctrl+C，正在停止引擎...")
        # engine.start() 应该处理 KeyboardInterrupt 并调用 stop
        # 添加显式 stop 函数，以防循环在调用 stop 函数之前异常退出
        # engine.start() should handle KeyboardInterrupt and call stop
        # Add explicit stop just in case loop exited abnormally before stop was called
        if engine and hasattr(engine, 'running') and engine.running:
            engine.stop()
    except Exception as e:
        logger.exception(f"策略引擎运行时发生意外错误: {e}")
        if engine and hasattr(engine, 'running') and engine.running:
            engine.stop()
    finally:
        # 如果尚未完成，请确保调用停止函数
        # 在尝试停止之前，检查引擎对象是否存在且是否具有正在运行的属性
        # Ensure stop is called if not already done
        # Check if engine object exists and has running attribute before attempting stop
        if 'engine' in locals() and hasattr(engine, 'running') and engine.running:
             logger.info("执行最终停止清理...")
             engine.stop()
        logger.info("策略引擎运行结束。")

if __name__ == "__main__":
    main()
