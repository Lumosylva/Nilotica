import sys
import os
import argparse
import json
import time # Import time if needed

from vnpy.trader.utility import load_json

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# +++ Import ConfigManager +++
from utils.config_manager import ConfigManager

from zmq_services.strategy_engine import StrategyEngine
# --- Remove old config import --- 
# from config import zmq_config as config
from utils.logger import setup_logging, logger

# --- Remove hardcoded STRATEGIES_CONFIG and loading --- 
# STRATEGIES_CONFIG = { ... }
# STRATEGIES_CONFIG_FILENAME: str = "strategies_setting.json"
# STRATEGIES_CONFIG.update(load_json(STRATEGIES_CONFIG_FILENAME))

def main():
    """Runs the strategy engine service."""
    parser = argparse.ArgumentParser(description="Run the Strategy Engine Service.")
    parser.add_argument(
        "--ctp-env", # Renamed from --env
        default="simnow", 
        help="The CTP environment name (e.g., 'simnow'). Currently informational for Strategy Engine."
    )
    # +++ Add --config-env argument +++
    parser.add_argument(
        "--config-env",
        default=None, # Default to dev
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

    # Setup logging with config_env
    setup_logging(service_name=f"StrategyEngineRunner[{args.ctp_env}]", level=args.log_level.upper(), config_env=args.config_env)

    # +++ Initialize ConfigManager with config_env +++
    config_service = ConfigManager(environment=args.config_env)

    # Log environments being used
    if args.ctp_env == "simnow" and '--ctp-env' not in sys.argv and '--env' not in sys.argv: # Check both old and new name for default message
        logger.info(f"No --ctp-env specified, using default CTP environment: {args.ctp_env}")
    else:
        logger.info(f"Strategy Engine CTP environment (informational): {args.ctp_env}")
    
    if args.config_env:
        logger.info(f"Using configuration environment: '{args.config_env}'")
    else:
        # This case should not happen if default is "dev"
        logger.info("No --config-env specified, using base global_config.yaml only.") 

    logger.info("正在初始化策略引擎...")

    # --- Remove old config check ---
    # if not hasattr(config, 'MARKET_DATA_PUB_ADDRESS') or \
    #    not hasattr(config, 'ORDER_GATEWAY_PUB_ADDRESS') or \
    #    not hasattr(config, 'ORDER_GATEWAY_REP_ADDRESS'):
    #     logger.critical("错误：配置文件 config.zmq_config 缺少必要的 ZMQ 地址 (MARKET_DATA_PUB_ADDRESS, ORDER_GATEWAY_PUB_ADDRESS, ORDER_GATEWAY_REP_ADDRESS)。")
    #     sys.exit(1)

    # +++ Get connection URLs and strategy config from ConfigManager +++
    md_pub_addr_raw = config_service.get_global_config("zmq_addresses.market_data_pub")
    order_gw_rep_addr_raw = config_service.get_global_config("zmq_addresses.order_gateway_rep")
    order_gw_pub_addr_raw = config_service.get_global_config("zmq_addresses.order_gateway_pub")
    strategies_config = config_service.get_strategies_config()

    # Check if required configurations were loaded
    if not all([md_pub_addr_raw, order_gw_rep_addr_raw, order_gw_pub_addr_raw]):
        logger.critical("错误：未能从 global_config.yaml 中获取必要的 ZMQ 地址。请检查配置。")
        sys.exit(1)
    if not strategies_config:
        logger.critical(f"错误：未能加载策略配置。请检查策略配置是否存在且格式正确。") # Use internal path for error message
        sys.exit(1)

    # Replace wildcard or 0.0.0.0 with localhost for connecting locally
    md_pub_addr_connect = md_pub_addr_raw.replace("*", "localhost").replace("0.0.0.0", "localhost")
    order_gw_rep_addr_connect = order_gw_rep_addr_raw.replace("*", "localhost").replace("0.0.0.0", "localhost")
    order_gw_pub_addr_connect = order_gw_pub_addr_raw.replace("*", "localhost").replace("0.0.0.0", "localhost")

    logger.info(f"连接行情发布器地址: {md_pub_addr_connect}")
    logger.info(f"连接订单网关请求地址: {order_gw_rep_addr_connect}")
    logger.info(f"连接订单回报发布器地址: {order_gw_pub_addr_connect}")
    logger.info(f"加载的策略配置: {strategies_config}") # Log loaded strategies

    # Create the StrategyEngine instance, passing the fetched config
    engine = None # Initialize for finally block
    try:
        engine = StrategyEngine(
            config_manager=config_service, # +++ Pass config_service instance +++
            gateway_pub_url=md_pub_addr_connect,      # Use connect address
            order_gw_rep_url=order_gw_rep_addr_connect, # <-- CORRECT KEYWORD
            order_report_url=order_gw_pub_addr_connect,# Use connect address
            strategies_config=strategies_config # Use fetched strategies config
        )
    except Exception as init_err:
         logger.exception(f"初始化 StrategyEngine 时发生严重错误: {init_err}")
         sys.exit(1)

    if not engine.strategies:
         logger.error("未能成功加载任何策略。引擎将退出。")
         sys.exit(1)

    logger.info("尝试启动策略引擎...")
    try:
        # The start method contains the main loop
        engine.start()
    except KeyboardInterrupt:
        logger.info("主程序检测到 Ctrl+C，正在停止引擎...")
        # engine.start() should handle KeyboardInterrupt and call stop
        # Add explicit stop just in case loop exited abnormally before stop was called
        if engine and hasattr(engine, 'running') and engine.running:
            engine.stop()
    except Exception as e:
        logger.exception(f"策略引擎运行时发生意外错误: {e}")
        if engine and hasattr(engine, 'running') and engine.running:
            engine.stop()
    finally:
        # Ensure stop is called if not already done
        # Check if engine object exists and has running attribute before attempting stop
        if 'engine' in locals() and hasattr(engine, 'running') and engine.running:
             logger.info("执行最终停止清理...")
             engine.stop()
        logger.info("策略引擎运行结束。")

if __name__ == "__main__":
    main()
