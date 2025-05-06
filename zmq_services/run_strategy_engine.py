import sys
import os
import argparse

from vnpy.trader.utility import load_json

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from zmq_services.strategy_engine import StrategyEngine
from config import zmq_config as config
from utils.logger import setup_logging, logger
import json

# --- Define Strategy Configuration ---
# This dictionary defines which strategies to load and their parameters.
STRATEGIES_CONFIG = {
    "SA509_Trend_1": {  # Unique name for this strategy instance
        "strategy_class": "zmq_services.strategies.threshold_strategy.ThresholdLiveStrategy", # <-- UPDATED PATH
        "vt_symbol": "SA509.CZCE", # Symbol the strategy trades
        "setting": {             # Strategy-specific parameters (MUST provide all required)
            "entry_threshold": "1310.0",      # Required: Decimal
            "profit_target_ticks": 10,        # Required: int
            "stop_loss_ticks": 5,           # Required: int
            "price_tick": "1.0",              # Required: Decimal
            "order_volume": "1.0",            # Required: Decimal
            "order_price_offset_ticks": 0     # +++ Add new parameter (int) +++
        }
    },
    # --- Add configuration for other strategies below ---
    # "AnotherStrategy_Instance": {
    #     "strategy_class": "zmq_services.strategies.another_strategy.AnotherStrategyClass",
    #     "vt_symbol": "ag2412.SHFE",
    #     "setting": {
    #         "param1_decimal": "100.5", # Example Decimal
    #         "param2_int": 50,          # Example Int
    #         # Add all required params for AnotherStrategyClass here
    #     }
    # },
}
# --- End Strategy Configuration ---
# Load global setting from json file.
STRATEGIES_CONFIG_FILENAME: str = "strategies_setting.json"
STRATEGIES_CONFIG.update(load_json(STRATEGIES_CONFIG_FILENAME))


def main():
    """Runs the strategy engine service."""
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Run the Strategy Engine Service.")
    parser.add_argument(
        "--env",
        default="simnow", # Keep consistent default, though engine might not use it
        help="The CTP environment name (e.g., 'simnow'). Currently informational for Strategy Engine."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the minimum logging level."
    )
    args = parser.parse_args()
    # --- End Argument Parsing ---

    # Setup logging at the beginning of main
    setup_logging(service_name=f"StrategyEngineRunner[{args.env}]", level=args.log_level.upper())

    # Log environment being used
    if args.env == "simnow" and '--env' not in sys.argv:
        logger.info("No --env specified, using default environment: simnow")
    else:
        logger.info(f"Running Strategy Engine for environment: {args.env}")

    logger.info("正在初始化策略引擎...")

    # Check required config addresses (ensure PUB/SUB addresses exist)
    if not hasattr(config, 'MARKET_DATA_PUB_ADDRESS') or \
       not hasattr(config, 'ORDER_GATEWAY_PUB_ADDRESS') or \
       not hasattr(config, 'ORDER_GATEWAY_REP_ADDRESS'):
        logger.critical("错误：配置文件 config.zmq_config 缺少必要的 ZMQ 地址 (MARKET_DATA_PUB_ADDRESS, ORDER_GATEWAY_PUB_ADDRESS, ORDER_GATEWAY_REP_ADDRESS)。")
        sys.exit(1)

    # Get connection URLs from config
    md_pub_addr_raw = config.MARKET_DATA_PUB_ADDRESS
    order_gw_rep_addr_raw = config.ORDER_GATEWAY_REP_ADDRESS
    order_gw_pub_addr_raw = config.ORDER_GATEWAY_PUB_ADDRESS

    # Replace wildcard or 0.0.0.0 with localhost for connecting locally
    md_pub_addr_connect = md_pub_addr_raw.replace("*", "localhost").replace("0.0.0.0", "localhost")
    order_gw_rep_addr_connect = order_gw_rep_addr_raw.replace("*", "localhost").replace("0.0.0.0", "localhost")
    order_gw_pub_addr_connect = order_gw_pub_addr_raw.replace("*", "localhost").replace("0.0.0.0", "localhost")

    logger.info(f"连接行情发布器地址: {md_pub_addr_connect}")
    logger.info(f"连接订单网关请求地址: {order_gw_rep_addr_connect}")
    logger.info(f"连接订单回报发布器地址: {order_gw_pub_addr_connect}")

    # Create the StrategyEngine instance, passing the config
    try:
        engine = StrategyEngine(
            gateway_pub_url=md_pub_addr_connect,      # Use connect address
            order_gw_rep_url=order_gw_rep_addr_connect, # <-- CORRECT KEYWORD
            order_report_url=order_gw_pub_addr_connect,# Use connect address
            strategies_config=STRATEGIES_CONFIG
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
        if hasattr(engine, 'running') and engine.running:
            engine.stop()
    except Exception as e:
        logger.exception(f"策略引擎运行时发生意外错误: {e}")
        if hasattr(engine, 'running') and engine.running:
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
