import time
import sys
import os
import logging

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the renamed service class and config
from zmq_services.strategy_subscriber import StrategyEngine
from config import zmq_config as config

# Import logger setup after adding path
try:
    from logger import setup_logging, getLogger
    # Setup logging for this runner script
    # Keep level INFO for runner itself, engine/strategies might have different levels
    setup_logging(service_name="StrategyEngineRunner", level="INFO")
except ImportError as log_err:
    print(f"CRITICAL: Failed to import or setup logger: {log_err}. Exiting.")
    sys.exit(1)

# --- Define Strategy Configuration ---
# This dictionary defines which strategies to load and their parameters.
STRATEGIES_CONFIG = {
    "SA509_Trend_1": {  # Unique name for this strategy instance
        "strategy_class": "zmq_services.strategies.sa509_strategy.SA509LiveStrategy", # Full path to the class
        "vt_symbol": "SA509.CZCE", # Symbol the strategy trades
        "setting": {             # Strategy-specific parameters (optional overrides)
            # "entry_threshold": 3060, # Example: Override entry threshold from config
            "order_volume": 1        # Example: Set order volume
            # Add other parameters defined in SA509LiveStrategy if needed
        }
    },
    # --- Add configuration for other strategies below ---
    # "AnotherStrategy_Instance": {
    #     "strategy_class": "zmq_services.strategies.another_strategy.AnotherStrategyClass",
    #     "vt_symbol": "ag2412.SHFE",
    #     "setting": {"param1": 100, "param2": 0.5}
    # },
}
# --- End Strategy Configuration ---


def main():
    """Runs the strategy engine service."""
    logger = getLogger(__name__)

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
            order_req_url=order_gw_rep_addr_connect,  # Use connect address
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
