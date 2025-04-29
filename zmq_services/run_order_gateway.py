import time
import sys
import os
import argparse

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.logger import setup_logging, logger
from config import zmq_config as config
from zmq_services.order_execution_gateway import OrderExecutionGatewayService

# Check for required RPC addresses in config
if not hasattr(config, 'ORDER_GATEWAY_REP_ADDRESS') or \
   not hasattr(config, 'ORDER_GATEWAY_PUB_ADDRESS'):
    # Use logger after setup if moved inside main, or print if logger not ready
    try:
        logger.critical("错误：配置文件 config.zmq_config 缺少 ORDER_GATEWAY_REP_ADDRESS 或 ORDER_GATEWAY_PUB_ADDRESS。")
    except NameError: # If logger is not yet defined
        print("CRITICAL: 错误：配置文件 config.zmq_config 缺少 ORDER_GATEWAY_REP_ADDRESS 或 ORDER_GATEWAY_PUB_ADDRESS。")
    sys.exit(1)

def main():
    """Runs the order execution gateway service (RPC Mode)."""
    # +++ Add Argument Parser +++
    parser = argparse.ArgumentParser(description="Run the Order Execution Gateway Service for a specific CTP environment.")
    # Make --env optional with a default value
    parser.add_argument(
        "--env", 
        default="simnow", 
        help="The CTP environment name (e.g., 'simnow', 'simnow7x24') defined in connect_ctp.json. Defaults to 'simnow'."
    )
    # +++ Add log-level argument +++
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the minimum logging level."
    )
    # +++ End add log-level +++
    args = parser.parse_args()

    # --- Setup Logging (Moved here) ---
    setup_logging(service_name="OrderGatewayRunner", level=args.log_level.upper())
    # --- End Logging Setup ---

    # Log if default environment is used
    if args.env == "simnow" and '--env' not in sys.argv:
        logger.info("No --env specified, using default environment: simnow")
    # --- End Argument Parser Modifications ---

    logger.info(f"正在初始化订单执行网关服务(RPC模式) for environment: [{args.env}]...")
    # Pass the environment name to the service constructor
    gw_service = OrderExecutionGatewayService(environment_name=args.env)

    logger.info(f"尝试启动服务(RPC模式) for [{args.env}]...")
    gw_service.start()

    try:
        # Keep the main thread alive while the service runs
        # Use is_active() from RpcServer
        while gw_service.is_active():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info(f"\n检测到 Ctrl+C，正在停止服务 [{args.env}]...")
    except Exception as err:
        logger.exception(f"服务运行时发生意外错误: {err}")
    finally:
        # Ensure graceful shutdown
        logger.info(f"开始停止服务(RPC模式) for [{args.env}]...")
        gw_service.stop()
        logger.info(f"订单执行网关服务(RPC模式) for [{args.env}] 已退出。")

if __name__ == "__main__":
    main()
