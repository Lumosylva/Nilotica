import time
import sys
import os
import argparse # Import argparse

# 1. Import logger setup and getLogger AFTER modifying sys.path
# Add project root to Python path to find vnpy modules and zmq_services
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from logger import setup_logging, getLogger # Now safe to import

# 2. Setup Logging for this specific service
# Call this early, before other imports that might log (like MarketDataGatewayService)
setup_logging(service_name="MarketDataGatewayRunner", level="INFO") # <-- 改回 INFO
# 3. Get a logger for this script
logger = getLogger(__name__)

# Import the service class
try:
    from zmq_services.market_data_gateway import MarketDataGatewayService
except ImportError as e:
    # 4. Replace error print with logger call
    logger.exception(f"Error importing MarketDataGatewayService: {e}")
    logger.info(f"Project root added to path: {project_root}")
    logger.info(f"Current sys.path: {sys.path}")
    logger.warning("Ensure the zmq_services directory and its __init__.py exist.")
    sys.exit(1)


def main():
    """Runs the market data gateway service (RPC Mode)."""

    # +++ Add Argument Parser +++
    parser = argparse.ArgumentParser(description="Run the Market Data Gateway Service for a specific CTP environment.")
    # Make --env optional with a default value
    parser.add_argument(
        "--env", 
        default="simnow", 
        help="The CTP environment name (e.g., 'simnow', 'simnow7x24') defined in connect_ctp.json. Defaults to 'simnow'."
    )
    args = parser.parse_args()

    # Log if default environment is used
    if args.env == "simnow" and '--env' not in sys.argv:
        logger.info("No --env specified, using default environment: simnow")
    # --- End Argument Parser Modifications ---

    logger.info(f"正在初始化行情网关服务 for environment: [{args.env}]...")
    # Pass the environment name to the service constructor
    gateway_service = MarketDataGatewayService(environment_name=args.env)

    logger.info(f"尝试启动服务 for [{args.env}]...")
    gateway_service.start() # start() method now handles connection and subscription

    logger.info(f"行情网关服务 for [{args.env}] 正在运行。按 Ctrl+C 停止。")

    try:
        # Keep the main thread alive while the service runs in background threads (EventEngine)
        while gateway_service.is_active(): # Use is_active() from RpcServer
            time.sleep(1) # Prevent busy-waiting
    except KeyboardInterrupt:
        logger.info(f"\n检测到 Ctrl+C，正在停止服务 [{args.env}]...")
    except Exception as err:
        logger.exception(f"服务运行时发生意外错误: {err}")
    finally:
        logger.info(f"开始停止服务 for [{args.env}]...")
        gateway_service.stop()
        logger.info(f"行情网关服务 for [{args.env}] 已退出。")

if __name__ == "__main__":
    main() 