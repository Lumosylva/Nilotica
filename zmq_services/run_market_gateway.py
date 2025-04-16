import time
import sys
import os

# 1. Import logger setup and getLogger AFTER modifying sys.path
# Add project root to Python path to find vnpy modules and zmq_services
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from logger import setup_logging, getLogger # Now safe to import

# 2. Setup Logging for this specific service
# Call this early, before other imports that might log (like MarketDataGatewayService)
setup_logging(service_name="MarketDataGatewayRunner") # Give a specific name
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
    """Runs the market data gateway service."""
    logger.info("正在初始化行情网关服务...")
    gateway_service = MarketDataGatewayService()

    logger.info("尝试启动服务...")
    gateway_service.start() # start() method now handles connection and subscription

    logger.info("行情网关服务正在运行。按 Ctrl+C 停止。")

    try:
        # Keep the main thread alive while the service runs in background threads (EventEngine)
        while gateway_service.running: # Check the running flag
            time.sleep(1) # Prevent busy-waiting
    except KeyboardInterrupt:
        logger.info("\n检测到 Ctrl+C，正在停止服务...")
    except Exception as e:
        logger.exception(f"服务运行时发生意外错误: {e}")
    finally:
        gateway_service.stop()
        logger.info("行情网关服务已退出。")

if __name__ == "__main__":
    main() 