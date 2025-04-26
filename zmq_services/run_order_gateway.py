import time
import sys
import os

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from logger import setup_logging, getLogger

setup_logging(service_name="OrderGatewayRunner") # Set service name
# Get logger instance
logger = getLogger(__name__)

# Import the service class
try:
    from zmq_services.order_execution_gateway import OrderExecutionGatewayService
except ImportError as e:
    print(f"Error importing OrderExecutionGatewayService: {e}")
    print(f"Project root added to path: {project_root}")
    print(f"Current sys.path: {sys.path}")
    print("Ensure zmq_services directory and its dependencies exist.")
    sys.exit(1)

def main():
    """Runs the order execution gateway service."""
    logger.info("正在初始化订单执行网关服务...")
    gateway_service = OrderExecutionGatewayService()

    logger.info("尝试启动服务...")
    gateway_service.start()

    logger.info("订单执行网关服务正在运行。按 Ctrl+C 停止。")

    try:
        # Keep the main thread alive while the service runs
        # Service logic runs in background threads (EventEngine, ZMQ PULL thread)
        while gateway_service.running:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\n检测到 Ctrl+C，正在停止服务...")
    except Exception as err:
        logger.exception(f"服务运行时发生意外错误: {err}")
    finally:
        # Ensure graceful shutdown
        gateway_service.stop()
        logger.info("订单执行网关服务已退出。")

if __name__ == "__main__":
    main()
