import time
import sys
import os

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

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
    # Get logger instance
    logger = getLogger(__name__)

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
        logger.info("主程序检测到 Ctrl+C，正在停止服务...")
    except Exception as e:
        logger.exception(f"服务运行时发生意外错误: {e}")
    finally:
        # Ensure graceful shutdown
        gateway_service.stop()
        logger.info("订单执行网关服务已退出。")

if __name__ == "__main__":
    # --- Setup Logging --- 
    # Add project root first
    project_root_setup = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root_setup not in sys.path:
        sys.path.insert(0, project_root_setup)
    # Now import logger setup
    try:
        from logger import setup_logging, getLogger
        setup_logging(service_name="OrderGatewayRunner") # Set service name
    except ImportError as log_err:
        print(f"CRITICAL: Failed to import or setup logger: {log_err}. Exiting.")
        sys.exit(1)

    main()
