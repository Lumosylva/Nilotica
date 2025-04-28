import time
import sys
import os

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from logger import setup_logging, getLogger

# Import config first to check addresses
from config import zmq_config as config

setup_logging(service_name="OrderGatewayRunner") # Set service name
# Get logger instance
logger = getLogger(__name__)

# Check for required RPC addresses in config
if not hasattr(config, 'ORDER_GATEWAY_REP_ADDRESS') or \
   not hasattr(config, 'ORDER_GATEWAY_PUB_ADDRESS'):
    logger.critical("错误：配置文件 config.zmq_config 缺少 ORDER_GATEWAY_REP_ADDRESS 或 ORDER_GATEWAY_PUB_ADDRESS。")
    sys.exit(1)

# Import the service class after check
try:
    from zmq_services.order_execution_gateway import OrderExecutionGatewayService
except ImportError as e:
    logger.exception(f"Error importing OrderExecutionGatewayService: {e}")
    logger.info(f"Project root added to path: {project_root}")
    logger.info(f"Current sys.path: {sys.path}")
    logger.warning("Ensure zmq_services directory and its dependencies exist.")
    sys.exit(1)

def main():
    """Runs the order execution gateway service (RPC Mode)."""
    logger.info("正在初始化订单执行网关服务(RPC模式)...")
    # Rename variable for clarity
    gw_service = OrderExecutionGatewayService()

    logger.info("尝试启动服务(RPC模式)...")
    gw_service.start()

    try:
        # Keep the main thread alive while the service runs
        # Use is_active() from RpcServer
        while gw_service.is_active():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("\n检测到 Ctrl+C，正在停止服务...")
    except Exception as err:
        logger.exception(f"服务运行时发生意外错误: {err}")
    finally:
        # Ensure graceful shutdown
        logger.info("开始停止服务(RPC模式)...")
        gw_service.stop()
        logger.info("订单执行网关服务(RPC模式)已退出。")

if __name__ == "__main__":
    main()
