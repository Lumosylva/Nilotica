import time
import sys
import os

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the service class and config
from zmq_services.risk_manager import RiskManagerService
from config import zmq_config as config


def main():
    """Runs the risk manager service."""
    # Get logger instance (assuming setup_logging was called in main block)
    logger = getLogger(__name__)

    logger.info("正在初始化风险管理器...")

    # Get connection URLs and limits from config
    md_url = config.MARKET_DATA_PUB_URL.replace("*", "localhost")
    report_url = config.ORDER_REPORT_PUB_URL.replace("*", "localhost")
    limits = config.MAX_POSITION_LIMITS

    risk_manager = RiskManagerService(md_url, report_url, limits)

    logger.info("尝试启动风险管理器...")
    try:
        # The start method contains the main loop
        risk_manager.start()
    except KeyboardInterrupt:
        logger.info("主程序检测到 Ctrl+C，正在停止...")
        # The risk_manager's start loop should catch KeyboardInterrupt and call stop
        if risk_manager.running:
             risk_manager.stop()
    except Exception as e:
        logger.exception(f"风险管理器运行时发生意外错误: {e}")
        if risk_manager.running:
             risk_manager.stop()
    finally:
        logger.info("风险管理器运行结束。")

if __name__ == "__main__":
    # --- Setup Logging --- 
    # Add project root first
    project_root_setup = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root_setup not in sys.path:
        sys.path.insert(0, project_root_setup)
    # Now import logger setup
    try:
        from logger import setup_logging, getLogger
        setup_logging(service_name="RiskManagerRunner") # Set service name
    except ImportError as log_err:
        print(f"CRITICAL: Failed to import or setup logger: {log_err}. Exiting.")
        sys.exit(1)

    main()
