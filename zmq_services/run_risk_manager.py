import sys
import os
import argparse

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from zmq_services.risk_manager import RiskManagerService
from config import zmq_config as config
from utils.logger import setup_logging, logger


def main():
    """Runs the risk manager service."""
    # --- Argument Parsing --- 
    parser = argparse.ArgumentParser(description="Run the Risk Manager Service.")
    parser.add_argument(
        "--env",
        default="simnow", # Keep consistent default, though RM might not use it yet
        help="The CTP environment name (e.g., 'simnow'). Currently informational for Risk Manager."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the minimum logging level."
    )
    args = parser.parse_args()
    # --- End Argument Parsing ---

    # --- Setup Logging --- 
    setup_logging(service_name=f"RiskManagerRunner[{args.env}]", level=args.log_level.upper())
    # --- End Logging Setup --- 

    # Log environment being used
    if args.env == "simnow" and '--env' not in sys.argv:
        logger.info(f"No --env specified, using default environment: simnow")
    else:
        logger.info(f"Running Risk Manager for environment: {args.env}")

    logger.info("正在初始化风险管理器...")

    # Get connection URLs and limits from config
    # Use the correct RPC PUB addresses
    md_url = config.MARKET_DATA_PUB_ADDRESS.replace("*", "localhost")
    report_url = config.ORDER_GATEWAY_PUB_ADDRESS.replace("*", "localhost")
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
    main()
