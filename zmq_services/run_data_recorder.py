import sys
import os
import argparse

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the service class and config
from zmq_services.data_recorder import DataRecorderService
from config import zmq_config as config
# Import new logger
from utils.logger import setup_logging, logger


def main():
    """Runs the data recorder service."""
    # --- Argument Parsing ---
    parser = argparse.ArgumentParser(description="Run the Data Recorder Service.")
    parser.add_argument(
        "--env",
        default="simnow", # Recorder usually listens to combined stream, use 'all' or similar
        help="Informational environment tag for the recorder (e.g., 'all', 'simnow')."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the minimum logging level."
    )
    args = parser.parse_args()
    # --- End Argument Parsing ---

    # Setup logging at the beginning of main, using parsed level
    setup_logging(service_name=f"DataRecorderRunner[{args.env}]", level=args.log_level.upper())

    # Log environment being used (informational)
    if args.env == "simnow" and '--env' not in sys.argv:
        logger.info("No --env specified, using default environment: simnow")
    else:
        logger.info(f"Running Data Recorder with environment: {args.env}")

    logger.info("正在初始化数据记录器...")

    # Get connection URLs and recording path from config using RPC addresses
    md_pub_addr = config.MARKET_DATA_PUB_ADDRESS.replace("*", "localhost")
    order_pub_addr = config.ORDER_GATEWAY_PUB_ADDRESS.replace("*", "localhost")
    rec_path = config.DATA_RECORDING_PATH
    # Ensure the path is absolute and uses correct OS separators
    rec_path = os.path.abspath(rec_path)

    recorder = DataRecorderService(md_pub_addr, order_pub_addr, rec_path)

    logger.info("尝试启动数据记录器...")
    try:
        # The start method contains the main loop
        recorder.start()
    except KeyboardInterrupt:
        logger.info("主程序检测到 Ctrl+C，正在停止...")
        # The recorder's start loop should catch KeyboardInterrupt and call stop
        if recorder.running:
            recorder.stop()
    except Exception as e:
        logger.exception(f"数据记录器运行时发生意外错误: {e}")
        if recorder.running:
            recorder.stop()
    finally:
        logger.info("数据记录器运行结束。")

if __name__ == "__main__":
    main()
