import time
import sys
import os
import argparse

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import new logger tools
from utils.logger import setup_logging, logger

# Import the service class
from zmq_services.market_data_gateway import MarketDataGatewayService


def main():
    """Runs the market data gateway service (RPC Mode)."""

    parser = argparse.ArgumentParser(description="Run the Market Data Gateway Service for a specific CTP environment.")
    parser.add_argument(
        "--env",
        default="simnow",
        help="The CTP environment name defined in connect_ctp.json. Defaults to 'simnow'."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the minimum logging level."
    )
    args = parser.parse_args()

    # --- Configure Logging ---
    setup_logging(service_name="RunMarketDataGateway")

    # --- Continue with application logic ---
    if args.env == "simnow" and '--env' not in sys.argv:
        logger.info("No --env specified, using default environment: simnow")

    logger.info(f"正在初始化行情网关服务 for environment: [{args.env}]...")
    gateway_service = None # Initialize to None for finally block
    try:
        gateway_service = MarketDataGatewayService(environment_name=args.env)

        logger.info(f"尝试启动服务 for [{args.env}]...")
        gateway_service.start() # start() method now handles connection and subscription

        logger.info(f"行情网关服务 for [{args.env}] 正在运行。按 Ctrl+C 停止。")

        # Keep the main thread alive
        while gateway_service.is_active():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info(f"\n检测到 Ctrl+C，正在停止服务 [{args.env}]...")
    except Exception as err:
        logger.exception(f"服务运行时发生意外错误 [{args.env}]: {err}") # Use logger.exception
    finally:
        logger.info(f"开始停止服务 for [{args.env}]...")
        if gateway_service:
            gateway_service.stop()
        logger.info(f"行情网关服务 for [{args.env}] 已退出。")

if __name__ == "__main__":
    main() 