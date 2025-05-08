import time
import sys
import os
import argparse

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# +++ Import ConfigManager +++
from utils.config_manager import ConfigManager

from utils.logger import setup_logging, logger
from zmq_services.market_data_gateway import MarketDataGatewayService


def main():
    """Runs the market data gateway service (RPC Mode)."""
    parser = argparse.ArgumentParser(description="Run the Market Data Gateway Service for a specific CTP environment.")
    parser.add_argument(
        "--ctp-env",  # Renamed from --env for clarity
        default="simnow",
        help="The CTP environment name defined in connect_ctp.json (or for general service identification). Defaults to 'simnow'."
    )
    # +++ Add --config-env argument +++
    parser.add_argument(
        "--config-env",
        default=None,  # Changed from "dev" to None
        type=str,
        help="The configuration environment to load (e.g., 'dev', 'prod', 'backtest'). Overrides global_config.yaml."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the minimum logging level."
    )
    args = parser.parse_args()

    # Use ctp-env for logger name, or combine if desired
    setup_logging(service_name=f"MarketGatewayRunner[{args.ctp_env}]", level=args.log_level.upper(), config_env=args.config_env)

    # +++ Initialize ConfigManager with config_env +++
    config_service = ConfigManager(environment=args.config_env)

    # --- Continue with application logic ---
    # The original check for default env might still be useful if ctp-env has specific default behavior
    if args.ctp_env == "simnow" and '--ctp-env' not in sys.argv and '--env' not in sys.argv: # Check both old and new name for default message
        logger.info(f"No --ctp-env specified, using default CTP environment: {args.ctp_env}")
    if args.config_env:
        logger.info(f"Using configuration environment: '{args.config_env}'")
    else:
        logger.info("No --config-env specified, using base global_config.yaml only.")


    gateway_service = None # Initialize to None for finally block
    # MarketDataGatewayService now gets its config (like ZMQ addresses) internally via its own ConfigManager instance
    # or if it were to take config_service as a parameter. For now, we assume it instantiates its own or is passed one.
    # The `environment_name` parameter to MarketDataGatewayService was for CTP account details primarily.
    logger.info(f"正在初始化行情网关服务(RPC模式) for CTP environment: [{args.ctp_env}]...")
    try:
        # +++ Pass config_service to the gateway +++
        gateway_service = MarketDataGatewayService(
            config_manager=config_service,
            environment_name=args.ctp_env
        )
        logger.info(f"尝试启动服务(RPC模式) for CTP env: [{args.ctp_env}]...")
        gateway_service.start() # start() method now handles connection and subscription
        logger.info(f"行情网关服务 for CTP env: [{args.ctp_env}] 正在运行。按 Ctrl+C 停止。")

        # Keep the main thread alive while the service runs
        while gateway_service.is_active():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info(f"\n检测到 Ctrl+C，正在停止服务 (CTP env: [{args.ctp_env}])...")
    except Exception as err:
        logger.exception(f"服务运行时发生意外错误 (CTP env: [{args.ctp_env}]): {err}")
    finally:
        logger.info(f"开始停止服务(RPC模式) for CTP env: [{args.ctp_env}]...")
        if gateway_service:
            gateway_service.stop()
        logger.info(f"行情网关服务(RPC模式) for CTP env: [{args.ctp_env}] 已退出。")

if __name__ == "__main__":
    main() 