import sys
import os
import argparse
import time # Import time for potential sleeps if needed

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# +++ Import ConfigManager +++
from utils.config_manager import ConfigManager

from zmq_services.risk_manager import RiskManagerService
# --- Remove old config import --- 
# from config import zmq_config as config
from utils.logger import setup_logging, logger


def main():
    """Runs the risk manager service."""
    parser = argparse.ArgumentParser(description="Run the Risk Manager Service.")
    parser.add_argument(
        "--ctp-env", # Renamed from --env
        default="simnow", 
        help="The CTP environment name (e.g., 'simnow'). Currently informational for Risk Manager."
    )
    # +++ Add --config-env argument +++
    parser.add_argument(
        "--config-env",
        default=None, # Default to dev
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

    # Setup logging with config_env
    setup_logging(service_name=f"RiskManagerRunner[{args.ctp_env}]", level=args.log_level.upper(), config_env=args.config_env)
    
    # +++ Initialize ConfigManager with config_env +++
    # config_service is initialized here so that RiskManagerService can access it internally
    # No need to pass it directly to RiskManagerService if it instantiates its own ConfigManager
    # However, to ensure consistency, if RiskManagerService were to accept a config_service instance,
    # this is where it would be passed from.
    config_service = ConfigManager(environment=args.config_env)

    # Log environments being used
    if args.ctp_env == "simnow" and '--ctp-env' not in sys.argv and '--env' not in sys.argv: # Check both old and new name for default message
        logger.info(f"No --ctp-env specified, using default CTP environment: {args.ctp_env}")
    else:
        logger.info(f"Risk Manager CTP environment (informational): {args.ctp_env}")

    if args.config_env:
        logger.info(f"Using configuration environment: '{args.config_env}'")
    else:
        # This case should not happen if default is "dev"
        logger.info("No --config-env specified, using base global_config.yaml only.")

    logger.info("正在初始化风险管理器...")

    # RiskManagerService now loads its own config internally using ConfigManager.
    # The following lines that fetched specific configs here are no longer needed for RiskManagerService instantiation.
    # md_pub_addr = config_service.get_global_config("zmq_addresses.market_data_pub", "tcp://*:5555")
    # order_report_pub_addr = config_service.get_global_config("zmq_addresses.order_gateway_pub", "tcp://*:5557")
    # max_pos_limits = config_service.get_global_config("risk_management.max_position_limits", {})
    # if not md_pub_addr or not order_report_pub_addr:
    #     logger.critical("错误：未能从配置中获取 market_data_pub 或 order_gateway_pub 地址。")
    #     sys.exit(1)
    # if not max_pos_limits:
    #     logger.warning("警告：未在配置中找到 risk_management.max_position_limits，持仓限制将不生效。")
    # md_url = md_pub_addr.replace("*", "localhost")
    # report_url = order_report_pub_addr.replace("*", "localhost")
    # logger.info(f"Risk Manager will connect to MD: {md_url}, Reports: {report_url}")
    # logger.info(f"Max Position Limits: {max_pos_limits}")

    risk_manager = RiskManagerService(config_manager=config_service) # +++ Pass config_service

    logger.info("尝试启动风险管理器...")
    try:
        risk_manager.start()
    except KeyboardInterrupt:
        logger.info("主程序检测到 Ctrl+C，正在停止...")
        if hasattr(risk_manager, 'running') and risk_manager.running:
             risk_manager.stop()
    except Exception as e:
        logger.exception(f"风险管理器运行时发生意外错误: {e}")
        if hasattr(risk_manager, 'running') and risk_manager.running:
             risk_manager.stop()
    finally:
        # Ensure stop is called if not already done and object exists
        if 'risk_manager' in locals() and hasattr(risk_manager, 'running') and risk_manager.running:
            logger.info("执行最终停止清理...")
            risk_manager.stop()
        logger.info("风险管理器运行结束。")

if __name__ == "__main__":
    main()
