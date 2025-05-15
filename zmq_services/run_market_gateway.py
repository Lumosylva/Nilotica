import argparse
import os
import sys
import time

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from config.constants.path import GlobalPath  # For project_root fallback
from utils.config_manager import ConfigManager
from utils.i18n import get_translator, setup_language
from utils.logger import logger, setup_logging
from zmq_services.market_data_gateway import MarketDataGatewayService


def main():
    """Runs the market data gateway service (RPC Mode)."""
    parser = argparse.ArgumentParser(description="Run the Market Data Gateway Service for a specific CTP environment.")
    parser.add_argument(
        "--ctp-env",  # Renamed from --env for clarity
        default="simnow",
        help="The CTP environment name defined in connect_ctp.json (or for general service identification). Defaults to 'simnow'."
    )
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
    # +++ Add --lang argument for overriding config language +++
    parser.add_argument(
        "--lang",
        type=str,
        default=None, # Default is None, will use config or i18n default
        help="Language code for i18n (e.g., en, zh_CN). Overrides configuration file setting."
    )
    args = parser.parse_args()

    # +++ Initialize i18n EARLY +++
    # The logic below correctly handles i18n initialization after ConfigManager is ready
    # and command-line arguments are parsed. The initialize_i18n() call was redundant.

    # Now initialize ConfigManager for this script's use
    # This instance is primarily for the MarketDataGatewayService if it needs it.
    config_service = ConfigManager(environment=args.config_env)

    # --- Determine and setup language --- 
    # Priority: command line -> config file -> default 'en'
    language_to_use = args.lang
    if not language_to_use:
        language_to_use = config_service.get_global_config("system.language", "en")
    
    # Get project root for setup_language
    # Preferring PROJECT_ROOT from config_service if it's consistently available and correct.
    # Otherwise, use the one defined in this script or from GlobalPath.
    # For now, assuming the script's `project_root` or GlobalPath is reliable.
    # If ConfigManager exposes a reliable PROJECT_ROOT, that would be ideal: config_service.PROJECT_ROOT
    actual_project_root = getattr(config_service, 'PROJECT_ROOT', project_root) # Use local project_root as fallback
    if not actual_project_root: # Further fallback if needed
        actual_project_root = GlobalPath.project_root_path

    setup_language(language_to_use, actual_project_root)
    
    # --- Define _ for convenience in this main function after language setup ---
    _ = get_translator()

    # Setup logging AFTER i18n is initialized and potentially overridden by --lang
    setup_logging(service_name=f"MarketGatewayRunner[{args.ctp_env}]", level=args.log_level.upper(), config_env=args.config_env)

    # --- Log application startup messages (now using _ for marking translatable strings) ---
    if args.ctp_env == "simnow" and '--ctp-env' not in sys.argv and '--env' not in sys.argv:
        logger.info(_("未指定 --ctp-env，使用默认 CTP 环境：{}"), args.ctp_env)
    if args.config_env:
        logger.info(_("使用配置环境：'{}'"), args.config_env)
    else:
        logger.info(_("未指定 --config-env，仅使用基本 global_config.yaml。"))

    if args.lang:
        logger.info(_("命令行覆盖的语言：{}"), args.lang)
    else:
        logger.info(_("使用配置中的语言（或默认）：{}"), language_to_use)

    gateway_service = None
    logger.info(_("正在为 CTP 环境初始化市场数据网关服务（RPC 模式）：[{}]..."), args.ctp_env)
    try:
        gateway_service = MarketDataGatewayService(
            config_manager=config_service, # Pass the script's ConfigManager instance
            environment_name=args.ctp_env
        )
        logger.info(_("尝试为 CTP 环境启动服务（RPC 模式）：[{}]..."), args.ctp_env)
        gateway_service.start()
        logger.info(_("CTP 环境 [{}] 的市场数据网关服务正在运行。按 Ctrl+C 停止。"), args.ctp_env)

        while gateway_service.is_active():
            time.sleep(0.1)
    except KeyboardInterrupt:
        logger.info(_("检测到 Ctrl+C，正在停止服务 (CTP 环境：[{}])..."), args.ctp_env)
    except Exception as err:
        logger.exception(_("服务运行时错误（CTP 环境：[{}]）：{}"), args.ctp_env, err)
    finally:
        logger.info(_("开始停止 CTP 环境的服务（RPC 模式）：[{}]..."), args.ctp_env)
        if gateway_service:
            gateway_service.stop()
        logger.info(_("CTP 环境 [{}] 的市场数据网关服务 (RPC 模式) 已退出。"), args.ctp_env)

if __name__ == "__main__":
    main() 