import argparse
import os
import sys

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.config_manager import ConfigManager
from utils.i18n import _

from utils.logger import logger, setup_logging
from zmq_services.risk_manager import RiskManagerService


def main():
    """Runs the risk manager service."""
    parser = argparse.ArgumentParser(description="Run the Risk Manager Service.")
    parser.add_argument(
        "--ctp-env",
        default="simnow", 
        help="The CTP environment name (e.g., 'simnow'). Currently informational for Risk Manager."
    )
    parser.add_argument(
        "--config-env",
        default=None,
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
        logger.info((_("未指定 --ctp-env，使用默认 CTP 环境：{}")).format(args.ctp_env))
    else:
        logger.info((_("风险管理器 CTP 环境（信息性）：{}")).format(args.ctp_env))

    if args.config_env:
        logger.info((_("使用配置环境：'{}'")).format(args.config_env))
    else:
        # This case should not happen if default is "dev"
        logger.info(_("未指定 --config-env，仅使用基本 global_config.yaml。"))

    logger.info(_("正在初始化风险管理器..."))


    risk_manager = RiskManagerService(config_manager=config_service)

    logger.info(_("尝试启动风险管理器..."))
    try:
        risk_manager.start()
    except KeyboardInterrupt:
        logger.info(_("主程序检测到 Ctrl+C，正在停止..."))
        if hasattr(risk_manager, 'running') and risk_manager.running:
             risk_manager.stop()
    except Exception as e:
        logger.exception((_("风险管理器运行时发生意外错误: {}")).format(e))
        if hasattr(risk_manager, 'running') and risk_manager.running:
             risk_manager.stop()
    finally:
        # Ensure stop is called if not already done and object exists
        if 'risk_manager' in locals() and hasattr(risk_manager, 'running') and risk_manager.running:
            logger.info(_("执行最终停止清理..."))
            risk_manager.stop()
        logger.info(_("风险管理器运行结束。"))

if __name__ == "__main__":
    main()
