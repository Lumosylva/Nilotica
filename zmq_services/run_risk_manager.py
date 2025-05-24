import os
import sys

from utils.service_common import runner_args

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.config_manager import ConfigManager
from utils.i18n import _

from utils.logger import logger, setup_logging
from zmq_services.risk_manager import RiskManagerService


def main():
    """
    运行风险管理服务。

    Runs the risk manager service.
    :return:
    """
    args = runner_args(arg_desc="Run the Risk Manager Service.")

    setup_logging(service_name=f"RiskManagerRunner[{args.ctp_env}]", level=args.log_level.upper(), config_env=args.config_env)
    
    config_service = ConfigManager(environment=args.config_env)

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
        if 'risk_manager' in locals() and hasattr(risk_manager, 'running') and risk_manager.running:
            logger.info(_("执行最终停止清理..."))
            risk_manager.stop()
        logger.info(_("风险管理器运行结束。"))

if __name__ == "__main__":
    main()
