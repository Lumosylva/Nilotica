import os
import sys
import time

from utils.service_common import runner_args

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.config_manager import ConfigManager
from utils.i18n import _
from utils.logger import logger, setup_logging
from zmq_services.order_execution_gateway import OrderExecutionGatewayService


def main():
    """
    运行交易网关服务（RPC模式）。

    Run the transaction gateway service (RPC mode).
    :return:
    """
    args = runner_args(arg_desc="Run the Order Execution Gateway Service for a specific CTP environment.")

    setup_logging(service_name=f"OrderGatewayRunner[{args.ctp_env}]", level=args.log_level.upper(), config_env=args.config_env)

    config_service = ConfigManager(environment=args.config_env)
    rep_addr = config_service.get_global_config("zmq_addresses.order_gateway_rep")
    pub_addr = config_service.get_global_config("zmq_addresses.order_gateway_pub")
    if not rep_addr or not pub_addr:
        logger.critical(_("错误：未能从配置中获取 zmq_addresses.order_gateway_rep 或 zmq_addresses.order_gateway_pub。请检查配置。"))
        sys.exit(1)
    logger.info(_("订单网关 ZMQ 地址已加载: REP='{}', PUB='{}'").format(rep_addr, pub_addr))
    
    gateway_service = None
    logger.info(_("正在初始化交易网关服务(RPC模式)..."))
    try:
        gateway_service = OrderExecutionGatewayService(
            config_manager=config_service, 
            environment_name=args.ctp_env
        )
        logger.info(_("尝试启动交易网关服务(RPC模式)"))
        gateway_service.start()
        logger.info(_("交易网关正在运行。按 Ctrl+C 停止。"))

        while gateway_service.is_active():
            time.sleep(0.1)
    except KeyboardInterrupt:
        logger.info(_("检测到 Ctrl+C，正在停止服务..."))
    except Exception as err:
        logger.exception(_("服务运行时发生意外错误: {}").format(err))
    finally:
        logger.info(_("开始停止服务(RPC模式)..."))
        if gateway_service:
            gateway_service.stop()
        logger.info(_("交易网关服务(RPC模式)已退出。"))

if __name__ == "__main__":
    main()
