import argparse
import os
import sys
import time

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# +++ Import ConfigManager +++
from utils.config_manager import ConfigManager
from utils.i18n import get_translator
from utils.logger import logger, setup_logging
from zmq_services.order_execution_gateway import OrderExecutionGatewayService


def main():
    """
    运行订单执行网关服务（RPC模式）。

    Runs the order execution gateway service (RPC Mode).
    """
    parser = argparse.ArgumentParser(description="Run the Order Execution Gateway Service for a specific CTP environment.")
    parser.add_argument(
        "--ctp-env",
        default="simnow", 
        help="The CTP environment name (e.g., 'simnow', 'simnow7x24') defined in connect_ctp.json. Defaults to 'simnow'."
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

    _ = get_translator()

    setup_logging(service_name=f"OrderGatewayRunner[{args.ctp_env}]", level=args.log_level.upper(), config_env=args.config_env)

    config_service = ConfigManager(environment=args.config_env)
    rep_addr = config_service.get_global_config("zmq_addresses.order_gateway_rep")
    pub_addr = config_service.get_global_config("zmq_addresses.order_gateway_pub")
    if not rep_addr or not pub_addr:
        logger.critical(_("错误：未能从配置中获取 zmq_addresses.order_gateway_rep 或 zmq_addresses.order_gateway_pub。请检查配置。"))
        sys.exit(1)
    logger.info(_("订单网关 ZMQ 地址已加载: REP='{}', PUB='{}'").format(rep_addr, pub_addr))
    
    if args.ctp_env == "simnow" and '--ctp-env' not in sys.argv and '--env' not in sys.argv:
        logger.info(_("未指定 --ctp-env，使用默认 CTP 环境：{}").format(args.ctp_env))
    if args.config_env:
        logger.info(_("使用配置环境：'{}'").format(args.config_env))
    else:
        logger.info(_("未指定 --config-env，仅使用基本 global_config.yaml。"))

    gateway_service = None
    logger.info(_("正在初始化订单执行网关服务(RPC模式) for CTP environment: [{}]...").format(args.ctp_env))
    try:
        gateway_service = OrderExecutionGatewayService(
            config_manager=config_service, 
            environment_name=args.ctp_env
        )
        logger.info(_("尝试启动服务(RPC模式) for CTP env: [{}]...").format(args.ctp_env))
        gateway_service.start()
        logger.info(_("订单执行网关 for CTP env: [{}] 正在运行。按 Ctrl+C 停止。").format(args.ctp_env))

        while gateway_service.is_active():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info(_("检测到 Ctrl+C，正在停止服务 (CTP env: [{}])...").format(args.ctp_env))
    except Exception as err:
        logger.exception(_("服务运行时发生意外错误 (CTP env: [{}]): {}").format(args.ctp_env, err))
    finally:
        logger.info(_("开始停止服务(RPC模式) for CTP env: [{}]...").format(args.ctp_env))
        if gateway_service:
            gateway_service.stop()
        logger.info(_("订单执行网关服务(RPC模式) for CTP env: [{}] 已退出。").format(args.ctp_env))

if __name__ == "__main__":
    main()
