import os
import sys
import time
# from pathlib import Path # Removed: FLOW_PATH no longer used

# --- Print CWD and sys.path for diagnostics ---
# (Keep these for now, can be removed later if CWD/sys.path are confirmed not to be the issue)
# print(f"--- Diagnostics for run_market_gateway.py (before sys.path modification) ---")
# print(f"Current Working Directory (CWD): {os.getcwd()}")
# print(f"Initial sys.path:")
# for p_init in sys.path:
# print(f"  {p_init}")
# print(f"--------------------------------------------------")
# --- End initial diagnostics ---

from utils.service_common import runner_args

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- Print CWD and sys.path for diagnostics (after sys.path modification) ---
# print(f"--- Diagnostics for run_market_gateway.py (after sys.path modification) ---")
# print(f"Current Working Directory (CWD) (should be same): {os.getcwd()}")
# print(f"Modified sys.path:")
# for p_mod in sys.path:
# print(f"  {p_mod}")
# print(f"---------------------------------------------------")
# --- End modified diagnostics --

# --- Import necessary components ---
from utils.config_manager import ConfigManager
from utils.i18n import _
from utils.logger import logger, setup_logging
# from vnpy_tts.api import MdApi as TtsMdApiSdk # Removed: Direct TTS test components
# from vnpy.trader.utility import load_json # Removed: Direct TTS test components
# from config.constants.params import Params # Removed: Direct TTS test components

# --- Import the actual service ---
from zmq_services.market_data_gateway import MarketDataGatewayService


# --- REMOVED DIRECT TTS TEST IMPLEMENTATION ---

def main():
    args = runner_args(arg_desc="Run the Market Data Gateway Service for a specific CTP/TTS environment.")
    setup_logging(service_name=f"MarketGatewayRunner[{args.ctp_env}]", level=args.log_level.upper(), config_env=args.config_env)
    logger.info(f"--- Market Data Gateway Service Runner --- PID: {os.getpid()} ---")

    config_service = ConfigManager(environment=args.config_env)
    pub_addr = config_service.get_global_config("zmq_addresses.market_data_pub")
    if not pub_addr:
        logger.critical(_("错误：未能从配置中获取 zmq_addresses.market_data_pub。请检查配置。"))
        sys.exit(1)
    logger.info(_("行情网关 ZMQ PUB 地址已加载: '{}'").format(pub_addr))

    gateway_service = None
    logger.info(_("正在初始化行情网关服务..."))
    try:
        gateway_service = MarketDataGatewayService(
            config_manager=config_service,
            environment_name=args.ctp_env
        )
        logger.info(_("尝试启动行情网关服务..."))
        # Pass the pub_address to the start method
        gateway_service.start(pub_address=pub_addr)
        logger.info(_("行情网关正在运行。按 Ctrl+C 停止。"))

        # Keep alive loop
        while gateway_service.is_active(): # Assuming is_active() reflects ZMQ and main loop state
            time.sleep(0.1) # Keep main thread alive

    except KeyboardInterrupt:
        logger.info(_("检测到 Ctrl+C，正在停止服务..."))
    except Exception as err:
        logger.exception(_("服务运行时发生意外错误: {}").format(err))
    finally:
        logger.info(_("开始停止服务..."))
        if gateway_service:
            gateway_service.stop()
        logger.info(_("行情网关服务已退出。"))

if __name__ == "__main__":
    main()
