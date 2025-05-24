import os
import sys

from utils.service_common import runner_args

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.config_manager import ConfigManager
from utils.logger import logger, setup_logging

from zmq_services.data_recorder import DataRecorderService


def main():
    """
    运行数据记录器服务。

    Runs the data recorder service.
    :return:
    """
    args = runner_args(arg_desc="Run the Data Recorder Service.")

    setup_logging(service_name=f"DataRecorderRunner[{args.ctp_env}]", level=args.log_level.upper(), config_env=args.config_env)

    config_service = ConfigManager(environment=args.config_env)

    logger.info("正在初始化数据记录器...")

    # Get connection URLs and recording path from ConfigManager
    md_pub_addr_raw = config_service.get_global_config("zmq_addresses.market_data_pub")
    order_pub_addr_raw = config_service.get_global_config("zmq_addresses.order_gateway_pub")
    # DataRecorderService itself uses ConfigManager for recorder_batch_size, so no need to pass it here.
    # It *does* need the data_recording_path passed to its constructor.
    rec_path_abs = config_service.get_data_recording_path() # This now returns the absolute path

    if not all([md_pub_addr_raw, order_pub_addr_raw, rec_path_abs]):
        missing = []
        if not md_pub_addr_raw: missing.append("zmq_addresses.market_data_pub")
        if not order_pub_addr_raw: missing.append("zmq_addresses.order_gateway_pub")
        if not rec_path_abs: missing.append("paths.data_recording_path")
        logger.critical(f"错误：未能从配置中获取必要的参数: {', '.join(missing)}。请检查配置。")
        sys.exit(1)
    
    md_connect_addr = md_pub_addr_raw.replace("*", "localhost").replace("0.0.0.0", "localhost")
    order_connect_addr = order_pub_addr_raw.replace("*", "localhost").replace("0.0.0.0", "localhost")

    logger.info(f"Data Recorder connecting to MD: {md_connect_addr}")
    logger.info(f"Data Recorder connecting to Order GW: {order_connect_addr}")
    logger.info(f"Data Recorder saving to path: {rec_path_abs}")

    recorder = None # Initialize for finally block
    try:
        recorder = DataRecorderService(
            config_manager=config_service,
            market_data_pub_addr=md_connect_addr, 
            order_gateway_pub_addr=order_connect_addr, 
            recording_path=rec_path_abs
        )
        logger.info("尝试启动数据记录器...")
        recorder.start()
    except KeyboardInterrupt:
        logger.info("主程序检测到 Ctrl+C，正在停止...")
        if recorder and hasattr(recorder, 'running') and recorder.running:
            recorder.stop()
    except Exception as e:
        logger.exception(f"数据记录器运行时发生意外错误: {e}")
        if recorder and hasattr(recorder, 'running') and recorder.running:
            recorder.stop()
    finally:
        if recorder and hasattr(recorder, 'running') and recorder.running:
             logger.info("执行最终停止清理...")
             recorder.stop()
        logger.info("数据记录器运行结束。")

if __name__ == "__main__":
    main()
