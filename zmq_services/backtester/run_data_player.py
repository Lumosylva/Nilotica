import sys
import os
import argparse
from datetime import datetime

# +++ Add Logger Import and Setup +++
from utils.logger import logger, setup_logging

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')) # Go up two levels
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the service class and config
from zmq_services.backtester.data_player import DataPlayerService
from config import zmq_config as config


def main():
    """Runs the data player service."""
    parser = argparse.ArgumentParser(description="Backtest Data Player Service")
    parser.add_argument("--date", type=str, default=datetime.now().strftime('%Y%m%d'), help="Playback date in YYYYMMDD format (default: today)")
    parser.add_argument("--speed", type=float, default=0, help="Playback speed multiplier (0=max, 1=realtime, >1 faster, <1 slower)")
    parser.add_argument("--path", type=str, default=config.BACKTEST_DATA_SOURCE_PATH, help=f"Path to recorded data directory (default: {config.BACKTEST_DATA_SOURCE_PATH})")
    # +++ Add log level argument +++
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Set the minimum logging level.")
    args = parser.parse_args()

    # --- Setup Logging --- 
    setup_logging(service_name=f"DataPlayer[{args.date}]", level=args.log_level.upper())

    # Use logger for configuration info
    logger.info(f"--- Data Player Configuration ---")
    logger.info(f"Playback Date: {args.date}")
    logger.info(f"Playback Speed: {args.speed if args.speed > 0 else 'Max'}")
    logger.info(f"Data Source Path: {os.path.abspath(args.path)}")
    logger.info(f"Publishing URL: {config.BACKTEST_DATA_PUB_URL}")
    logger.info(f"--------------------------------")

    data_path = os.path.abspath(args.path)
    pub_url = config.BACKTEST_DATA_PUB_URL

    try:
        player = DataPlayerService(data_path, pub_url, args.date)
    except Exception as e:
        logger.exception(f"初始化 DataPlayerService 时出错: {e}")
        return

    logger.info("尝试加载数据...")
    if player.load_data():
        logger.info("尝试启动回放...")
        try:
            player.start_playback(playback_speed=args.speed)
        except KeyboardInterrupt:
            logger.info("主程序检测到 Ctrl+C，正在停止...")
            # Player should handle its own stop on KeyboardInterrupt in start_playback
        except Exception as e:
            logger.exception(f"回放服务运行时发生意外错误: {e}")
        finally:
            # Ensure player stops if not already stopped
            if player and hasattr(player, 'running') and player.running:
                player.stop()
            logger.info("数据回放服务运行结束。")
    else:
        logger.error("未能加载数据，退出。")

if __name__ == "__main__":
    main()
