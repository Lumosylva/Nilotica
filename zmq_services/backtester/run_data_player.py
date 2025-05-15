import argparse
import os
import sys
from datetime import datetime

# +++ Import ConfigManager +++
from utils.config_manager import ConfigManager

# +++ Add Logger Import and Setup +++
from utils.logger import logger, setup_logging

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')) # Go up two levels
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the service class
from zmq_services.backtester.data_player import DataPlayerService

# --- Remove old config import --- 
# from config import zmq_config as config


def main():
    """Runs the data player service."""
    parser = argparse.ArgumentParser(description="Backtest Data Player Service")
    parser.add_argument("--date", type=str, default=datetime.now().strftime('%Y%m%d'), help="Playback date in YYYYMMDD format (default: today)")
    parser.add_argument("--speed", type=float, default=0, help="Playback speed multiplier (0=max, 1=realtime, >1 faster, <1 slower)")
    # --path argument will now get its default from ConfigManager initialized with config-env
    parser.add_argument("--path", type=str, default=None, help="Path to recorded data directory. Overrides path from config.") 
    # +++ Add --config-env argument +++
    parser.add_argument(
        "--config-env",
        default="backtest", # Default to backtest for this service
        type=str,
        help="The configuration environment to load (e.g., 'dev', 'prod', 'backtest'). Affects default data path and ZMQ URLs."
    )
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], help="Set the minimum logging level.")
    args = parser.parse_args()

    # --- Setup Logging with config_env ---
    setup_logging(service_name=f"DataPlayer[{args.date}]", level=args.log_level.upper(), config_env=args.config_env)

    # --- Initialize ConfigManager AFTER parsing args, using config_env ---
    config_service = ConfigManager(environment=args.config_env)

    # Determine data_path: use --path if provided, else from config
    if args.path:
        data_path = os.path.abspath(args.path)
        logger.info(f"Using data source path from command line: {data_path}")
    else:
        data_path = config_service.get_backtest_data_source_path()
        if not data_path:
            logger.error("回测数据源路径 'paths.backtest_data_source_path' 未在配置中找到，且未通过 --path 参数提供。无法启动。")
            sys.exit(1)
        logger.info(f"Using data source path from config ('{args.config_env}' environment): {data_path}")

    # Get pub_url from ConfigManager (now environment-aware)
    pub_url = config_service.get_global_config("zmq_addresses.backtest_data_pub")
    if not pub_url:
        logger.error(f"错误: 'zmq_addresses.backtest_data_pub' 未在配置 ('{args.config_env}' environment) 中定义。 Data Player 无法启动。")
        sys.exit(1)

    # Log effective configurations
    logger.info(f"--- Data Player Effective Configuration (Env: '{args.config_env}') ---")
    logger.info(f"Playback Date: {args.date}")
    logger.info(f"Playback Speed: {args.speed if args.speed > 0 else 'Max'}")
    logger.info(f"Effective Data Source Path: {data_path}")
    logger.info(f"Publishing URL: {pub_url}")
    logger.info(f"--------------------------------")

    player = None # Initialize for finally block
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
        except Exception as e:
            logger.exception(f"回放服务运行时发生意外错误: {e}")
        finally:
            if player and hasattr(player, 'running') and player.running:
                player.stop()
            logger.info("数据回放服务运行结束。")
    else:
        logger.error("未能加载数据，退出。")

if __name__ == "__main__":
    main()
