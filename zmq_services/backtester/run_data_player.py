import time
import sys
import os
import argparse
from datetime import datetime

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')) # Go up two levels
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the service class and config
try:
    from zmq_services.backtester.data_player import DataPlayerService
    from zmq_services import config
except ImportError as e:
    print(f"Error importing modules: {e}")
    print(f"Project root added to path: {project_root}")
    print(f"Current sys.path: {sys.path}")
    print("Ensure zmq_services/backtester directory and its project_files exist.")
    sys.exit(1)

def main():
    """Runs the data player service."""
    parser = argparse.ArgumentParser(description="Backtest Data Player Service")
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now().strftime('%Y%m%d'),
        help="Playback date in YYYYMMDD format (default: today)"
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=0,
        help="Playback speed multiplier (0=max, 1=realtime, >1 faster, <1 slower)"
    )
    parser.add_argument(
        "--path",
        type=str,
        default=config.BACKTEST_DATA_SOURCE_PATH,
        help=f"Path to recorded data directory (default: {config.BACKTEST_DATA_SOURCE_PATH})"
    )
    args = parser.parse_args()

    print(f"--- Data Player Configuration ---")
    print(f"Playback Date: {args.date}")
    print(f"Playback Speed: {args.speed if args.speed > 0 else 'Max'}")
    print(f"Data Source Path: {os.path.abspath(args.path)}")
    print(f"Publishing URL: {config.BACKTEST_DATA_PUB_URL}")
    print(f"--------------------------------")

    data_path = os.path.abspath(args.path)
    pub_url = config.BACKTEST_DATA_PUB_URL

    player = DataPlayerService(data_path, pub_url, args.date)

    print("尝试加载数据...")
    if player.load_data():
        print("尝试启动回放...")
        try:
            player.start_playback(playback_speed=args.speed)
        except KeyboardInterrupt:
            print("\n主程序检测到 Ctrl+C，正在停止...")
            # Player should handle its own stop on KeyboardInterrupt in start_playback
        except Exception as e:
            print(f"回放服务运行时发生意外错误: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Ensure player stops if not already stopped
            if player.running:
                player.stop()
            print("数据回放服务运行结束。")
    else:
        print("未能加载数据，退出。")


if __name__ == "__main__":
    main()
