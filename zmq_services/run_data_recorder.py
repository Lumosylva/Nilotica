import time
import sys
import os

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the service class and config
try:
    from zmq_services.data_recorder import DataRecorderService
    from zmq_services import config
except ImportError as e:
    print(f"Error importing modules: {e}")
    print(f"Project root added to path: {project_root}")
    print(f"Current sys.path: {sys.path}")
    print("Ensure zmq_services directory and its project_files exist.")
    sys.exit(1)

def main():
    """Runs the data recorder service."""
    print("正在初始化数据记录器...")

    # Get connection URLs and recording path from config
    md_url = config.MARKET_DATA_PUB_URL.replace("*", "localhost")
    report_url = config.ORDER_REPORT_PUB_URL.replace("*", "localhost")
    rec_path = config.DATA_RECORDING_PATH
    # Ensure the path is absolute and uses correct OS separators
    rec_path = os.path.abspath(rec_path)

    recorder = DataRecorderService(md_url, report_url, rec_path)

    print("尝试启动数据记录器...")
    try:
        # The start method contains the main loop
        recorder.start()
    except KeyboardInterrupt:
        print("\n主程序检测到 Ctrl+C，正在停止...")
        # The recorder's start loop should catch KeyboardInterrupt and call stop
        if recorder.running:
            recorder.stop()
    except Exception as e:
        print(f"数据记录器运行时发生意外错误: {e}")
        if recorder.running:
            recorder.stop()
    finally:
        print("数据记录器运行结束。")

if __name__ == "__main__":
    main()
