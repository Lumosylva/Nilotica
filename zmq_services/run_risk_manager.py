import time
import sys
import os

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the service class and config
try:
    from zmq_services.risk_manager import RiskManagerService
    from zmq_services import config
except ImportError as e:
    print(f"Error importing modules: {e}")
    print(f"Project root added to path: {project_root}")
    print(f"Current sys.path: {sys.path}")
    print("Ensure zmq_services directory and its files exist.")
    sys.exit(1)

def main():
    """Runs the risk manager service."""
    print("正在初始化风险管理器...")

    # Get connection URLs and limits from config
    md_url = config.MARKET_DATA_PUB_URL.replace("*", "localhost")
    report_url = config.ORDER_REPORT_PUB_URL.replace("*", "localhost")
    limits = config.MAX_POSITION_LIMITS

    risk_manager = RiskManagerService(md_url, report_url, limits)

    print("尝试启动风险管理器...")
    try:
        # The start method contains the main loop
        risk_manager.start()
    except KeyboardInterrupt:
        print("\n主程序检测到 Ctrl+C，正在停止...")
        # The risk_manager's start loop should catch KeyboardInterrupt and call stop
        if risk_manager.running:
             risk_manager.stop()
    except Exception as e:
        print(f"风险管理器运行时发生意外错误: {e}")
        if risk_manager.running:
             risk_manager.stop()
    finally:
        print("风险管理器运行结束。")

if __name__ == "__main__":
    main()
