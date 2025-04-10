import time
import sys
import os

# Add project root to Python path to find vnpy modules and zmq_services
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the service class
try:
    from zmq_services.market_data_gateway import MarketDataGatewayService
except ImportError as e:
    print(f"Error importing MarketDataGatewayService: {e}")
    print(f"Project root added to path: {project_root}")
    print(f"Current sys.path: {sys.path}")
    print("Ensure the zmq_services directory and its __init__.py exist.")
    sys.exit(1)


def main():
    """Runs the market data gateway service."""
    print("正在初始化行情网关服务...")
    gateway_service = MarketDataGatewayService()

    print("尝试启动服务...")
    gateway_service.start() # start() method now handles connection and subscription

    print("行情网关服务正在运行。按 Ctrl+C 停止。")

    try:
        # Keep the main thread alive while the service runs in background threads (EventEngine)
        while gateway_service.running: # Check the running flag
            time.sleep(1) # Prevent busy-waiting
    except KeyboardInterrupt:
        print("\n检测到 Ctrl+C，正在停止服务...")
    except Exception as e:
        print(f"服务运行时发生意外错误: {e}")
    finally:
        gateway_service.stop()
        print("行情网关服务已退出。")

if __name__ == "__main__":
    main() 