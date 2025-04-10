import time
import sys
import os

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import the subscriber class and config
try:
    from zmq_services.strategy_subscriber import StrategySubscriber
    from zmq_services import config
except ImportError as e:
    print(f"Error importing modules: {e}")
    print(f"Project root added to path: {project_root}")
    print(f"Current sys.path: {sys.path}")
    print("Ensure zmq_services directory and its files exist.")
    sys.exit(1)

def main():
    """Runs the strategy subscriber service."""
    print("正在初始化策略订阅器...")
    
    # Determine the connection URLs
    md_gateway_url = config.MARKET_DATA_PUB_URL.replace("*", "localhost")
    order_req_target_url = config.ORDER_REQUEST_PULL_URL.replace("*", "localhost")
    order_report_source_url = config.ORDER_REPORT_PUB_URL.replace("*", "localhost")
    
    # Get symbols to subscribe to from config
    symbols_to_subscribe = config.SUBSCRIBE_SYMBOLS
    
    subscriber = StrategySubscriber(
        gateway_pub_url=md_gateway_url,
        order_req_url=order_req_target_url,         # Pass the order request URL
        order_report_url=order_report_source_url,   # Pass the order report URL
        subscribe_symbols=symbols_to_subscribe
    )
    
    print("尝试启动订阅器...")
    try:
        # The start method contains the main loop and blocking logic
        subscriber.start()
    except KeyboardInterrupt:
        print("\n主程序检测到 Ctrl+C，正在停止...")
        # The subscriber's start loop should catch KeyboardInterrupt and call stop,
        # but we call it here again just in case.
        if subscriber.running:
            subscriber.stop()
    except Exception as e:
        print(f"订阅器运行时发生意外错误: {e}")
        if subscriber.running:
             subscriber.stop()
    finally:
        print("策略订阅器运行结束。")

if __name__ == "__main__":
    main()
