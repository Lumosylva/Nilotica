import zmq
import msgpack
import time
import threading
from datetime import datetime

# VNPY imports - Adjust paths if necessary based on your project structure
# Assuming zmq_services is at the same level as vnpy, vnpy_ctp etc.
import sys
import os
# Add project root to Python path to find vnpy modules
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from vnpy.event import EventEngine, Event
    from vnpy.trader.gateway import BaseGateway
    from vnpy.trader.object import TickData, SubscribeRequest, LogData
    from vnpy.trader.event import EVENT_TICK, EVENT_LOG, EVENT_CONTRACT # Import necessary events
    from vnpy.trader.setting import SETTINGS # Use vnpy's global settings if preferred
    from vnpy_ctp import CtpGateway # Import the specific gateway
except ImportError as e:
    print(f"Error importing vnpy modules: {e}")
    print("Please ensure vnpy and vnpy_ctp are installed and accessible.")
    print(f"Project root added to path: {project_root}")
    print(f"Current sys.path: {sys.path}")
    sys.exit(1)

# Import local config
from . import config

# --- Helper Function for Serialization ---
def vnpy_object_to_dict(obj):
    """Converts specific vnpy objects to a dictionary suitable for msgpack."""
    if isinstance(obj, TickData):
        # Explicitly list fields to include and handle conversions
        dt_iso = obj.datetime.isoformat() if obj.datetime else None
        lt_iso = obj.localtime.isoformat() if obj.localtime else None # Handle localtime if present
        exchange_val = obj.exchange.value if hasattr(obj.exchange, 'value') else str(obj.exchange)

        return {
            "gateway_name": obj.gateway_name,
            "symbol": obj.symbol,
            "exchange": exchange_val, # Use converted value
            "datetime": dt_iso,       # Use converted value
            "name": obj.name,
            "volume": obj.volume,
            "turnover": obj.turnover,
            "open_interest": obj.open_interest,
            "last_price": obj.last_price,
            "last_volume": obj.last_volume,
            "limit_up": obj.limit_up,
            "limit_down": obj.limit_down,
            "open_price": obj.open_price,
            "high_price": obj.high_price,
            "low_price": obj.low_price,
            "pre_close": obj.pre_close,
            "bid_price_1": obj.bid_price_1,
            "bid_price_2": obj.bid_price_2,
            "bid_price_3": obj.bid_price_3,
            "bid_price_4": obj.bid_price_4,
            "bid_price_5": obj.bid_price_5,
            "ask_price_1": obj.ask_price_1,
            "ask_price_2": obj.ask_price_2,
            "ask_price_3": obj.ask_price_3,
            "ask_price_4": obj.ask_price_4,
            "ask_price_5": obj.ask_price_5,
            "bid_volume_1": obj.bid_volume_1,
            "bid_volume_2": obj.bid_volume_2,
            "bid_volume_3": obj.bid_volume_3,
            "bid_volume_4": obj.bid_volume_4,
            "bid_volume_5": obj.bid_volume_5,
            "ask_volume_1": obj.ask_volume_1,
            "ask_volume_2": obj.ask_volume_2,
            "ask_volume_3": obj.ask_volume_3,
            "ask_volume_4": obj.ask_volume_4,
            "ask_volume_5": obj.ask_volume_5,
            "localtime": lt_iso,      # Use converted value
            "vt_symbol": obj.vt_symbol
            # Add other fields from TickData if necessary and ensure they are serializable
        }
    # Add handling for other vnpy objects if needed (e.g., BarData, OrderData)
    elif isinstance(obj, datetime): # Handle standalone datetime objects if they appear in data
        return obj.isoformat()
    # Add specific handlers for BarData, OrderData, PositionData etc. if you plan to publish them
    # elif isinstance(obj, BarData): ...
    else:
        # Fallback for basic types that msgpack can handle directly
        if isinstance(obj, (str, int, float, bool, list, tuple, dict, bytes, type(None))):
            return obj
        # Try generic __dict__ as a last resort, but might fail for complex types
        try:
            d = obj.__dict__
            # Recursively convert nested objects (be careful with circular references)
            # This is a simple recursion, might need improvement
            for key, value in d.items():
                 d[key] = vnpy_object_to_dict(value) # Apply conversion recursively
            return d
        except AttributeError:
            # Final fallback for unhandled types
            print(f"Warning: Unhandled type encountered during serialization: {type(obj)}. Converting to string.")
            return str(obj)


# --- Market Data Gateway Service ---
class MarketDataGatewayService:
    def __init__(self):
        """Initializes the gateway service."""
        self.context = zmq.Context()
        self.publisher = self.context.socket(zmq.PUB)
        self.publisher.bind(config.MARKET_DATA_PUB_URL)
        print(f"行情发布器绑定到: {config.MARKET_DATA_PUB_URL}")

        self.event_engine = EventEngine()
        self.gateway: BaseGateway = CtpGateway(self.event_engine, "CTP_MarketData") # Use a unique gateway name

        # Prepare CTP gateway settings from config
        self.ctp_setting = {
            "用户名": config.CTP_USER_ID,
            "密码": config.CTP_PASSWORD,
            "经纪商代码": config.CTP_BROKER_ID,
            "交易服务器": config.CTP_TD_ADDRESS, # Needed for login, even if only using MD
            "行情服务器": config.CTP_MD_ADDRESS,
            "产品名称": config.CTP_PRODUCT_INFO,
            "授权编码": config.CTP_AUTH_CODE,
            # Add other CTP settings if required by your vnpy_ctp version
        }
        # Map keys if vnpy_ctp expects different keys (check CtpGateway.connect)
        # Example mapping if needed:
        # self.ctp_setting = {
        #     "userID": config.CTP_USER_ID,
        #     "password": config.CTP_PASSWORD,
        #     # ... map other keys ...
        # }

        self.running = False
        self._subscribe_list = [] # Store pending subscriptions

        print("行情网关服务初始化完成。")

    def process_event(self, event: Event):
        """Processes events from the EventEngine."""
        event_type = event.type
        if event_type == EVENT_TICK:
            tick: TickData = event.data
            # print(f"收到Tick: {tick.vt_symbol} - Price: {tick.last_price}") # Debug print
            self.publish_data(tick)
        elif event_type == EVENT_LOG:
            log: LogData = event.data
            # Check the type of log.level before accessing .name
            if isinstance(log.level, int):
                level_str = str(log.level) # Use the integer directly
            elif hasattr(log.level, 'name'):
                level_str = log.level.name # Use the enum name if available
            else:
                level_str = str(log.level) # Fallback to string representation
            print(f"[VNPY LOG] {level_str}: {log.msg}")
        elif event_type == EVENT_CONTRACT:
            # Handle contract data if needed, e.g., confirm subscriptions
            pass # Placeholder

    def publish_data(self, data_object):
        """Serializes and publishes vnpy data object via ZeroMQ."""
        if isinstance(data_object, TickData):
            topic_str = f"TICK.{data_object.vt_symbol}"
            message_type = "TICK"
        # Add elif for BarData, etc. if needed
        # elif isinstance(data_object, BarData):
        #    topic_str = f"BAR.{data_object.vt_symbol}.{data_object.interval.value}" # Example
        #    message_type = "BAR"
        else:
            print(f"收到未知类型数据，无法发布: {type(data_object)}")
            return

        topic = topic_str.encode('utf-8')

        # Call the revised helper function BEFORE creating the final message dict
        serializable_vnpy_data = vnpy_object_to_dict(data_object)

        message = {
            "topic": topic_str,
            "type": message_type, # Added type field for easier filtering on subscriber side
            "source": "MarketDataGateway",
            "timestamp": time.time_ns(), # Use ns for higher precision if needed downstream
            "data": serializable_vnpy_data # Use the pre-converted data
        }

        try:
            # Pack the whole message. The 'default' might still catch edge cases if
            # vnpy_object_to_dict's fallback is hit, or if other parts of 'message'
            # contain unexpected types (less likely here).
            # Consider removing 'default' if confident all types are pre-handled.
            packed_message = msgpack.packb(message, default=vnpy_object_to_dict, use_bin_type=True)
            self.publisher.send_multipart([topic, packed_message])
            # print(f"发布: {topic_str}") # Debug print
        except Exception as e:
            print(f"序列化或发布消息时出错 ({topic_str}): {e}")
            # Avoid printing potentially huge raw data in production, maybe log relevant parts
            print(f"原始消息结构 (部分): {{'topic': '{topic_str}', 'type': '{message_type}', 'source': '{message['source']}', ...}}")
            # If you still need to debug the data part:
            # import json
            # try:
            #     print(f"可序列化数据 (JSON): {json.dumps(serializable_vnpy_data, indent=2)}")
            # except Exception as json_e:
            #     print(f"无法将数据转为JSON进行调试: {json_e}")


    def start(self):
        """Starts the event engine and connects the gateway."""
        if self.running:
            print("行情网关服务已在运行中。")
            return

        print("启动行情网关服务...")
        self.running = True
        self.event_engine.register(EVENT_TICK, self.process_event)
        self.event_engine.register(EVENT_LOG, self.process_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_event) # Listen for contract events if needed
        self.event_engine.start()
        print("事件引擎已启动。")

        print("连接 CTP 网关...")
        # Ensure the setting keys match what CtpGateway.connect expects
        # You might need to inspect the CtpGateway code or vnpy documentation
        # Common keys: userID, password, brokerID, tdAddress, mdAddress, productInfo, authCode
        vnpy_settings = {
            "用户名": config.CTP_USER_ID,
            "密码": config.CTP_PASSWORD,
            "经纪商代码": config.CTP_BROKER_ID,
            "交易服务器": config.CTP_TD_ADDRESS,
            "行情服务器": config.CTP_MD_ADDRESS,
            "产品名称": config.CTP_PRODUCT_INFO,
            "授权编码": config.CTP_AUTH_CODE,
            # Add other required fields with default values if needed
             "环境": "实盘" # Or "仿真" based on your brokerID/setup
        }
        try:
            self.gateway.connect(vnpy_settings) # Use the mapped dictionary
            print("CTP 网关连接请求已发送。等待连接成功...")

            # Give some time for connection and login before subscribing
            # A better approach is to wait for a connection status event if vnpy provides one
            time.sleep(10) # Adjust sleep time as needed, or implement event-based waiting

            print("订阅行情...")
            self._subscribe_list = [] # Reset list before subscribing
            for vt_symbol in config.SUBSCRIBE_SYMBOLS:
                try:
                    symbol, exchange_str = vt_symbol.split('.')
                    # Need to import Exchange from vnpy.trader.constant
                    from vnpy.trader.constant import Exchange
                    exchange = Exchange(exchange_str) # Convert string to Exchange enum
                    req = SubscribeRequest(symbol=symbol, exchange=exchange)
                    self.gateway.subscribe(req)
                    self._subscribe_list.append(vt_symbol) # Track successful subscriptions
                    print(f"  发送订阅请求: {vt_symbol}")
                    time.sleep(0.5) # Avoid sending requests too fast
                except ValueError:
                     print(f"  错误的合约格式，跳过订阅: {vt_symbol} (应为 SYMBOL.EXCHANGE)")
                except Exception as e:
                     print(f"  订阅 {vt_symbol} 时出错: {e}")
            print(f"已发送 {len(self._subscribe_list)} 个合约的订阅请求。")

        except Exception as e:
            print(f"连接或订阅 CTP 网关时发生严重错误: {e}")
            self.stop() # Stop if connection fails


    def stop(self):
        """Stops the service and cleans up resources."""
        if not self.running:
            print("行情网关服务未运行。")
            return

        print("停止行情网关服务...")
        self.running = False

        # Check if EventEngine has is_active() public method, otherwise use _active
        # Adjust based on your vnpy version
        try:
             if self.event_engine.is_active(): # Prefer public method if exists
                 self.event_engine.stop()
                 print("事件引擎已停止。")
        except AttributeError:
             if hasattr(self.event_engine, '_active') and self.event_engine._active: # Fallback to private
                 self.event_engine.stop()
                 print("事件引擎已停止。")
             else:
                 print("无法确定事件引擎状态或引擎已停止。")


        if self.gateway:
            self.gateway.close()
            print("CTP 网关已关闭。")

        if self.publisher:
            self.publisher.close()
            print("ZeroMQ 发布器已关闭。")

        if self.context:
            self.context.term() # Use terminate() or term() based on pyzmq version, term() is common
            print("ZeroMQ Context 已终止。")

        print("行情网关服务已停止。")

# --- Main execution block (for testing) ---
# Usually, you'd run this from a separate run script.
if __name__ == "__main__":
    # Simple test run
    gateway_service = MarketDataGatewayService()
    gateway_service.start()

    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("接收到中断信号，正在停止服务...")
        gateway_service.stop()
        print("服务已安全停止。") 