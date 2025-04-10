import zmq
import msgpack
import time
import threading
import sys
import os
from datetime import datetime

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# VNPY imports
try:
    from vnpy.event import EventEngine, Event
    from vnpy.trader.gateway import BaseGateway
    from vnpy.trader.object import (OrderData, TradeData, OrderRequest, CancelRequest,
                                    LogData, ContractData) # Import necessary objects
    from vnpy.trader.event import (EVENT_ORDER, EVENT_TRADE, EVENT_LOG, EVENT_CONTRACT,
                                   EVENT_TIMER) # Import necessary events
    from vnpy.trader.constant import (Direction, OrderType, Exchange, Offset, Status)
    from vnpy_ctp import CtpGateway # Import the specific gateway
except ImportError as e:
    print(f"Error importing vnpy modules: {e}")
    print("Please ensure vnpy and vnpy_ctp are installed and accessible.")
    print(f"Project root added to path: {project_root}")
    print(f"Current sys.path: {sys.path}")
    sys.exit(1)

# Import local config
from . import config

# --- Helper Functions for Serialization/Deserialization ---

def vnpy_data_to_dict(obj):
    """Converts OrderData or TradeData to a dictionary suitable for msgpack."""
    if isinstance(obj, (OrderData, TradeData)): # Add TickData if needed here too
        d = obj.__dict__
        # Convert Enums and Datetime
        for key, value in d.items():
            if isinstance(value, (Direction, OrderType, Exchange, Offset, Status)):
                d[key] = value.value # Use Enum value
            elif isinstance(value, datetime):
                d[key] = value.isoformat() if value else None
        return d
    elif isinstance(obj, datetime):
         return obj.isoformat()
    # Add handlers for other types if needed
    else:
        # Fallback for basic types
        if isinstance(obj, (str, int, float, bool, list, tuple, dict, bytes, type(None))):
            return obj
        try:
             # Recursive attempt (use with caution)
             d = obj.__dict__
             for key, value in d.items():
                 d[key] = vnpy_data_to_dict(value)
             return d
        except AttributeError:
            print(f"Warning: Unhandled type in vnpy_data_to_dict: {type(obj)}. Converting to string.")
            return str(obj)

def dict_to_order_request(data_dict):
    """Converts a dictionary back into a vnpy OrderRequest object."""
    try:
        # Convert string representations back to enums
        direction = Direction(data_dict['direction'])
        order_type = OrderType(data_dict['type'])
        exchange = Exchange(data_dict['exchange'])
        offset = Offset(data_dict.get('offset', Offset.NONE.value)) # Default to NONE if not provided

        req = OrderRequest(
            symbol=data_dict['symbol'],
            exchange=exchange,
            direction=direction,
            type=order_type,
            volume=data_dict['volume'],
            price=data_dict.get('price', 0.0), # Price might be optional for market orders
            offset=offset,
            reference=data_dict.get('reference', "zmq_gw") # Add a reference
        )
        return req
    except KeyError as e:
        print(f"创建 OrderRequest 失败：缺少关键字段 {e}")
        print(f"原始数据: {data_dict}")
        return None
    except ValueError as e:
        print(f"创建 OrderRequest 失败：无效的枚举值 {e}")
        print(f"原始数据: {data_dict}")
        return None
    except Exception as e:
        print(f"创建 OrderRequest 时发生未知错误: {e}")
        print(f"原始数据: {data_dict}")
        return None

# --- Order Execution Gateway Service ---
class OrderExecutionGatewayService:
    def __init__(self):
        """Initializes the execution gateway service."""
        self.context = zmq.Context()

        # Socket to receive order requests from strategies
        self.order_puller = self.context.socket(zmq.PULL)
        self.order_puller.bind(config.ORDER_REQUEST_PULL_URL)
        print(f"订单请求接收器绑定到: {config.ORDER_REQUEST_PULL_URL}")

        # Socket to publish order/trade updates
        self.report_publisher = self.context.socket(zmq.PUB)
        self.report_publisher.bind(config.ORDER_REPORT_PUB_URL)
        print(f"订单/成交回报发布器绑定到: {config.ORDER_REPORT_PUB_URL}")

        # VNPY setup (similar to market data gateway)
        self.event_engine = EventEngine()
        self.gateway: BaseGateway = CtpGateway(self.event_engine, "CTP_Execution") # Unique gateway name

        # CTP settings (primarily needs TD connection)
        self.ctp_setting = {
            "用户名": config.CTP_USER_ID,
            "密码": config.CTP_PASSWORD,
            "经纪商代码": config.CTP_BROKER_ID,
            "交易服务器": config.CTP_TD_ADDRESS,
            "行情服务器": config.CTP_MD_ADDRESS, # Often needed even for TD-only for contract query
            "产品名称": config.CTP_PRODUCT_INFO,
            "授权编码": config.CTP_AUTH_CODE,
            "环境": "实盘" # Or "仿真"
        }

        self.running = False
        self.req_thread = None # Thread for receiving ZMQ requests

        print("订单执行网关服务初始化完成。")

    def process_vnpy_event(self, event: Event):
        """Processes ORDER and TRADE events from the vnpy EventEngine."""
        event_type = event.type

        if event_type == EVENT_ORDER:
            order: OrderData = event.data
            # print(f"收到订单回报: {order.vt_orderid} - Status: {order.status}") # Debug
            self.publish_report(order, "ORDER_STATUS")
        elif event_type == EVENT_TRADE:
            trade: TradeData = event.data
            # print(f"收到成交回报: {trade.vt_orderid} - Price: {trade.price}, Vol: {trade.volume}") # Debug
            self.publish_report(trade, "TRADE")
        elif event_type == EVENT_LOG:
            log: LogData = event.data
            level_str = log.level.name if hasattr(log.level, 'name') else str(log.level)
            print(f"[VNPY LOG - ExecGW] {level_str}: {log.msg}")
        elif event_type == EVENT_CONTRACT:
             # Contract info might be useful, e.g., for order validation
             contract: ContractData = event.data
             # print(f"收到合约信息: {contract.vt_symbol}") # Debug
             pass # Store contract details if needed

    def publish_report(self, data_object, report_type: str):
        """Serializes and publishes order/trade reports via ZeroMQ."""
        if isinstance(data_object, OrderData):
            topic_str = f"ORDER_STATUS.{data_object.vt_symbol}"
            # Add order ID to topic for finer filtering?
            # topic_str = f"ORDER_STATUS.{data_object.vt_symbol}.{data_object.vt_orderid}"
        elif isinstance(data_object, TradeData):
            topic_str = f"TRADE.{data_object.vt_symbol}"
            # topic_str = f"TRADE.{data_object.vt_symbol}.{data_object.vt_orderid}"
        else:
            print(f"收到未知类型回报，无法发布: {type(data_object)}")
            return

        topic = topic_str.encode('utf-8')
        serializable_data = vnpy_data_to_dict(data_object)

        message = {
            "topic": topic_str,
            "type": report_type,
            "source": "OrderExecutionGateway",
            "timestamp": time.time_ns(),
            "data": serializable_data
        }

        try:
            packed_message = msgpack.packb(message, default=vnpy_data_to_dict, use_bin_type=True)
            self.report_publisher.send_multipart([topic, packed_message])
            # print(f"发布回报: {topic_str}") # Debug
        except Exception as e:
            print(f"序列化或发布回报时出错 ({topic_str}): {e}")
            # Avoid printing large data in production
            print(f"原始回报结构 (部分): {{'topic': '{topic_str}', 'type': '{report_type}', ...}}")

    def process_order_requests(self):
        """Runs in a separate thread to process incoming ZMQ order requests."""
        print("订单请求处理线程已启动。")
        while self.running:
            try:
                # Blocking recv on the PULL socket
                packed_request = self.order_puller.recv()
                if not self.running: # Check again after recv returns
                    break

                request_msg = msgpack.unpackb(packed_request, raw=False)
                request_data = request_msg.get('data')
                print(f"收到订单请求消息: {request_data}") # Debug

                if not request_data:
                    print("错误：收到的订单请求消息缺少 'data' 字段。")
                    continue

                # Convert dict to vnpy OrderRequest
                order_request = dict_to_order_request(request_data)

                if order_request:
                    # Send order via vnpy gateway
                    vt_orderid = self.gateway.send_order(order_request)
                    if vt_orderid:
                        print(f"  订单请求已发送至 CTP 网关: {order_request.symbol}, 本地ID: {vt_orderid}")
                    else:
                        print(f"  错误: CTP 网关未能发送订单请求: {order_request.symbol}")
                        # Optionally publish a rejection status back?
                else:
                    print("  错误: 无法将收到的消息转换为有效的 OrderRequest。")
                    # Optionally publish a rejection status back?

            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    print("订单请求线程：ZMQ Context 已终止。")
                    break # Exit loop cleanly
                else:
                     print(f"订单请求线程 ZMQ 错误: {e}")
                     time.sleep(1)
            except msgpack.UnpackException as e:
                 print(f"订单请求消息解码错误: {e}")
            except Exception as e:
                 print(f"处理订单请求时发生未知错误: {e}")
                 # Log traceback here in production
                 time.sleep(1)

        print("订单请求处理线程已停止。")

    def start(self):
        """Starts the event engine, connects the gateway, and starts the request thread."""
        if self.running:
            print("订单执行网关服务已在运行中。")
            return

        print("启动订单执行网关服务...")
        self.running = True

        # Start vnpy event engine
        self.event_engine.register(EVENT_ORDER, self.process_vnpy_event)
        self.event_engine.register(EVENT_TRADE, self.process_vnpy_event)
        self.event_engine.register(EVENT_LOG, self.process_vnpy_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_vnpy_event)
        self.event_engine.start()
        print("事件引擎已启动。")

        # Connect CTP gateway
        try:
            self.gateway.connect(self.ctp_setting)
            print("CTP 交易网关连接请求已发送。等待连接成功...")
            # Ideally, wait for a connection success log/event
            time.sleep(10) # Simple wait
            print("CTP 交易网关应已连接。")
        except Exception as e:
            print(f"连接 CTP 交易网关时发生严重错误: {e}")
            self.stop()
            return

        # Start the ZMQ request processing thread
        self.req_thread = threading.Thread(target=self.process_order_requests)
        self.req_thread.daemon = True # Allow main thread to exit even if this thread is running
        self.req_thread.start()

        print("订单执行网关服务启动完成。")

    def stop(self):
        """Stops the service and cleans up resources."""
        if not self.running:
            # print("订单执行网关服务未运行。")
            return

        print("停止订单执行网关服务...")
        self.running = False

        # Stop ZMQ request thread first
        # Send a dummy message or close socket to unblock recv?
        # Or rely on self.running flag check within the loop
        # Closing sockets before thread join might be cleaner

        if self.order_puller:
             self.order_puller.close()
             print("ZeroMQ 订单请求接收器已关闭。")
        if self.report_publisher:
             self.report_publisher.close()
             print("ZeroMQ 订单/成交回报发布器已关闭。")

        if self.req_thread and self.req_thread.is_alive():
            print("等待订单请求处理线程退出...")
            self.req_thread.join(timeout=5) # Wait for thread to finish
            if self.req_thread.is_alive():
                print("警告：订单请求处理线程未在超时内退出。")

        # Stop vnpy components
        try:
            if self.event_engine.is_active():
                self.event_engine.stop()
                print("事件引擎已停止。")
        except AttributeError:
            if hasattr(self.event_engine, '_active') and self.event_engine._active:
                self.event_engine.stop()
                print("事件引擎已停止。")

        if self.gateway:
            self.gateway.close()
            print("CTP 交易网关已关闭。")

        if self.context:
             # Check if context is already terminated before terminating again
             # if not self.context.closed:
             #    self.context.term()
             # Simplified: assume term() is idempotent or handles errors
             try:
                 self.context.term()
                 print("ZeroMQ Context 已终止。")
             except zmq.ZMQError as e:
                  print(f"终止 ZeroMQ Context 时出错 (可能已终止): {e}")

        print("订单执行网关服务已停止。")

# --- Main execution block (for testing) ---
if __name__ == "__main__":
    gw_service = OrderExecutionGatewayService()
    gw_service.start()

    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n主程序接收到中断信号，正在停止...")
    finally:
        gw_service.stop()
        print("订单执行网关测试运行结束。")
