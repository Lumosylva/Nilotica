import zmq
import msgpack
import time
import threading
import sys
import os
from datetime import datetime
from typing import Dict, Tuple
import configparser

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

# --- 移除硬编码规则 ---
# COMMISSION_RULES = { ... }
# --- 结束移除 ---

# +++ 添加函数：加载产品信息 +++
def load_product_info(filepath: str) -> Tuple[Dict, Dict]:
    """Loads commission rules and multipliers from an INI file."""
    parser = configparser.ConfigParser()
    if not os.path.exists(filepath):
        print(f"错误：产品信息文件未找到 {filepath}")
        return {}, {}

    try:
        parser.read(filepath, encoding='utf-8')
    except Exception as e:
        print(f"错误：读取产品信息文件 {filepath} 时出错: {e}")
        return {}, {}

    commission_rules = {}
    contract_multipliers = {}

    for symbol in parser.sections():
        if not parser.has_option(symbol, 'multiplier'):
            print(f"警告：文件 {filepath} 中的 [{symbol}] 缺少 'multiplier'，跳过此合约。")
            continue
        
        try:
            multiplier = parser.getfloat(symbol, 'multiplier')
            contract_multipliers[symbol] = multiplier

            # 解析手续费规则 (允许部分缺失，使用 getfloat/getint 并提供默认值 0)
            rule = {
                "open_rate": parser.getfloat(symbol, 'open_rate', fallback=0.0),
                "close_rate": parser.getfloat(symbol, 'close_rate', fallback=0.0),
                "open_fixed": parser.getfloat(symbol, 'open_fixed', fallback=0.0),
                "close_fixed": parser.getfloat(symbol, 'close_fixed', fallback=0.0),
                "min_commission": parser.getfloat(symbol, 'min_commission', fallback=0.0)
                # 可以添加 closetoday_rate/fixed 等如果你的INI文件包含它们
            }
            commission_rules[symbol] = rule
            # print(f"加载 {symbol}: Multiplier={multiplier}, Rules={rule}") # Debug
        except ValueError as e:
            print(f"警告：解析文件 {filepath} 中 [{symbol}] 的数值时出错: {e}，跳过此合约。")
        except Exception as e:
            print(f"警告：处理文件 {filepath} 中 [{symbol}] 时发生未知错误: {e}，跳过此合约。")
            
    print(f"从 {filepath} 加载了 {len(contract_multipliers)} 个合约的乘数和 {len(commission_rules)} 个合约的手续费规则。")
    return commission_rules, contract_multipliers
# +++ 结束添加 +++

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
        self.contracts: Dict[str, ContractData] = {}

        # +++ 加载产品信息 +++
        # 修正路径：从 zmq_services 目录出发，向上一级到项目根目录，再进入 config
        config_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config'))
        info_filepath = os.path.join(config_dir, 'project_files', 'product_info.ini')
        print(f"尝试从 {info_filepath} 加载产品信息...")
        self.commission_rules, self.contract_multipliers = load_product_info(info_filepath)
        if not self.commission_rules or not self.contract_multipliers:
             print("警告：未能成功加载手续费规则或合约乘数，手续费计算可能不准确！")
        # +++ 结束加载 +++

        print("订单执行网关服务初始化完成。")

    def process_vnpy_event(self, event: Event):
        """Processes ORDER, TRADE, LOG, and CONTRACT events."""
        event_type = event.type

        if event_type == EVENT_ORDER:
            order: OrderData = event.data
            self.publish_report(order, "ORDER_STATUS")
        elif event_type == EVENT_TRADE:
            trade: TradeData = event.data
            calculated_commission = 0.0
            
            # +++ 使用加载的规则计算手续费 +++
            contract = self.contracts.get(trade.vt_symbol)
            # 注意：手续费规则使用 symbol (e.g., "SA505"), 合约信息使用 vt_symbol
            rules = self.commission_rules.get(trade.symbol)

            if contract and rules:
                # 获取乘数 (这里不再需要，因为计算时直接用 contract.size)
                # multiplier = self.contract_multipliers.get(trade.symbol, 1) # 确保乘数存在
                
                turnover = trade.price * trade.volume * contract.size
                rate = 0.0
                fixed = 0.0
                min_comm = rules.get('min_commission', 0.0)
                
                if trade.offset == Offset.OPEN:
                    rate = rules.get('open_rate', 0.0)
                    fixed = rules.get('open_fixed', 0.0)
                elif trade.offset in [Offset.CLOSE, Offset.CLOSETODAY, Offset.CLOSEYESTERDAY]: 
                    rate = rules.get('close_rate', 0.0)
                    fixed = rules.get('close_fixed', 0.0)
                else:
                    print(f"警告: 未知的开平仓方向 {trade.offset} 用于手续费计算，按开仓处理。")
                    rate = rules.get('open_rate', 0.0)
                    fixed = rules.get('open_fixed', 0.0)

                if fixed > 0:
                    calculated_commission = trade.volume * fixed
                elif rate > 0:
                    calculated_commission = turnover * rate
                
                calculated_commission = max(calculated_commission, min_comm)
                print(f"收到成交回报: {trade.vt_tradeid}, 使用加载规则计算手续费: {calculated_commission:.2f}") # 更新日志
            
            elif not contract:
                print(f"警告: 未找到成交 {trade.vt_tradeid} 对应的合约信息 ({trade.vt_symbol})，手续费计为 0。")
            elif not rules:
                 print(f"警告: 未找到成交 {trade.vt_tradeid} 对应的手续费规则 ({trade.symbol})，手续费计为 0。")
            # +++ 结束计算 +++

            self.publish_report(trade, "TRADE", calculated_commission=calculated_commission)
        elif event_type == EVENT_LOG:
            log: LogData = event.data
            level_str = log.level.name if hasattr(log.level, 'name') else str(log.level)
            print(f"[VNPY LOG - ExecGW] {level_str}: {log.msg}")
        elif event_type == EVENT_CONTRACT:
             contract: ContractData = event.data
             self.contracts[contract.vt_symbol] = contract # Store contract details
             # print(f"收到合约信息: {contract.vt_symbol}") # Debug

    def publish_report(self, data_object, report_type: str, calculated_commission: float = None):
        """Serializes and publishes reports via ZeroMQ, adding calculated commission if provided."""
        if isinstance(data_object, OrderData):
            topic_str = f"ORDER_STATUS.{data_object.vt_symbol}"
        elif isinstance(data_object, TradeData):
            topic_str = f"TRADE.{data_object.vt_symbol}"
        else:
            print(f"收到未知类型回报，无法发布: {type(data_object)}")
            return

        topic = topic_str.encode('utf-8')
        serializable_data = vnpy_data_to_dict(data_object)

        # +++ 添加计算出的手续费 +++
        if report_type == "TRADE" and calculated_commission is not None:
            serializable_data['calculated_commission'] = calculated_commission
            # print(f"  添加 calculated_commission: {calculated_commission} 到回报数据") # Debug
        # +++ 结束添加 +++

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
        except Exception as e:
            print(f"序列化或发布回报时出错 ({topic_str}): {e}")
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
                    # +++ 检查合约是否存在 (带重试) +++
                    vt_symbol = f"{order_request.symbol}.{order_request.exchange.value}"
                    contract = self.contracts.get(vt_symbol)
                    if not contract:
                        # 如果合约不存在，稍等片刻重试几次
                        max_retries = 5
                        retry_delay = 0.2 # 秒
                        for i in range(max_retries):
                            print(f"警告: 合约 {vt_symbol} 尚未缓存，等待 {retry_delay} 秒后重试 ({i+1}/{max_retries})...")
                            time.sleep(retry_delay)
                            contract = self.contracts.get(vt_symbol)
                            if contract:
                                print(f"信息: 重试成功，找到合约 {vt_symbol}。")
                                break # 找到合约，跳出重试循环
                        
                        # 如果重试后仍然找不到
                        if not contract:
                            print(f"错误: 尝试下单的合约 {vt_symbol} 信息不存在 (已重试 {max_retries} 次)。订单无法发送。")
                            # 可以选择性地发送拒绝回报给策略
                            # self.send_rejection_report(order_request, "合约不存在")
                            continue # 跳过此订单
                    # +++ 结束检查 +++

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
