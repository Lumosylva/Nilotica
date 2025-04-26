from typing import Dict, Any, Set

import zmq
import msgpack
import time
import sys
import os
import pickle
from datetime import datetime
import logging
import importlib # For dynamic loading

from zmq_services.strategy_base import BaseLiveStrategy

# Add project root to Python path if needed (e.g., for vnpy types if reconstructing objects)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import new config location
from config import zmq_config as config

# Import vnpy constants if needed for constructing order requests
from vnpy.trader.constant import Direction, OrderType, Exchange, Offset, Status
from vnpy.trader.object import TickData, OrderData, TradeData, AccountData, LogData # Added AccountData, LogData, TickData

from logger import setup_logging, getLogger # Now safe to import

# Constants for Health Checks
from datetime import datetime, time as dt_time # For trading hours check
MARKET_DATA_TIMEOUT = 30.0 # seconds
PING_INTERVAL = 5.0      # seconds
PING_TIMEOUT_MS = 2500   # milliseconds

# 2. Setup Logging for this specific service
# Call this early, before other imports that might log (like MarketDataGatewayService)
setup_logging(service_name="StrategyEngine", level="INFO") # Renamed service name
# 3. Get a logger for this script
logger = getLogger(__name__)

# --- Helper function to reconstruct vnpy objects (Optional for now) ---
# If you need full vnpy objects in the strategy, uncomment and potentially expand this.
# from vnpy.trader.object import TickData
# from vnpy.trader.constant import Exchange
#
# def dict_to_vnpy_object(data_dict, obj_type):
#     """Converts a dictionary back into a vnpy object."""
#     if obj_type == "TICK":
#         try:
#             # Handle datetime conversion from ISO string
#             dt_str = data_dict.get('datetime')
#             dt_obj = datetime.fromisoformat(dt_str) if dt_str else None
#             data_dict['datetime'] = dt_obj
#
#             # Handle localtime conversion
#             lt_str = data_dict.get('localtime')
#             lt_obj = datetime.fromisoformat(lt_str) if lt_str else None
#             data_dict['localtime'] = lt_obj
#
#             # Handle exchange conversion
#             exchange_str = data_dict.get('exchange')
#             exchange_obj = Exchange(exchange_str) if exchange_str else None
#             data_dict['exchange'] = exchange_obj
#
#             # Create TickData object (ensure all necessary fields are present)
#             # This might require more careful handling based on TickData.__init__ args
#             # or by setting attributes after creating a default object.
#             tick = TickData(**data_dict) # This assumes dict keys match TickData attributes
#             return tick
#         except Exception as e:
#             print(f"Error reconstructing TickData object: {e}")
#             print(f"Original data: {data_dict}")
#             return None
#     # Add elif for other types like BarData
#     else:
#         return data_dict # Return dict if type is unknown/unhandled

# --- Strategy Engine (Renamed from StrategySubscriber) ---
class StrategyEngine: # Renamed class
    def __init__(self, gateway_pub_url: str, order_req_url: str, order_report_url: str, strategies_config: Dict[str, Dict[str, Any]]):
        """
        Initializes the strategy engine, loads strategies, and sets up communication.

        Args:
            gateway_pub_url: ZMQ PUB URL for market data.
            order_req_url: ZMQ REQ URL for sending order requests (to Order Gateway REP).
            order_report_url: ZMQ PUB URL for receiving order/trade reports.
            strategies_config: Configuration for strategies to load.
                Example:
                {
                    "SA509_1": {
                        "strategy_class": "zmq_services.strategies.sa509_strategy.SA509LiveStrategy",
                        "vt_symbol": "SA509.CZCE",
                        "setting": {
                            "entry_threshold": 3050, # Example override
                            "order_volume": 2
                        }
                    },
                    # Add other strategies here...
                }
        """
        self.logger = getLogger(__name__) # Use instance logger
        self.context = zmq.Context()

        # --- ZMQ Socket Setup (Same as before) ---
        self.subscriber = self.context.socket(zmq.SUB)
        self.subscriber.setsockopt(zmq.LINGER, 0)
        self.subscriber.connect(gateway_pub_url)
        self.subscriber.connect(order_report_url)
        self.logger.info(f"策略引擎连接行情发布器: {gateway_pub_url}")
        self.logger.info(f"策略引擎连接回报发布器: {order_report_url}")

        self.order_requester = self.context.socket(zmq.REQ)
        self.order_requester.setsockopt(zmq.LINGER, 0)
        self.order_requester.connect(order_req_url)
        self.logger.info(f"策略引擎连接订单网关: {order_req_url}")
        # --- End ZMQ Setup ---

        # --- Strategy Loading and Management ---
        self.strategies: Dict[str, BaseLiveStrategy] = {}
        self.symbol_strategy_map: Dict[str, BaseLiveStrategy] = {} # Map vt_symbol to its strategy instance
        self.subscribed_symbols: Set[str] = set() # Track all symbols needed by strategies

        self.logger.info("开始加载策略...")
        for strategy_name, config_data in strategies_config.items():
            if strategy_name in self.strategies:
                 self.logger.error(f"策略名称 '{strategy_name}' 重复，跳过加载。")
                 continue
            try:
                class_path = config_data["strategy_class"]
                vt_symbol = config_data["vt_symbol"]
                setting = config_data.get("setting", {})

                # Validate vt_symbol format (simple check)
                if '.' not in vt_symbol:
                     self.logger.error(f"策略 '{strategy_name}' 的 vt_symbol '{vt_symbol}' 格式无效，应为 'symbol.exchange'。跳过加载。")
                     continue

                # Dynamically import the strategy class
                module_path, class_name = class_path.rsplit('.', 1)
                module = importlib.import_module(module_path)
                strategy_class = getattr(module, class_name)

                # Check inheritance
                if not issubclass(strategy_class, BaseLiveStrategy):
                     self.logger.error(f"类 '{class_path}' 不是 BaseLiveStrategy 的子类。跳过加载策略 '{strategy_name}'。")
                     continue

                # Create strategy instance
                strategy_instance = strategy_class(
                    strategy_engine=self,       # Pass the engine instance itself
                    strategy_name=strategy_name,
                    vt_symbol=vt_symbol,
                    setting=setting
                )

                self.strategies[strategy_name] = strategy_instance
                # Map the specific vt_symbol to this strategy instance
                if vt_symbol in self.symbol_strategy_map:
                     # Allow multiple strategies per symbol for now, but log warning
                     self.logger.warning(f"vt_symbol '{vt_symbol}' 已被其他策略 '{self.symbol_strategy_map[vt_symbol].strategy_name}' 注册。策略 '{strategy_name}' 也将监听此合约。")
                     # In process_tick etc., we might need to loop through all strategies for a symbol
                     # For simplicity now, keep overwriting. This needs refinement if multi-strat per symbol is desired.
                     # TODO: Refine symbol -> strategy mapping (e.g., list of strategies per symbol)
                self.symbol_strategy_map[vt_symbol] = strategy_instance # Overwrites for now
                self.subscribed_symbols.add(vt_symbol)

                self.logger.info(f"策略 '{strategy_name}' (类: {class_name}, 合约: {vt_symbol}) 加载成功。")

            except ImportError:
                self.logger.exception(f"导入策略类 '{class_path}' 失败。跳过加载策略 '{strategy_name}'。")
            except AttributeError:
                 self.logger.exception(f"在模块 '{module_path}' 中未找到策略类 '{class_name}'。跳过加载策略 '{strategy_name}'。")
            except KeyError as e:
                self.logger.error(f"策略 '{strategy_name}' 的配置缺少必要键: {e}。跳过加载。")
            except Exception as e:
                self.logger.exception(f"加载策略 '{strategy_name}' 时发生未知错误: {e}")
        self.logger.info(f"策略加载完成。共加载 {len(self.strategies)} 个策略实例。")
        if not self.strategies:
             self.logger.error("错误：未成功加载任何策略。引擎无法运行。")
             # Consider exiting or handling this state appropriately
             # For now, allow continuing but log error.
        # --- End Strategy Loading ---


        # --- Update Subscription Logic ---
        self.subscribe_topics = []
        topic_map = {
            "tick": "tick.", "trade": "trade.", "order": "order.",
            "account": "account.", "contract": "contract.", "log": "log"
        }

        if not self.subscribed_symbols:
            self.logger.warning("没有加载任何策略或策略未指定合约，将不订阅任何特定合约数据！")
        else:
            for vt_symbol in self.subscribed_symbols:
                tick_topic = f"{topic_map['tick']}{vt_symbol}"
                # Subscribe to trades related to the symbol might be less efficient than generic prefix
                # trade_topic = f"{topic_map['trade']}{vt_symbol}"
                contract_topic = f"{topic_map['contract']}{vt_symbol}"

                self.subscriber.subscribe(tick_topic.encode('utf-8'))
                # self.subscriber.subscribe(trade_topic.encode('utf-8')) # Subscribe to specific trades
                self.subscriber.subscribe(contract_topic.encode('utf-8'))
                self.subscribe_topics.extend([tick_topic, contract_topic]) # Removed trade_topic here
                self.logger.info(f"  订阅特定合约主题: {tick_topic}, {contract_topic}")

        # Subscribe to generic topics (orders, accounts, logs, trades)
        self.subscriber.subscribe(f"{topic_map['order']}".encode('utf-8')) # Prefix for all orders
        self.subscriber.subscribe(f"{topic_map['trade']}".encode('utf-8')) # Prefix for all trades
        self.subscriber.subscribe(f"{topic_map['account']}".encode('utf-8')) # Prefix for all accounts
        self.subscriber.subscribe(topic_map['log'].encode('utf-8')) # Global logs
        self.subscribe_topics.extend([f"{topic_map['order']}", f"{topic_map['trade']}", f"{topic_map['account']}", topic_map['log']])
        self.logger.info(f"  订阅通用主题前缀: {topic_map['order']}*, {topic_map['trade']}*, {topic_map['account']}*, {topic_map['log']}")

        self.logger.info(f"最终订阅 {len(self.subscribe_topics)} 个主题/前缀。")
        # --- End Subscription Logic ---

        # --- Health Status Flags & Timers (Mostly unchanged) ---
        self.market_data_ok = True
        self.gateway_connected = True # Assumed initially, verified by ping
        self.last_tick_time: Dict[str, float] = {} # Tracks last tick time per symbol
        self.last_ping_time = time.time()
        # --- End Health Status ---

        self.running = False
        # Remove SA509 specific state - managed by strategy instances now
        self.last_order_time = {} # Cool down per symbol, keep at engine level
        # self.last_order_status = {} # Status tracking is now per-strategy + active_orders set
        self.trades = [] # Keep a global list of recent trades? Optional.

        # Initialize loaded strategies
        self.logger.info("正在初始化所有已加载的策略...")
        for strategy in self.strategies.values():
             strategy._init_strategy() # Call internal init method
        self.logger.info("所有策略初始化完成。")

    # --- Helper Methods for Health Checks (adapted from RiskManager) --- 

    def _get_connect_url(self, base_url: str) -> str:
        """Replaces wildcard address with localhost for connection."""
        # This might not be needed anymore if we only connect via RPC addresses
        if base_url.startswith("tcp://*"):
            return base_url.replace("tcp://*", "tcp://localhost", 1)
        elif base_url.startswith("tcp://0.0.0.0"):
            return base_url.replace("tcp://0.0.0.0", "tcp://localhost", 1)
        else:
            return base_url

    # --- Add missing _is_trading_hours method ---
    def _is_trading_hours(self) -> bool:
        """Checks if the current time is within any defined trading session."""
        now_dt = datetime.now()
        current_time = now_dt.time()
        try:
            for start_str, end_str in config.FUTURES_TRADING_SESSIONS:
                start_time = dt_time.fromisoformat(start_str) 
                end_time = dt_time.fromisoformat(end_str)
                if start_time <= end_time: # Simple case: session within the same day
                    if start_time <= current_time < end_time:
                        return True
                # else: # Handle overnight sessions if needed (add complex logic here if required)
                #     if current_time >= start_time or current_time < end_time:
                #          return True
        except Exception as e:
            self.logger.error(f"(Strategy) 检查交易时间时出错: {e}")
            return True # Fail open: assume trading hours if config is wrong
        return False # Not in any session
    # --- End add missing method ---

    # --- Update _send_ping to use order_requester and pickle/RPC format ---
    def _send_ping(self):
        """Sends a PING request to the Order Gateway via RPC and handles the reply."""
        log_prefix = "[Ping OrderGW]" if self.gateway_connected else "[Ping OrderGW - Reconnecting]"
        self.logger.debug(f"{log_prefix} Sending...")

        # Format according to vnpy.rpc: ("ping", (), {})
        req_tuple = ("ping", (), {})
        current_time = time.time()
        self.last_ping_time = current_time

        try:
            # Use Poller for non-blocking send/recv with timeout check
            poller = zmq.Poller()
            poller.register(self.order_requester, zmq.POLLIN)

            # Send PING using pickle with the correct format
            packed_request = pickle.dumps(req_tuple)
            self.order_requester.send(packed_request) # Use blocking send on REQ

            # Wait for PONG reply with timeout
            readable = dict(poller.poll(PING_TIMEOUT_MS))
            if self.order_requester in readable and readable[self.order_requester] & zmq.POLLIN:
                packed_reply = self.order_requester.recv() # Use blocking recv on REQ
                # Decode PONG using pickle - expecting [True, "pong"]
                reply = pickle.loads(packed_reply)

                # Check for the RpcServer success format [True, "pong"]
                if isinstance(reply, (list, tuple)) and len(reply) == 2 and reply[0] is True and reply[1] == "pong":
                    self.logger.debug("Received PONG successfully from OrderGW.")
                    if not self.gateway_connected:
                        self.logger.info("与订单执行网关的连接已恢复。")
                        self.gateway_connected = True
                else:
                    self.logger.warning(f"Received unexpected reply to PING from OrderGW: {reply}")
                    if self.gateway_connected:
                        self.logger.error("与订单执行网关的连接可能存在问题 (Unexpected PING reply)! ")
                        self.gateway_connected = False
            else:
                # Timeout waiting for reply
                self.logger.warning(f"{log_prefix} PING request timed out after {PING_TIMEOUT_MS}ms.")
                if self.gateway_connected:
                    self.logger.error(f"与订单执行网关的连接丢失 (PING timeout)!")
                self.gateway_connected = False
                # No automatic socket reset needed for REQ on timeout, just mark disconnected

        except zmq.ZMQError as e:
            self.logger.error(f"{log_prefix} 发送 PING 或接收 PONG 时 ZMQ 错误: {e}")
            if self.gateway_connected:
                self.logger.error(f"与订单执行网关的连接丢失 ({e})! ")
            self.gateway_connected = False
        except Exception as e:
            self.logger.exception(f"{log_prefix} 发送或处理 PING/PONG 时发生未知错误：{e}")
            if self.gateway_connected:
                self.logger.error("与订单执行网关的连接丢失 (Unknown Error)!")
            self.gateway_connected = False

    # --- End update _send_ping ---

    # --- Update send_limit_order to use order_requester and pickle/RPC format ---
    def send_limit_order(self, symbol: str, exchange: Exchange, direction: Direction, price: float, volume: float, offset: Offset = Offset.NONE):
        """Sends a limit order request to the Order Execution Gateway via RPC."""
        if not self.gateway_connected:
            self.logger.error(f"无法发送订单 ({symbol} {direction.value} {volume} @ {price}): 与订单执行网关失去连接。")
            return None # Indicate failure

        # Apply cooling down period
        cool_down_period = config.ORDER_COOL_DOWN_SECONDS
        last_sent = self.last_order_time.get(symbol, 0)
        if time.time() - last_sent < cool_down_period:
            self.logger.warning(f"订单发送冷却中 ({symbol}): 距离上次发送不足 {cool_down_period} 秒。")
            return None

        self.logger.info(f"准备发送限价单: {symbol}.{exchange.value} {direction.value} {volume} @ {price} Offset: {offset.value}")

        # Construct the request dictionary expected by OrderExecutionGatewayService.send_order
        order_req_dict = {
            "symbol": symbol,
            "exchange": exchange.value, # Send enum value
            "direction": direction.value, # Send enum value
            "type": OrderType.LIMIT.value, # Send enum value
            "volume": float(volume),
            "price": float(price),
            "offset": offset.value, # Send enum value
            "reference": "StrategySub" # Optional reference
        }

        # Format according to vnpy.rpc: (method_name, args_tuple, kwargs_dict)
        req_tuple = ("send_order", (order_req_dict,), {})

        try:
            packed_request = pickle.dumps(req_tuple)
            self.order_requester.send(packed_request)

            # Wait for the reply
            poller = zmq.Poller()
            poller.register(self.order_requester, zmq.POLLIN)
            timeout_ms = 5000 # 5 seconds timeout
            events = dict(poller.poll(timeout_ms))

            if self.order_requester in events:
                packed_reply = self.order_requester.recv()
                reply = pickle.loads(packed_reply)

                # Check RpcServer reply format [True/False, result/traceback]
                if isinstance(reply, (list, tuple)) and len(reply) == 2:
                    if reply[0] is True: # Success
                        vt_orderid = reply[1]
                        self.logger.info(f"订单发送成功 (网关回复): {symbol}, VT_OrderID: {vt_orderid}")
                        self.last_order_time[symbol] = time.time() # Update cool down timer
                        return vt_orderid # Return the order ID
                    else: # Failure
                        error_msg = reply[1]
                        self.logger.error(f"订单发送失败 (网关回复): {symbol}. 错误: {error_msg}")
                        return None
                else:
                    self.logger.error(f"收到来自订单网关的意外回复格式: {reply}")
                    return None
            else:
                self.logger.error(f"发送订单 ({symbol}) 请求超时 ({timeout_ms}ms)。")
                # Mark gateway as disconnected after timeout
                if self.gateway_connected:
                     self.logger.error("与订单执行网关的连接丢失 (Order Send Timeout)!")
                     self.gateway_connected = False
                return None

        except zmq.ZMQError as e:
            self.logger.error(f"发送订单 ({symbol}) 时 ZMQ 错误: {e}")
            if self.gateway_connected:
                 self.logger.error(f"与订单执行网关的连接丢失 (ZMQ Error on Send)!")
                 self.gateway_connected = False
            return None
        except Exception as e:
            self.logger.exception(f"发送或处理订单 ({symbol}) 回复时出错：{e}")
            return None
    # --- End update send_limit_order ---

    # +++ Add cancel_limit_order method using RPC +++
    def cancel_limit_order(self, vt_orderid: str):
        """Sends a cancel order request to the Order Execution Gateway via RPC."""
        if not self.gateway_connected:
            self.logger.error(f"无法发送撤单指令 ({vt_orderid}): 与订单执行网关失去连接。")
            return False # Indicate failure

        if not vt_orderid:
            self.logger.error("尝试发送空 vt_orderid 的撤单请求。")
            return False

        self.logger.info(f"准备发送撤单指令: {vt_orderid}")

        # Construct the request dictionary expected by OrderExecutionGatewayService.cancel_order
        cancel_req_dict = {"vt_orderid": vt_orderid}

        # Format according to vnpy.rpc: (method_name, args_tuple, kwargs_dict)
        req_tuple = ("cancel_order", (cancel_req_dict,), {})

        try:
            packed_request = pickle.dumps(req_tuple)
            self.order_requester.send(packed_request)

            # Wait for the reply
            poller = zmq.Poller()
            poller.register(self.order_requester, zmq.POLLIN)
            timeout_ms = 5000 # 5 seconds timeout
            events = dict(poller.poll(timeout_ms))

            if self.order_requester in events:
                packed_reply = self.order_requester.recv()
                reply = pickle.loads(packed_reply)

                # Check RpcServer reply format [True/False, result/traceback]
                if isinstance(reply, (list, tuple)) and len(reply) == 2:
                    if reply[0] is True: # Success (result is the status dict)
                        status_dict = reply[1]
                        self.logger.info(f"撤单指令发送成功 (网关回复): {vt_orderid}. 回复: {status_dict}")
                        return True
                    else: # Failure
                        error_msg = reply[1]
                        self.logger.error(f"撤单指令发送失败 (网关回复): {vt_orderid}. 错误: {error_msg}")
                        return False
                else:
                    self.logger.error(f"收到来自订单网关的意外撤单回复格式: {reply}")
                    return False
            else:
                self.logger.error(f"发送撤单指令 ({vt_orderid}) 请求超时 ({timeout_ms}ms)。")
                # Mark gateway as disconnected after timeout
                if self.gateway_connected:
                     self.logger.error("与订单执行网关的连接丢失 (Cancel Send Timeout)!")
                     self.gateway_connected = False
                return False

        except zmq.ZMQError as e:
            self.logger.error(f"发送撤单指令 ({vt_orderid}) 时 ZMQ 错误: {e}")
            if self.gateway_connected:
                 self.logger.error(f"与订单执行网关的连接丢失 (ZMQ Error on Cancel)!")
                 self.gateway_connected = False
            return False
        except Exception as e:
            self.logger.exception(f"发送或处理撤单指令 ({vt_orderid}) 回复时出错：{e}")
            return False
    # +++ End add cancel_limit_order +++


    def start(self):
        """Starts listening for messages and running the strategy logic."""
        if self.running:
            self.logger.warning("策略引擎已在运行中。")
            return
        if not self.strategies:
             self.logger.error("没有加载任何策略，引擎无法启动。")
             return

        self.logger.info("启动策略引擎服务...")
        self.running = True

        # Start all loaded strategies
        self.logger.info("正在启动所有已加载的策略...")
        for strategy in self.strategies.values():
            strategy._start_strategy() # Call internal start method
        self.logger.info("所有策略已启动。")

        self.logger.info("策略引擎服务已启动，开始监听消息...")

        while self.running:
            # Poll subscriber socket with a timeout
            poller = zmq.Poller()
            poller.register(self.subscriber, zmq.POLLIN)
            poll_timeout_ms = 1000 # Check every second
            socks = dict(poller.poll(poll_timeout_ms))

            try:
                # --- Process incoming PUB/SUB messages ---
                if self.subscriber in socks and socks[self.subscriber] == zmq.POLLIN:
                    parts = self.subscriber.recv_multipart(zmq.NOBLOCK)

                    if len(parts) == 2:
                        topic_bytes, data_bytes = parts
                    else:
                        self.logger.warning(f"收到包含意外部分数量 ({len(parts)}) 的消息: {parts}")
                        continue

                    if not self.running: break

                    try:
                        topic_str = topic_bytes.decode('utf-8', errors='ignore')
                        # --- Data Deserialization ---
                        # Assuming RpcServer pickled the vnpy objects directly
                        data_obj = pickle.loads(data_bytes)

                        # --- Event Dispatching ---
                        if topic_str.startswith("tick."):
                            vt_symbol = topic_str[len("tick."):]
                            if isinstance(data_obj, TickData):
                                self.process_tick(vt_symbol, data_obj)
                            else:
                                self.logger.warning(f"Received non-TickData object on tick topic {topic_str}: {type(data_obj)}")
                        elif topic_str.startswith("order."):
                            if isinstance(data_obj, OrderData):
                                self.process_order_update(data_obj)
                            else:
                                self.logger.warning(f"Received non-OrderData object on order topic {topic_str}: {type(data_obj)}")
                        elif topic_str.startswith("trade."):
                            if isinstance(data_obj, TradeData):
                                self.process_trade_update(data_obj)
                            else:
                                self.logger.warning(f"Received non-TradeData object on trade topic {topic_str}: {type(data_obj)}")
                        elif topic_str.startswith("account."):
                            if isinstance(data_obj, AccountData):
                                self.process_account_update(data_obj)
                            else:
                                self.logger.warning(f"Received non-AccountData object on account topic {topic_str}: {type(data_obj)}")
                        elif topic_str == "log":
                             if isinstance(data_obj, LogData):
                                self.process_log(data_obj)
                             else:
                                self.logger.warning(f"Received non-LogData object on log topic: {type(data_obj)}")
                        elif topic_str.startswith("contract."):
                             # Contract data might be useful for strategies, dispatch if needed
                             # vt_symbol = topic_str[len("contract."):]
                             # strategy = self.symbol_strategy_map.get(vt_symbol)
                             # if strategy and hasattr(strategy, 'on_contract'):
                             #     strategy.on_contract(data_obj)
                             pass # Ignore contract data for now
                        else:
                            self.logger.warning(f"未知的消息主题: {topic_str}")

                    except pickle.UnpicklingError as e:
                        self.logger.error(f"Pickle 解码错误: {e}. Topic: {topic_bytes.decode('utf-8', errors='ignore')}")
                    except Exception as msg_proc_e:
                        self.logger.exception(f"处理订阅消息时出错 (Topic: {topic_bytes.decode('utf-8', errors='ignore')}): {msg_proc_e}")

                # --- Periodic Health Checks (logic remains the same) ---
                current_time = time.time()
                if current_time - self.last_ping_time >= PING_INTERVAL:
                    self._send_ping()

                if self._is_trading_hours():
                    # Market data timeout check logic...
                    found_stale_symbol = False
                    stale_symbols = []
                    check_time = time.time()
                    for symbol in self.subscribed_symbols:
                        last_ts = self.last_tick_time.get(symbol)
                        if last_ts is None:
                             # Consider stale if never received after a grace period (e.g. 2 * timeout)
                             # Check against engine start time or first ping?
                             # For simplicity, let's use a fixed grace period after first successful ping
                             # This part needs careful design based on expected startup behavior
                             # Simplified: Check if engine running > timeout and no tick received
                             if (check_time - self.last_ping_time > PING_INTERVAL) and self.gateway_connected : # Check if we expect data
                                  # Check if strategy is running and expects data
                                  strategy = self.symbol_strategy_map.get(symbol)
                                  if strategy and strategy.trading:
                                      if check_time - strategy.start_time > MARKET_DATA_TIMEOUT: # Requires strategy.start_time
                                           found_stale_symbol = True
                                           stale_symbols.append(f"{symbol} (no tick received)")
                        elif check_time - last_ts > MARKET_DATA_TIMEOUT:
                            found_stale_symbol = True
                            stale_symbols.append(f"{symbol} (last {check_time - last_ts:.1f}s ago)")

                    if found_stale_symbol:
                        if self.market_data_ok:
                            self.logger.warning(f"[交易时段内] 行情数据可能中断或延迟! 超时合约: {', '.join(stale_symbols)}")
                            self.market_data_ok = False
                    elif not self.market_data_ok:
                        self.logger.info("所有监控合约的行情数据流已恢复。")
                        self.market_data_ok = True


            except zmq.ZMQError as err:
                 if not self.running or err.errno == zmq.ETERM:
                     self.logger.info(f"ZMQ 错误 ({err.errno}) 发生在服务停止期间，正常退出。")
                     break
                 else:
                     self.logger.error(f"主循环中意外的 ZMQ 错误: {err}")
                     self.running = False # Stop on other ZMQ errors
            except KeyboardInterrupt:
                self.logger.info("策略引擎检测到中断信号，开始停止...")
                self.running = False
            except Exception as err:
                self.logger.exception(f"策略引擎主循环发生未知错误：{err}")
                time.sleep(1) # Avoid rapid looping

        self.logger.info("策略引擎主循环结束。")
        self.stop() # Ensure cleanup is called


    # --- Strategy Logic Handlers (Now Dispatchers) ---
    def process_tick(self, vt_symbol: str, tick: TickData):
        """Dispatch tick data to the relevant strategy."""
        self.last_tick_time[vt_symbol] = time.time() # Update timeout tracker

        strategy = self.symbol_strategy_map.get(vt_symbol)
        if strategy:
            # self.logger.debug(f"Dispatching tick for {vt_symbol} to strategy {strategy.strategy_name}")
            try:
                strategy.on_tick(tick)
            except Exception as e:
                 self.logger.exception(f"策略 {strategy.strategy_name} 在 on_tick 处理 {vt_symbol} 时出错: {e}")
        # else: # Log if no strategy found? Can be noisy.
             # self.logger.debug(f"No strategy registered for tick symbol: {vt_symbol}")

    def process_order_update(self, order: OrderData):
        """Dispatch order update to the relevant strategy."""
        vt_symbol = getattr(order, 'vt_symbol', None)
        if not vt_symbol:
            self.logger.warning(f"Order update received without vt_symbol: {order}")
            return

        strategy = self.symbol_strategy_map.get(vt_symbol)
        if strategy:
            # self.logger.debug(f"Dispatching order update {order.vt_orderid} for {vt_symbol} to strategy {strategy.strategy_name}")
             try:
                 strategy.on_order(order)
             except Exception as e:
                 self.logger.exception(f"策略 {strategy.strategy_name} 在 on_order 处理 {order.vt_orderid} 时出错: {e}")
        # else: # May receive orders for symbols not actively managed by strategies here
             # self.logger.debug(f"No strategy registered for order symbol: {vt_symbol}")


    def process_trade_update(self, trade: TradeData):
        """Dispatch trade update to the relevant strategy."""
        vt_symbol = getattr(trade, 'vt_symbol', None)
        if not vt_symbol:
            self.logger.warning(f"Trade update received without vt_symbol: {trade}")
            return

        # Log the trade globally first
        self.logger.info(f"收到成交回报: {trade.vt_symbol} OrderID: {trade.vt_orderid} Offset: {trade.offset.value if hasattr(trade,'offset') else trade.offset} Price: {trade.price} Vol: {trade.volume}")
        self.trades.append(trade) # Optional: store globally

        strategy = self.symbol_strategy_map.get(vt_symbol)
        if strategy:
            # self.logger.debug(f"Dispatching trade update {trade.vt_tradeid} for {vt_symbol} to strategy {strategy.strategy_name}")
            try:
                strategy.on_trade(trade)
            except Exception as e:
                self.logger.exception(f"策略 {strategy.strategy_name} 在 on_trade 处理 {trade.vt_tradeid} 时出错: {e}")
        # else:
             # self.logger.debug(f"No strategy registered for trade symbol: {vt_symbol}")

    def process_account_update(self, account: AccountData):
        """Process account data update (potentially dispatch to all strategies)."""
        accountid = getattr(account, 'accountid', 'N/A')
        balance = getattr(account, 'balance', 0.0)
        available = getattr(account, 'available', 0.0)
        # Log account update globally?
        # self.logger.info(f"账户更新: ID={accountid}, Balance={balance:.2f}, Available={available:.2f}")

        # Dispatch to all strategies if they implement on_account
        for strategy in self.strategies.values():
             if hasattr(strategy, 'on_account'):
                 try:
                     strategy.on_account(account)
                 except Exception as e:
                     self.logger.exception(f"策略 {strategy.strategy_name} 在 on_account 处理时出错: {e}")

    def process_log(self, log: LogData):
        """Processes log messages from gateways (log globally)."""
        gateway_name = getattr(log, 'gateway_name', 'UnknownGW')
        msg = getattr(log, 'msg', '')
        level = getattr(log, 'level', logging.INFO) # Default to INFO

        # Use the engine's logger
        log_func = self.logger.info # Default
        if level == logging.DEBUG: log_func = self.logger.debug
        elif level == logging.WARNING: log_func = self.logger.warning
        elif level == logging.ERROR: log_func = self.logger.error
        elif level == logging.CRITICAL: log_func = self.logger.critical

        log_func(f"[GW LOG - {gateway_name}] {msg}")


    def stop(self):
        """Stops the engine and all loaded strategies."""
        if not self.running:
            # self.logger.warning("策略引擎未运行。") # Can be noisy if called multiple times
            return

        self.logger.info("正在停止策略引擎服务...")
        self.running = False # Signal loops to stop

        # Stop all loaded strategies first
        self.logger.info("正在停止所有已加载的策略...")
        for strategy in self.strategies.values():
             strategy._stop_strategy() # Call internal stop method
        self.logger.info("所有策略已停止。")


        # Close sockets and context (same as before)
        self.logger.info("关闭 ZMQ sockets 和 context...")
        for socket_attr in ['subscriber', 'order_requester']:
            socket = getattr(self, socket_attr, None)
            if socket:
                try:
                    # Check if context is terminated before closing socket
                    if self.context and not self.context.closed:
                         socket.close(linger=0)
                         self.logger.info(f"ZeroMQ socket '{socket_attr}' 已关闭。")
                    else:
                         self.logger.warning(f"Context terminated, skipping close for socket '{socket_attr}'.")
                except Exception as e_close:
                     self.logger.warning(f"关闭 socket '{socket_attr}' 时出错: {e_close}")

        if self.context and not self.context.closed:
            try:
                self.context.term()
                self.logger.info("ZeroMQ Context 已终止。")
            except zmq.ZMQError as e:
                 self.logger.error(f"终止 ZeroMQ Context 时出错 (可能已终止): {e}")
        self.logger.info("策略引擎已停止。")

# --- Main execution block (for testing, now handled by run_strategy_subscriber.py) ---
# if __name__ == "__main__":
#     # Example Usage (Adapt URLs and symbols as needed)
#     md_url = "tcp://localhost:5555"
#     order_req_url = "tcp://localhost:5556"
#     order_report_url = "tcp://localhost:5557"
#     symbols = ["SA505.CZCE"] # Example symbol
#
#     subscriber = StrategySubscriber(md_url, order_req_url, order_report_url, symbols)
#
#     try:
#         subscriber.start()
#     except KeyboardInterrupt:
#         print("\nKeyboardInterrupt detected. Stopping subscriber...")
#     finally:
#         subscriber.stop()
#         print("Subscriber stopped.")
