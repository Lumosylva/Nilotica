import zmq
import time
import threading
import sys
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, Tuple, Any
import configparser
import logging
import pickle
import queue
import math

from vnpy.trader.utility import load_json

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from logger import setup_logging, getLogger

# VNPY imports
try:
    from vnpy.event import EventEngine, Event
    from vnpy.trader.gateway import BaseGateway
    from vnpy.trader.object import (OrderData, TradeData, OrderRequest, CancelRequest,
                                    LogData, ContractData, AccountData, PositionData)
    from vnpy.trader.event import (EVENT_ORDER, EVENT_TRADE, EVENT_LOG, EVENT_CONTRACT, EVENT_ACCOUNT, EVENT_POSITION)
    from vnpy.trader.constant import (Direction, OrderType, Exchange, Offset, Status)
    from vnpy_ctp import CtpGateway
    from vnpy.rpc import RpcServer
except ImportError as e:
    try:
        getLogger(__name__).critical(f"Error importing vnpy modules: {e}", exc_info=True)
    except Exception as e:
        print(f"CRITICAL: Error importing vnpy modules: {e}")
        print("Please ensure vnpy and vnpy_ctp are installed and accessible.")
        print(f"CRITICAL: Project root added to path: {project_root}")
        print(f"CRITICAL: Current sys.path: {sys.path}")
    sys.exit(1)

# Import new config location
from config import zmq_config as config
# Import constants from config
from config.zmq_config import PUBLISH_BATCH_SIZE, CHINA_TZ # HEARTBEAT_INTERVAL_S likely unused

# +++ 添加函数：加载产品信息 +++
def load_product_info(filepath: str) -> Tuple[Dict, Dict]:
    """Loads commission rules and multipliers from an INI file."""
    logger = getLogger('load_product_info')
    parser = configparser.ConfigParser()
    if not os.path.exists(filepath):
        logger.error(f"错误：产品信息文件未找到 {filepath}")
        return {}, {}
    try:
        parser.read(filepath, encoding='utf-8')
    except Exception as err:
        logger.exception(f"错误：读取产品信息文件 {filepath} 时出错：{err}")
        return {}, {}
    commission_rules = {}
    contract_multipliers = {}
    for symbol in parser.sections():
        if not parser.has_option(symbol, 'multiplier'):
            logger.warning(f"警告：文件 {filepath} 中的 [{symbol}] 缺少 'multiplier'，跳过此合约。")
            continue
        try:
            multiplier = parser.getfloat(symbol, 'multiplier')
            contract_multipliers[symbol] = multiplier
            rule = {
                "open_rate": parser.getfloat(symbol, 'open_rate', fallback=0.0),
                "close_rate": parser.getfloat(symbol, 'close_rate', fallback=0.0),
                "open_fixed": parser.getfloat(symbol, 'open_fixed', fallback=0.0),
                "close_fixed": parser.getfloat(symbol, 'close_fixed', fallback=0.0),
                "min_commission": parser.getfloat(symbol, 'min_commission', fallback=0.0)
            }
            commission_rules[symbol] = rule
        except ValueError as err:
            logger.warning(f"警告：解析文件 {filepath} 中 [{symbol}] 的数值时出错: {err}，跳过此合约。")
        except Exception as err:
            logger.warning(f"警告：处理文件 {filepath} 中 [{symbol}] 时发生未知错误: {err}，跳过此合约。")
    logger.info(f"从 {filepath} 加载了 {len(contract_multipliers)} 个合约的乘数和 {len(commission_rules)} 个合约的手续费规则。")
    return commission_rules, contract_multipliers
# +++ 结束添加 +++

# --- Helper Functions for Serialization/Deserialization ---

def vnpy_data_to_dict(obj):
    logger = getLogger('vnpy_data_to_dict')
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
    # Add AccountData handler
    elif isinstance(obj, AccountData):
        d = obj.__dict__
        # Convert Enums if any in AccountData (check AccountData definition)
        # Example: if hasattr(obj, 'account_type') and isinstance(obj.account_type, SomeEnum):
        #    d['account_type'] = obj.account_type.value
        return d
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
            logger.warning(f"Warning: Unhandled type in vnpy_data_to_dict: {type(obj)}. Converting to string.")
            return str(obj)

def dict_to_order_request(data_dict: Dict[str, Any]) -> OrderRequest | None:
    """Converts a dictionary back into a vnpy OrderRequest object."""
    logger = getLogger('dict_to_order_request')
    try:
        direction = Direction(data_dict['direction'])
        order_type = OrderType(data_dict['type'])
        exchange = Exchange(data_dict['exchange'])
        offset = Offset(data_dict.get('offset', Offset.NONE.value))

        req = OrderRequest(
            symbol=data_dict['symbol'],
            exchange=exchange,
            direction=direction,
            type=order_type,
            volume=data_dict['volume'],
            price=data_dict.get('price', 0.0),
            offset=offset,
            reference=data_dict.get('reference', "rpc_gw")
        )
        return req
    except KeyError as err:
        logger.error(f"创建 OrderRequest 失败：缺少关键字段：{err} from data {data_dict}")
        return None
    except ValueError as err:
        logger.error(f"创建 OrderRequest 失败：无效的枚举值：{err} from data {data_dict}")
        return None
    except Exception as err:
        logger.exception(f"创建 OrderRequest 时发生未知错误：{err} from data {data_dict}")
        return None

# --- Constants (Removed local definitions) ---
# HEARTBEAT_INTERVAL_S = 1.0
# PUBLISH_BATCH_SIZE = 1000 
# CHINA_TZ = ZoneInfo("Asia/Shanghai") 

# --- Order Execution Gateway Service (RPC Mode) ---
class OrderExecutionGatewayService(RpcServer):
    """
    Order execution gateway service using CtpGateway and RpcServer.
    Handles order sending/canceling via RPC calls and publishes order/trade updates.
    Uses a dedicated publisher thread for thread-safety.
    """
    def __init__(self, environment_name: str):
        """Initializes the execution gateway service for a specific environment."""
        super().__init__()
        self.logger = getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.logger.info(f"Initializing OrderExecutionGatewayService for environment: [{environment_name}]...")
        self.environment_name = environment_name # Store for logging

        self._event_counter = 0
        self._counter_lock = threading.Lock()
        self._last_logged_account_values: Dict[str, Tuple[float, float]] = {}
        self.active_order_ids: set[str] = set()

        # VNPY setup
        self.event_engine = EventEngine()
        # Add env name to gateway name for potential clarity if running multiple
        self.gateway: CtpGateway = CtpGateway(self.event_engine, f"CTP_Exec_{environment_name}")

        # --- Load and Select CTP Settings --- 
        self.ctp_setting: dict | None = None
        try:
            all_ctp_settings = load_json("connect_ctp.json")
            if environment_name in all_ctp_settings:
                self.ctp_setting = all_ctp_settings[environment_name]
                self.logger.info(f"Loaded CTP settings for environment: [{environment_name}]")
            else:
                self.logger.error(f"Environment '{environment_name}' not found in connect_ctp.json! Cannot connect CTP.")
        except FileNotFoundError:
            self.logger.error("connect_ctp.json not found! Cannot connect CTP.")
        except Exception as e:
            self.logger.exception(f"Error loading or parsing connect_ctp.json: {e}")
        # --- End Load and Select --- 

        self.contracts: Dict[str, ContractData] = {}
        self.last_account_data: AccountData | None = None

        # Load product info for commission calculation
        config_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config'))
        info_filepath = os.path.join(config_dir, 'project_files', 'product_info.ini')
        self.logger.info(f"尝试从 {info_filepath} 加载产品信息...")
        self.commission_rules, self.contract_multipliers = load_product_info(info_filepath)
        if not self.commission_rules or not self.contract_multipliers:
             self.logger.warning("警告：未能成功加载手续费规则或合约乘数，手续费计算可能不准确！")

        # Register RPC handlers
        self.register(self.send_order)
        self.register(self.cancel_order)
        self.register(self.query_contracts)
        self.register(self.query_account)
        self.register(self.ping)

        # Publisher Queue
        self._publish_queue = queue.Queue()

        # Publisher Thread
        self._publisher_active = threading.Event()
        self._publisher_thread = None

        self.logger.info(f"订单执行网关服务(RPC模式) for [{environment_name}] 初始化完成。")

    # --- RPC Handlers ---
    def send_order(self, req_dict: Dict[str, Any]) -> str | None:
        """
        RPC handler for sending an order request.
        Returns vt_orderid on success, None on failure.
        """
        self.logger.info(f"RPC: 收到 send_order 请求: {req_dict}")
        order_request: OrderRequest | None = dict_to_order_request(req_dict)

        if not order_request:
            self.logger.error("RPC: send_order - 无法将请求转换为 OrderRequest。")
            return None

        try:
            vt_orderid = self.gateway.send_order(order_request)
            if vt_orderid:
                self.logger.info(f"RPC: send_order - 订单已发送至 CTP: {order_request.symbol}, VT_OrderID: {vt_orderid}")
                self.active_order_ids.add(vt_orderid)
                return vt_orderid
            else:
                # Log if the gateway itself failed to send without raising an exception
                self.logger.error(f"RPC: send_order - CTP 网关未能发送订单 (gateway.send_order 返回 None): {order_request.symbol}")
                return None
        except Exception as e:
            # Log exceptions raised by the gateway's send_order
            self.logger.exception(f"RPC: send_order - 发送订单时 CTP 网关出错: {e}")
            return None

    def cancel_order(self, req_dict: Dict[str, Any]) -> Dict[str, str]:
        """
        RPC handler for cancelling an order.
        Requires 'vt_orderid' in the request dictionary.
        Returns a status dictionary.
        """
        vt_orderid = req_dict.get('vt_orderid')
        self.logger.info(f"RPC: 收到 cancel_order 请求 for {vt_orderid}")
        if not vt_orderid:
            self.logger.error("RPC: cancel_order - 收到无效的撤单指令：缺少 'vt_orderid' 字段。")
            return {"status": "error", "message": "Missing vt_orderid"}

        try:
            order_to_cancel = self.gateway.get_order(vt_orderid)
            if not order_to_cancel:
                self.logger.error(f"RPC: cancel_order - 尝试撤销失败：网关未找到订单 {vt_orderid}")
                return {"status": "error", "message": f"Order {vt_orderid} not found in gateway"}

            req = CancelRequest(
                orderid=order_to_cancel.orderid,
                symbol=order_to_cancel.symbol,
                exchange=order_to_cancel.exchange,
            )
            self.gateway.cancel_order(req)
            self.logger.info(f"RPC: cancel_order - 撤单请求已发送 for {vt_orderid}")
            return {"status": "ok", "message": f"Cancel request sent for {vt_orderid}"}

        except AttributeError:
             self.logger.error("RPC: cancel_order - 当前网关对象不支持 get_order 方法。")
             return {"status": "error", "message": "Gateway does not support get_order, cannot cancel."}
        except Exception as e:
            self.logger.exception(f"RPC: cancel_order - 处理撤单指令时出错 (vt_orderid: {vt_orderid}): {e}")
            return {"status": "error", "message": f"Error processing cancel command: {e}"}

    def query_contracts(self) -> Dict[str, Dict]:
        """RPC handler to query available contracts."""
        self.logger.info("RPC: 收到 query_contracts 请求")
        return {vt_symbol: contract.__dict__ for vt_symbol, contract in self.contracts.items()}

    def query_account(self) -> Dict | None:
        """RPC handler to query the latest account data."""
        self.logger.info("RPC: 收到 query_account 请求")
        if self.last_account_data:
            return self.last_account_data.__dict__
        else:
            return None

    def ping(self) -> str:
        """Handles ping request from client."""
        return "pong"

    # --- Event Processing (Restored with Filtering) ---
    def process_vnpy_event(self, event: Event):
        """Processes events from the EventEngine and puts *filtered* events onto the publish queue."""
        event_type = event.type
        data_obj = event.data

        # Increment counter for ALL received events
        with self._counter_lock:
            self._event_counter += 1

        # --- FILTERING LOGIC ---
        # Define which event types should be published via the queue
        # Add or remove types based on what the client needs to subscribe to
        allowed_event_types = {
            EVENT_ORDER,
            EVENT_TRADE,
            EVENT_ACCOUNT,
            EVENT_POSITION, # Publish position updates if clients need them
            # EVENT_CONTRACT, # Publish contract updates if clients need them
        }

        if event_type not in allowed_event_types:
            # Log discarded event types occasionally for debugging
            if self._event_counter % 5000 == 1: # Log every 5000th event processed
                 self.logger.debug(f"事件处理器: 过滤掉的事件类型: {event_type} (总事件计数: {self._event_counter})")
            # --- Handle side effects for non-published types ---
            if event_type == EVENT_CONTRACT:
                # Still store contract data locally for query_contracts RPC
                contract: ContractData = data_obj
                self.contracts[contract.vt_symbol] = contract
            elif event_type == EVENT_LOG:
                # Log events are handled by the logger, no need to process here
                pass
            # --- End side effects ---
            return # Ignore unwanted event types for publishing

        # --- Process and Serialize Allowed Event Types ---
        topic_bytes = None
        data_bytes = None
        try:
            if event_type == EVENT_ORDER:
                order: OrderData = data_obj
                # +++ Add Order Status Logging (Conditional on active_order_ids) +++
                if order.vt_orderid in self.active_order_ids:
                    self.logger.info(
                        f"订单状态更新: [{order.datetime.strftime('%H:%M:%S')}] VTOrderID={order.vt_orderid}, "
                        f"状态={order.status.value}, "
                        f"已成交={order.traded}/{order.volume}, "
                        f"价格={order.price}, "
                        f"代码={order.symbol}, "
                        f"方向={order.direction.value}, "
                        f"开平={order.offset.value}"
                    )
                # --- End Order Status Logging ---
                topic_bytes = f"order.{order.vt_orderid}".encode('utf-8')
                data_bytes = pickle.dumps(order)
            elif event_type == EVENT_TRADE:
                trade: TradeData = data_obj
                # +++ Add Trade Logging (Conditional on active_order_ids) +++
                if trade.vt_orderid in self.active_order_ids:
                    self.logger.info(
                        f"新成交回报: [{trade.datetime.strftime('%H:%M:%S')}] VTOrderID={trade.vt_orderid}, "
                        f"成交ID={trade.vt_tradeid}, "
                        f"代码={trade.symbol}, "
                        f"方向={trade.direction.value}, "
                        f"开平={trade.offset.value}, "
                        f"价格={trade.price}, "
                        f"数量={trade.volume}"
                    )
                # --- End Trade Logging ---
                topic_bytes = f"trade.{trade.vt_symbol}".encode('utf-8')
                data_bytes = pickle.dumps(trade)
            elif event_type == EVENT_ACCOUNT:
                account: AccountData = data_obj
                self.last_account_data = account # Update local cache

                # +++ Add Account Update Logging (Change-driven with Tolerance) +++
                accountid = account.accountid
                current_balance = account.balance
                current_available = account.available
                last_balance, last_available = self._last_logged_account_values.get(accountid, (None, None))

                log_needed = False
                # Always log the first time we see an account
                if last_balance is None or last_available is None:
                    log_needed = True
                else:
                    # Log if balance OR available changed beyond the tolerance
                    balance_changed = not math.isclose(current_balance, last_balance, abs_tol=0.01)
                    available_changed = not math.isclose(current_available, last_available, abs_tol=0.01)
                    if balance_changed or available_changed:
                        log_needed = True

                if log_needed:
                    self.logger.info(
                        f"账户资金更新: AccountID={accountid}, "
                        f"Balance={current_balance:.2f}, "
                        f"Available={current_available:.2f}, "
                        f"Frozen={account.frozen:.2f}"
                    )
                    # Update last logged values
                    self._last_logged_account_values[accountid] = (current_balance, current_available)
                # --- End Account Update Logging ---

                topic_bytes = f"account.{accountid}".encode('utf-8')
                data_bytes = pickle.dumps(account)
            elif event_type == EVENT_POSITION:
                 position: PositionData = data_obj
                 # +++ Change Position Update Logging to DEBUG +++
                 self.logger.debug(
                     f"持仓更新: 代码={position.vt_symbol}, "
                     f"方向={position.direction.value}, "
                     f"数量={position.volume}, "
                     f"昨仓={position.yd_volume}, "
                     f"冻结={position.frozen}, "
                     f"均价={position.price:.3f}"
                 )
                 # --- End Position Update Logging ---
                 topic_bytes = f"position.{position.vt_symbol}".encode('utf-8')
                 data_bytes = pickle.dumps(position)
            # elif event_type == EVENT_CONTRACT: # If publishing contracts is needed
            #     contract: ContractData = data_obj
            #     self.contracts[contract.vt_symbol] = contract # Update local cache
            #     topic_bytes = f"contract.{contract.vt_symbol}".encode('utf-8')
            #     data_bytes = pickle.dumps(contract)

            # --- Put serialized data onto the publish queue ---
            if topic_bytes and data_bytes:
                self._publish_queue.put((topic_bytes, data_bytes))
                # Log putting action occasionally for debugging
                if self._event_counter % 1000 == 1: # Log less frequently
                    self.logger.debug(f"事件处理器: 将事件放入发布队列: Topic={topic_bytes.decode('utf-8', 'ignore')}")

        except Exception as e:
             # Log error during processing/pickling *before* queueing
             self.logger.exception(f"处理 VNPY 事件 {event_type} (类型: {type(data_obj).__name__}) 并准备放入队列时出错: {e}")

    # --- Publisher Thread Method (Simplified) ---
    def _run_publish(self):
        """Runs in a separate thread to get messages from the publish queue and send via ZMQ PUB socket."""
        self.logger.info("发布线程启动 (批处理模式)。")
        socket_ready_logged = False
        last_queue_size_log_time = time.time()
        queue_size_log_interval = 30 # Log queue size every 30 seconds (Increased interval)
        log_counter = 0
        hwm_warning_logged_recently = False
        last_hwm_warning_time = 0
        hwm_warning_interval = 10
        loop_count = 0

        while self._publisher_active.is_set():
            loop_count += 1
            # Optional: Reduce debug logging frequency further if needed
            # if loop_count % 5000 == 1:
            #     self.logger.debug(f"发布线程: Loop {loop_count}")

            current_time = time.time()

            # --- Log queue size periodically (Change to DEBUG) ---
            if current_time - last_queue_size_log_time >= queue_size_log_interval:
                 publish_qsize = self._publish_queue.qsize()
                 self.logger.debug(f"发布线程: 当前发布队列大小 (self._publish_queue): {publish_qsize}") # DEBUG
                 with self._counter_lock:
                     current_event_count = self._event_counter
                 self.logger.debug(f"发布线程: 处理的 VNPY 事件总数 (计数器): {current_event_count}") # DEBUG
                 last_queue_size_log_time = current_time
                 # Reset HWM warning flag if queue is small
                 if publish_qsize < 500:
                     hwm_warning_logged_recently = False
            # --- End Queue Size Logging ---

            # --- Batch Get Logic (remains the same) ---
            batch = []
            try:
                # Get first item with timeout
                first_item = self._publish_queue.get(timeout=0.1)
                batch.append(first_item)
                # Get subsequent items without blocking
                for _ in range(PUBLISH_BATCH_SIZE - 1):
                     try:
                         item = self._publish_queue.get_nowait()
                         batch.append(item)
                     except queue.Empty:
                         break
            except queue.Empty:
                # No items received during timeout, continue outer loop
                continue

            # --- Process Batch ---
            if not batch:
                continue

            # Check socket status
            socket_available = hasattr(self, '_socket_pub') and self._socket_pub and not self._socket_pub.closed
            if not socket_available:
                 if loop_count % 100 == 1: # Log unavailability occasionally
                     self.logger.warning("发布线程：MAIN PUB socket 不可用或已关闭。本批次消息将被丢弃。")
                 # Mark tasks done since we can't send
                 for _ in batch:
                     self._publish_queue.task_done()
                 time.sleep(0.2)
                 continue

            if not socket_ready_logged:
                self.logger.info("发布线程：检测到 MAIN PUB socket 可用。")
                self.logger.info("订单执行网关服务(RPC模式)准备就绪。按 Ctrl+C 停止主程序。")
                socket_ready_logged = True

            # Process each item in the batch
            items_processed_in_batch = 0
            for item_index, (topic_bytes, data_bytes) in enumerate(batch):
                topic_str_for_log = topic_bytes.decode('utf-8', 'ignore')
                task_done_called = False
                try:
                    self._socket_pub.send_multipart([topic_bytes, data_bytes], flags=zmq.NOBLOCK)
                    items_processed_in_batch += 1
                    # Optional: Log successful send occasionally
                    # if log_counter % 100 == 1:
                    #    self.logger.debug(f"发布线程: Sent Topic: {topic_str_for_log}")
                except zmq.Again:
                    # HWM reached or no subscriber - message dropped by ZMQ
                    qsize_on_hwm = self._publish_queue.qsize()
                    self.logger.warning(f"发布线程: [SEND HWM/Again] ZMQ发送缓冲区满或无订阅者，消息被丢弃. Topic: {topic_str_for_log}. 当前队列大小: {qsize_on_hwm}")
                    items_processed_in_batch += 1 # Count as processed even if dropped
                    # Throttle HWM warnings
                    if not hwm_warning_logged_recently or (current_time - last_hwm_warning_time > hwm_warning_interval):
                        hwm_warning_logged_recently = True
                        last_hwm_warning_time = current_time
                except zmq.ZMQError as send_err:
                    self.logger.error(f"发布线程: [SEND ZMQERR] ZMQ 发送错误: {send_err}. Topic: {topic_str_for_log}")
                    items_processed_in_batch += 1 # Count as processed to mark task done
                except Exception as send_gen_err:
                    self.logger.exception(f"发布线程: [SEND GENERR] 未知发送错误: {send_gen_err}. Topic: {topic_str_for_log}")
                    items_processed_in_batch += 1 # Count as processed
                finally:
                    # Ensure task_done is called for every item attempted from the queue
                    try:
                        self._publish_queue.task_done()
                        task_done_called = True
                    except Exception as td_err:
                         # This should not happen with standard queue
                         self.logger.error(f"发布线程: CRITICAL! 调用 task_done 时出错: {td_err}")
            log_counter += items_processed_in_batch # Increment log counter based on processed items
            # --- End Process Batch ---

        self.logger.info("发布线程停止。")
    # --- End Publisher Thread Method ---

    # --- Lifecycle Management (Simplified) ---
    def start(self, rep_address=None, pub_address=None):
        """Starts RpcServer, EventEngine, Gateway, and Publisher threads."""
        if self.is_active():
            self.logger.warning("订单执行网关服务(RPC模式)已在运行中。")
            return

        self.logger.info("启动订单执行网关服务(RPC模式)...")

        # Start RpcServer first
        try:
            super().start(
                rep_address=config.ORDER_GATEWAY_REP_ADDRESS,
                pub_address=config.ORDER_GATEWAY_PUB_ADDRESS
            )
            self.logger.info(f"RPC 服务器已启动。 REP: {config.ORDER_GATEWAY_REP_ADDRESS}, PUB: {config.ORDER_GATEWAY_PUB_ADDRESS}")
            # Set socket options...
            if hasattr(self, '_socket_pub') and self._socket_pub:
                try:
                    sndhwm = 1000
                    self._socket_pub.setsockopt(zmq.SNDHWM, sndhwm)
                    self.logger.info(f"为 MAIN PUB socket (_socket_pub) 设置了 ZMQ.SNDHWM 选项: {sndhwm}")
                except Exception as opt_err:
                     self.logger.error(f"为 MAIN PUB socket (_socket_pub) 设置 ZMQ 选项时出错: {opt_err}")
            else:
                 self.logger.warning("无法设置 ZMQ 选项：RpcServer 未能成功创建 _socket_pub。")
        except Exception as e:
            self.logger.exception(f"启动 RPC 服务器时失败: {e}")
            return

        # Start Publisher Thread
        if not self._publisher_thread or not self._publisher_thread.is_alive():
             self._publisher_active.set()
             self._publisher_thread = threading.Thread(target=self._run_publish, daemon=True)
             self._publisher_thread.start()
             self.logger.info("发布线程已启动。")
        else:
             self.logger.warning("发布线程似乎已在运行。")

        # Start Event Engine and Register Handlers
        try:
            self.event_engine.register(EVENT_ORDER, self.process_vnpy_event)
            self.event_engine.register(EVENT_TRADE, self.process_vnpy_event)
            self.event_engine.register(EVENT_LOG, self.process_vnpy_event) # Still need to process for counter/side-effects if any
            self.event_engine.register(EVENT_CONTRACT, self.process_vnpy_event) # Still need to process for side-effects
            self.event_engine.register(EVENT_ACCOUNT, self.process_vnpy_event)
            self.event_engine.register(EVENT_POSITION, self.process_vnpy_event) # Register handler for Position events
            # Register for any other events you might want to count or have side effects for
            self.event_engine.start()
            self.logger.info("事件引擎已启动，并注册了事件处理器。")
        except Exception as e:
             self.logger.exception(f"启动或注册事件引擎时出错: {e}")
             self.stop()
             return

        # Connect CTP Gateway
        if not self.ctp_setting:
            self.logger.error("CTP settings not loaded or environment invalid, cannot connect CTP gateway.")
            # Optionally stop the service here if CTP is essential
            # self.stop()
            return

        try:
            self.logger.info(f"CTP 连接配置 (Env: {self.environment_name}): UserID={self.ctp_setting.get('userid')}, "
                             f"BrokerID={self.ctp_setting.get('broker_id')}, TD={self.ctp_setting.get('td_address')}")
            self.gateway.connect(self.ctp_setting) # Uses the selected setting
            self.logger.info("CTP 连接请求已发送。")
        except Exception as err:
            self.logger.exception(f"连接 CTP 交易网关时发生严重错误 (Env: {self.environment_name}): {err}")
            self.stop()
            return

        self.logger.info(f"订单执行网关服务(RPC模式) for [{self.environment_name}] 启动完成。")

    def stop(self):
        """Stops all components."""
        # Check if already stopped or never started properly
        is_rpc_active = self.is_active()
        was_publisher_running = self._publisher_thread and self._publisher_thread.is_alive()
        # was_heartbeat_running is removed

        # if not is_rpc_active and not was_publisher_running: # Simplified check
        #      return

        self.logger.info("停止订单执行网关服务(RPC模式)...")

        # --- REMOVED Stopping Heartbeat Thread ---

        # Stop Publisher Thread first
        if self._publisher_thread and self._publisher_thread.is_alive():
             self.logger.info("正在停止发布线程...")
             self._publisher_active.clear()
             self._publisher_thread.join(timeout=2.0)
             if self._publisher_thread.is_alive():
                  self.logger.error("无法正常停止发布线程。")
             else:
                  self.logger.info("发布线程已停止。")
             self._publisher_thread = None
        # elif was_publisher_running: # Log if thread was running but RpcServer wasn't active
        #      self.logger.warning("服务未激活，但发布线程仍在运行，已尝试停止。")

        # Stop Event Engine
        try:
            # Check if event engine was actually started and is active
            if hasattr(self.event_engine, '_active') and self.event_engine._active:
                self.event_engine.stop()
                self.logger.info("事件引擎已停止。")
            # else: # Optional: Log if never started/already stopped
            #     self.logger.info("事件引擎未运行或已停止。")
        except Exception as e:
            self.logger.exception(f"停止事件引擎时出错: {e}")

        # Close CTP Gateway
        if self.gateway:
            try:
                self.gateway.close()
                self.logger.info("CTP 交易网关已关闭。")
            except Exception as e:
                self.logger.exception(f"关闭 CTP 网关时出错: {e}")

        # Stop RpcServer last
        if is_rpc_active:
            try:
                 super().stop() # RpcServer.stop() closes sockets
                 self.logger.info("RPC 服务器已停止。")
            except Exception as e:
                 self.logger.exception(f"停止 RPC 服务器时出错: {e}")

        self.logger.info("订单执行网关服务(RPC模式)已停止。")

# --- Main execution block (for testing) ---
if __name__ == "__main__":
    try:
        setup_logging(service_name="OrderExecutionGateway_DirectRunRPC")
    except ImportError as log_err:
        print(f"CRITICAL: Failed to import or setup logger: {log_err}. Exiting.")
        sys.exit(1)

    logger_main = getLogger(__name__)
    logger_main.info("Starting direct test run (RPC Mode)...")

    if not hasattr(config, 'ORDER_GATEWAY_REP_ADDRESS') or \
       not hasattr(config, 'ORDER_GATEWAY_PUB_ADDRESS'):
        logger_main.critical("错误：配置文件 config.zmq_config 缺少 ORDER_GATEWAY_REP_ADDRESS 或 ORDER_GATEWAY_PUB_ADDRESS。")
        sys.exit(1)

    gw_service = OrderExecutionGatewayService("TestEnv")
    gw_service.start()

    try:
        while gw_service.is_active():
            time.sleep(1)
    except KeyboardInterrupt:
        logger_main.info("主程序接收到中断信号，正在停止...")
    except Exception as e:
        logger_main.exception(f"主测试循环发生未处理错误：{e}")
    finally:
        logger_main.info("开始停止服务...")
        gw_service.stop()
        logger_main.info("订单执行网关测试运行结束 (RPC Mode)。")
