import zmq
import time
import threading
import sys
import os
from datetime import datetime
from typing import Dict, Tuple, Any
import configparser
import pickle
import queue
import msgpack

from vnpy.trader.utility import load_json

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.logger import logger

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
        logger.critical(f"Error importing vnpy modules: {e}", exc_info=True)
    except Exception as e:
        print(f"CRITICAL: Error importing vnpy modules: {e}")
        print("Please ensure vnpy and vnpy_ctp are installed and accessible.")
        print(f"CRITICAL: Project root added to path: {project_root}")
        print(f"CRITICAL: Current sys.path: {sys.path}")
    sys.exit(1)

from config import zmq_config as config
from config.zmq_config import PUBLISH_BATCH_SIZE # HEARTBEAT_INTERVAL_S likely unused
from utils.converter import convert_vnpy_obj_to_dict

# --- Add Heartbeat Constant ---
HEARTBEAT_INTERVAL_S = 5.0 # Send heartbeat every 5 seconds
# --- End Add --- 

# +++ 添加函数：加载产品信息 +++
def load_product_info(filepath: str) -> Tuple[Dict, Dict]:
    """Loads commission rules and multipliers from an INI file."""
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
        logger.info(f"Initializing OrderExecutionGatewayService for environment: [{environment_name}]...")
        self.environment_name = environment_name # Store for logging

        self._event_counter = 0
        self._counter_lock = threading.Lock()
        self._last_logged_account_values: Dict[str, Tuple[float, float]] = {}
        self.active_order_ids: set[str] = set()
        # +++ Add timestamp for timed account logging +++
        self._last_account_log_time: float = 0.0 
        # +++ End Add +++
        # +++ Add cache for active orders +++
        self.order_cache: Dict[str, OrderData] = {}
        # +++ End Add +++

        # VNPY setup
        self.event_engine = EventEngine()
        # Add env name to gateway name for potential clarity if running multiple
        self.gateway: CtpGateway = CtpGateway(self.event_engine, f"CTP_{environment_name}")

        # --- Load and Select CTP Settings --- 
        self.ctp_setting: dict | None = None
        try:
            all_ctp_settings = load_json("connect_ctp.json")
            if environment_name in all_ctp_settings:
                self.ctp_setting = all_ctp_settings[environment_name]
                logger.info(f"Loaded CTP settings for environment: [{environment_name}]")
            else:
                logger.error(f"Environment '{environment_name}' not found in connect_ctp.json! Cannot connect CTP.")
        except FileNotFoundError:
            logger.error("connect_ctp.json not found! Cannot connect CTP.")
        except Exception as e:
            logger.exception(f"Error loading or parsing connect_ctp.json: {e}")
        # --- End Load and Select --- 

        self.contracts: Dict[str, ContractData] = {}
        self.last_account_data: AccountData | None = None

        # Load product info for commission calculation
        config_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config'))
        info_filepath = os.path.join(config_dir, 'project_files', 'product_info.ini')
        logger.info(f"尝试从 {info_filepath} 加载产品信息...")
        self.commission_rules, self.contract_multipliers = load_product_info(info_filepath)
        if not self.commission_rules or not self.contract_multipliers:
             logger.warning("警告：未能成功加载手续费规则或合约乘数，手续费计算可能不准确！")

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

        # --- Add Heartbeat Thread Variables ---
        self._heartbeat_active = threading.Event()
        self._heartbeat_thread = None
        self._heartbeat_topic = b"heartbeat.ordergw" # Define topic as bytes
        # --- End Add ---

        logger.info(f"订单执行网关服务(RPC模式) for [{environment_name}] 初始化完成。")

    # --- RPC Handlers ---
    def send_order(self, req_dict: Dict[str, Any]) -> str | None:
        """
        RPC handler for sending an order request.
        Returns vt_orderid on success, None on failure.
        """
        logger.info(f"RPC: 收到 send_order 请求: {req_dict}")
        order_request: OrderRequest | None = dict_to_order_request(req_dict)

        if not order_request:
            logger.error("RPC: send_order - 无法将请求转换为 OrderRequest。")
            return None

        try:
            vt_orderid = self.gateway.send_order(order_request)
            if vt_orderid:
                logger.info(f"RPC: send_order - 订单已发送至 CTP: {order_request.symbol}, VT_OrderID: {vt_orderid}")
                self.active_order_ids.add(vt_orderid)
                return vt_orderid
            else:
                # Log if the gateway itself failed to send without raising an exception
                logger.error(f"RPC: send_order - CTP 网关未能发送订单 (gateway.send_order 返回 None): {order_request.symbol}")
                return None
        except Exception as e:
            # Log exceptions raised by the gateway's send_order
            logger.exception(f"RPC: send_order - 发送订单时 CTP 网关出错: {e}")
            return None

    def cancel_order(self, req_dict: Dict[str, Any]) -> Dict[str, str]:
        """
        RPC handler for cancelling an order.
        Requires 'vt_orderid' in the request dictionary.
        Returns a status dictionary.
        """
        vt_orderid = req_dict.get('vt_orderid')
        logger.info(f"RPC: 收到 cancel_order 请求 for {vt_orderid}")
        if not vt_orderid:
            logger.error("RPC: cancel_order - 收到无效的撤单指令：缺少 'vt_orderid' 字段。")
            return {"status": "error", "message": "Missing vt_orderid"}

        try:
            # +++ Replace self.gateway.get_order with cache lookup +++
            # order_to_cancel = self.gateway.get_order(vt_orderid) # Original line - Error
            order_to_cancel = self.order_cache.get(vt_orderid)
            # +++ End Replace +++

            if not order_to_cancel:
                # logger.error(f"RPC: cancel_order - 尝试撤销失败：网关未找到订单 {vt_orderid}") # Original error message
                logger.warning(f"RPC: cancel_order - 尝试撤销失败：在内部缓存中未找到活动订单 {vt_orderid} (可能已完成或不存在)") # New warning
                return {"status": "error", "message": f"Order {vt_orderid} not found in active cache"}

            req = CancelRequest(
                orderid=vt_orderid, # Use the vt_orderid directly
                symbol=order_to_cancel.symbol,
                exchange=order_to_cancel.exchange,
            )
            self.gateway.cancel_order(req)
            logger.info(f"RPC: cancel_order - 撤单请求已发送 for {vt_orderid}")
            return {"status": "ok", "message": f"Cancel request sent for {vt_orderid}"}

        # except AttributeError: # Remove this specific exception handler for get_order
        #      logger.error("RPC: cancel_order - 当前网关对象不支持 get_order 方法。")
        #      return {"status": "error", "message": "Gateway does not support get_order, cannot cancel."}
        except Exception as e:
            logger.exception(f"RPC: cancel_order - 处理撤单指令时出错 (vt_orderid: {vt_orderid}): {e}")
            return {"status": "error", "message": f"Error processing cancel command: {e}"}

    def query_contracts(self) -> Dict[str, Dict]:
        """RPC handler to query available contracts."""
        logger.info("RPC: 收到 query_contracts 请求")
        return {vt_symbol: contract.__dict__ for vt_symbol, contract in self.contracts.items()}

    def query_account(self) -> Dict | None:
        """RPC handler to query the latest account data."""
        logger.info("RPC: 收到 query_account 请求")
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
                 logger.debug(f"事件处理器: 过滤掉的事件类型: {event_type} (总事件计数: {self._event_counter})")
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
                # +++ Update order cache +++
                if order.is_active():
                    self.order_cache[order.vt_orderid] = order
                elif order.vt_orderid in self.order_cache:
                    del self.order_cache[order.vt_orderid]
                # +++ End update cache +++

                # +++ Remove order ID from active set if order is no longer active +++
                if not order.is_active():
                    if order.vt_orderid in self.active_order_ids:
                        self.active_order_ids.discard(order.vt_orderid)
                        logger.debug(f"订单 {order.vt_orderid} 状态变为非活动 ({order.status.value})，已从 active_order_ids 移除。")
                # +++ End remove order ID +++

                # +++ Add Order Status Logging (Conditional on active_order_ids) +++
                if order.vt_orderid in self.active_order_ids or not order.is_active():
                    # --- FIX: Add check for None datetime before formatting ---
                    time_str = order.datetime.strftime('%H:%M:%S') if order.datetime else "NoTime"
                    logger.info(
                        f"订单状态更新: [{time_str}] VTOrderID={order.vt_orderid}, " # Use time_str
                        f"状态={order.status.value}, "
                        f"已成交={order.traded}/{order.volume}, "
                        f"价格={order.price}, "
                        f"代码={order.symbol}, "
                        f"方向={order.direction.value}, "
                        f"开平={order.offset.value}"
                    )
                    # --- End FIX ---
                # --- End Order Status Logging ---
                topic_bytes = f"order.{order.vt_orderid}".encode('utf-8')
                # Convert object to dict THEN msgpack
                dict_data = convert_vnpy_obj_to_dict(order)
                data_bytes = msgpack.packb(dict_data, use_bin_type=True)
            elif event_type == EVENT_TRADE:
                trade: TradeData = data_obj
                # +++ 无条件记录收到成交事件 +++
                logger.info(f"收到 EVENT_TRADE: VTOrderID={getattr(trade, 'vt_orderid', 'N/A')}, Symbol={getattr(trade, 'vt_symbol', 'N/A')}")
                # +++ 结束无条件记录 +++

                topic_bytes = f"trade.{trade.vt_symbol}".encode('utf-8')
                # Convert object to dict THEN msgpack
                dict_data = convert_vnpy_obj_to_dict(trade)
                data_bytes = msgpack.packb(dict_data, use_bin_type=True)
            elif event_type == EVENT_ACCOUNT:
                account: AccountData = data_obj
                self.last_account_data = account # Update local cache
                # +++ Get accountid FIRST +++
                accountid = account.accountid 
                # +++ End Get +++

                # +++ Add Time Check for Account Update Logging +++
                current_time = time.time()
                if current_time - self._last_account_log_time >= 300:
                    logger.info(
                        f"账户资金更新 (每5分钟): AccountID={accountid}, "
                        f"Balance={account.balance:.2f}, "
                        f"Available={account.available:.2f}, "
                        f"Frozen={account.frozen:.2f}"
                    )
                    self._last_account_log_time = current_time
                # --- End Time Check and Logging Logic ---

                topic_bytes = f"account.{accountid}".encode('utf-8')
                # Convert object to dict THEN msgpack
                dict_data = convert_vnpy_obj_to_dict(account)
                data_bytes = msgpack.packb(dict_data, use_bin_type=True)
            elif event_type == EVENT_POSITION:
                 position: PositionData = data_obj
                 # +++ Change Position Update Logging to DEBUG +++
                 logger.debug(
                     f"持仓更新: 代码={position.vt_symbol}, "
                     f"方向={position.direction.value}, "
                     f"数量={position.volume}, "
                     f"昨仓={position.yd_volume}, "
                     f"冻结={position.frozen}, "
                     f"均价={position.price:.3f}"
                 )
                 # --- End Position Update Logging ---
                 topic_bytes = f"position.{position.vt_symbol}".encode('utf-8')
                 # Convert object to dict THEN msgpack
                 dict_data = convert_vnpy_obj_to_dict(position)
                 data_bytes = msgpack.packb(dict_data, use_bin_type=True)
            # elif event_type == EVENT_CONTRACT: # If publishing contracts is needed
            #     contract: ContractData = data_obj
            #     self.contracts[contract.vt_symbol] = contract # Update local cache
            #     topic_bytes = f"contract.{contract.vt_symbol}".encode('utf-8')
            #     # Convert object to dict THEN msgpack
            #     dict_data = convert_vnpy_obj_to_dict(contract)
            #     data_bytes = msgpack.packb(dict_data, use_bin_type=True)

            # --- Put serialized data onto the publish queue ---
            if topic_bytes and data_bytes:
                self._publish_queue.put((topic_bytes, data_bytes))
                # Log putting action occasionally for debugging
                if self._event_counter % 1000 == 1: # Log less frequently
                    logger.debug(f"事件处理器: 将事件放入发布队列: Topic={topic_bytes.decode('utf-8', 'ignore')}")

        except Exception as e:
             # Log error during processing/pickling *before* queueing
             logger.exception(f"处理 VNPY 事件 {event_type} (类型: {type(data_obj).__name__}) 并准备放入队列时出错: {e}")

    # --- Publisher Thread Method (Simplified) ---
    def _run_publish(self):
        """Runs in a separate thread to get messages from the publish queue and send via ZMQ PUB socket."""
        logger.info("发布线程启动 (批处理模式)。")
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
            current_time = time.time()

            # --- Log queue size periodically (Change to DEBUG) ---
            if current_time - last_queue_size_log_time >= queue_size_log_interval:
                 publish_qsize = self._publish_queue.qsize()
                 logger.debug(f"发布线程: 当前发布队列大小 (self._publish_queue): {publish_qsize}") # DEBUG
                 with self._counter_lock:
                     current_event_count = self._event_counter
                 logger.debug(f"发布线程: 处理的 VNPY 事件总数 (计数器): {current_event_count}") # DEBUG
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
                     logger.warning("发布线程：MAIN PUB socket 不可用或已关闭。本批次消息将被丢弃。")
                 # Mark tasks done since we can't send
                 for _ in batch:
                     self._publish_queue.task_done()
                 time.sleep(0.2)
                 continue

            if not socket_ready_logged:
                logger.info("发布线程：检测到 MAIN PUB socket 可用。")
                logger.info("订单执行网关服务(RPC模式)准备就绪。按 Ctrl+C 停止主程序。")
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
                    #    logger.debug(f"发布线程: Sent Topic: {topic_str_for_log}")
                except zmq.Again:
                    # HWM reached or no subscriber - message dropped by ZMQ
                    qsize_on_hwm = self._publish_queue.qsize()
                    logger.warning(f"发布线程: [SEND HWM/Again] ZMQ发送缓冲区满或无订阅者，消息被丢弃. Topic: {topic_str_for_log}. 当前队列大小: {qsize_on_hwm}")
                    items_processed_in_batch += 1 # Count as processed even if dropped
                    # Throttle HWM warnings
                    if not hwm_warning_logged_recently or (current_time - last_hwm_warning_time > hwm_warning_interval):
                        hwm_warning_logged_recently = True
                        last_hwm_warning_time = current_time
                except zmq.ZMQError as send_err:
                    logger.error(f"发布线程: [SEND ZMQERR] ZMQ 发送错误: {send_err}. Topic: {topic_str_for_log}")
                    items_processed_in_batch += 1 # Count as processed to mark task done
                except Exception as send_gen_err:
                    logger.exception(f"发布线程: [SEND GENERR] 未知发送错误: {send_gen_err}. Topic: {topic_str_for_log}")
                    items_processed_in_batch += 1 # Count as processed
                finally:
                    # Ensure task_done is called for every item attempted from the queue
                    try:
                        self._publish_queue.task_done()
                        task_done_called = True
                    except Exception as td_err:
                         # This should not happen with standard queue
                         logger.error(f"发布线程: CRITICAL! 调用 task_done 时出错: {td_err}")
            log_counter += items_processed_in_batch # Increment log counter based on processed items
            # --- End Process Batch ---

        logger.info("发布线程停止。")
    # --- End Publisher Thread Method ---

    # --- Add Heartbeat Thread Method ---
    def _run_heartbeat(self):
        """Runs in a separate thread to periodically send heartbeat messages."""
        logger.info(f"心跳线程启动 (间隔: {HEARTBEAT_INTERVAL_S}s, 主题: {self._heartbeat_topic.decode()})。")
        loop_count = 0
        while self._heartbeat_active.is_set():
            loop_count += 1
            # Check if publisher socket is ready
            socket_available = hasattr(self, '_socket_pub') and self._socket_pub and not self._socket_pub.closed
            if socket_available:
                try:
                    current_timestamp = time.time()
                    heartbeat_dict = {"timestamp": current_timestamp}
                    data_bytes = msgpack.packb(heartbeat_dict, use_bin_type=True)
                    self._socket_pub.send_multipart([self._heartbeat_topic, data_bytes], flags=zmq.NOBLOCK)
                    # Log heartbeat sending occasionally
                    if loop_count % 12 == 1: # e.g., log every minute if interval is 5s
                        logger.debug(f"发送心跳: Timestamp={datetime.fromtimestamp(current_timestamp).isoformat()}")
                except zmq.Again:
                     # Should not happen often with NOBLOCK on PUB if HWM not reached, but log if it does
                     logger.warning("发送心跳时遇到 ZMQ EAGAIN (缓冲区可能已满？)")
                except Exception as e:
                     logger.exception(f"发送心跳时出错: {e}")
            else:
                # Log if socket is not ready (less frequently)
                if loop_count % 6 == 1:
                    logger.warning("心跳线程：MAIN PUB socket 不可用或已关闭，无法发送心跳。")

            # Wait for the next interval, checking the active flag periodically
            # This allows faster shutdown than a single sleep(INTERVAL)
            wait_interval = 0.5 # Check every 0.5 seconds
            remaining_wait = HEARTBEAT_INTERVAL_S
            while remaining_wait > 0 and self._heartbeat_active.is_set():
                time.sleep(min(wait_interval, remaining_wait))
                remaining_wait -= wait_interval

        logger.info("心跳线程停止。")
    # --- End Add Heartbeat Thread Method ---

    # --- Lifecycle Management (Simplified) ---
    def start(self, rep_address=None, pub_address=None):
        """Starts RpcServer, EventEngine, Gateway, Publisher, and Heartbeat threads."""
        if self.is_active():
            logger.warning(f"订单执行网关服务(RPC模式) for [{self.environment_name}] 已在运行中。")
            return

        logger.info(f"启动订单执行网关服务(RPC模式) for [{self.environment_name}]...")

        # Start RpcServer first
        try:
            super().start(
                rep_address=config.ORDER_GATEWAY_REP_ADDRESS,
                pub_address=config.ORDER_GATEWAY_PUB_ADDRESS
            )
            logger.info(f"RPC 服务器已启动。 REP: {config.ORDER_GATEWAY_REP_ADDRESS}, PUB: {config.ORDER_GATEWAY_PUB_ADDRESS}")
            # Set socket options...
            if hasattr(self, '_socket_pub') and self._socket_pub:
                try:
                    sndhwm = 1000
                    self._socket_pub.setsockopt(zmq.SNDHWM, sndhwm)
                    logger.info(f"为 MAIN PUB socket (_socket_pub) 设置了 ZMQ.SNDHWM 选项: {sndhwm}")
                except Exception as opt_err:
                     logger.error(f"为 MAIN PUB socket (_socket_pub) 设置 ZMQ 选项时出错: {opt_err}")
            else:
                 logger.warning("无法设置 ZMQ 选项：RpcServer 未能成功创建 _socket_pub。")
        except Exception as e:
            logger.exception(f"启动 RPC 服务器时失败: {e}")
            return

        # Start Publisher Thread
        if not self._publisher_thread or not self._publisher_thread.is_alive():
             self._publisher_active.set()
             self._publisher_thread = threading.Thread(target=self._run_publish, daemon=True)
             self._publisher_thread.start()
             logger.info("发布线程已启动。")
        else:
             logger.warning("发布线程似乎已在运行。")

        # --- Start Heartbeat Thread --- 
        if not self._heartbeat_thread or not self._heartbeat_thread.is_alive():
            self._heartbeat_active.set()
            self._heartbeat_thread = threading.Thread(target=self._run_heartbeat, daemon=True)
            self._heartbeat_thread.start()
            logger.info("心跳线程已启动。")
        else:
             logger.warning("心跳线程似乎已在运行。")
        # --- End Start Heartbeat --- 

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
            logger.info("事件引擎已启动，并注册了事件处理器。")
        except Exception as e:
             logger.exception(f"启动或注册事件引擎时出错: {e}")
             self.stop()
             return

        # Connect CTP Gateway
        if not self.ctp_setting:
            logger.error("CTP settings not loaded or environment invalid, cannot connect CTP gateway.")
            # Optionally stop the service here if CTP is essential
            # self.stop()
            return

        try:
            logger.info(f"CTP 连接配置 (Env: {self.environment_name}): UserID={self.ctp_setting.get('userid')}, "
                             f"BrokerID={self.ctp_setting.get('broker_id')}, TD={self.ctp_setting.get('td_address')}")
            self.gateway.connect(self.ctp_setting) # Uses the selected setting
            logger.info("CTP 连接请求已发送。")
        except Exception as err:
            logger.exception(f"连接 CTP 交易网关时发生严重错误 (Env: {self.environment_name}): {err}")
            self.stop()
            return

        logger.info(f"订单执行网关服务(RPC模式) for [{self.environment_name}] 启动完成。")

    def stop(self):
        """Stops all components."""
        # Check if already stopped or never started properly
        is_rpc_active = self.is_active()

        logger.info("停止订单执行网关服务(RPC模式)...")

        # --- Stop Heartbeat Thread --- 
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            logger.info("正在停止心跳线程...")
            self._heartbeat_active.clear()
            self._heartbeat_thread.join(timeout=2.0) # Wait a bit for it to finish
            if self._heartbeat_thread.is_alive():
                logger.error("无法正常停止心跳线程。")
            else:
                logger.info("心跳线程已停止。")
            self._heartbeat_thread = None
        # --- End Stop Heartbeat --- 

        # Stop Publisher Thread first
        if self._publisher_thread and self._publisher_thread.is_alive():
             logger.info("正在停止发布线程...")
             self._publisher_active.clear()
             self._publisher_thread.join(timeout=2.0)
             if self._publisher_thread.is_alive():
                  logger.error("无法正常停止发布线程。")
             else:
                  logger.info("发布线程已停止。")
             self._publisher_thread = None
        # elif was_publisher_running: # Log if thread was running but RpcServer wasn't active
        #      logger.warning("服务未激活，但发布线程仍在运行，已尝试停止。")

        # Stop Event Engine
        try:
            # Check if event engine was actually started and is active
            if hasattr(self.event_engine, '_active') and self.event_engine._active:
                self.event_engine.stop()
                logger.info("事件引擎已停止。")
            # else: # Optional: Log if never started/already stopped
            #     logger.info("事件引擎未运行或已停止。")
        except Exception as e:
            logger.exception(f"停止事件引擎时出错: {e}")

        # Close CTP Gateway
        if self.gateway:
            try:
                self.gateway.close()
                logger.info("CTP 交易网关已关闭。")
            except Exception as e:
                logger.exception(f"关闭 CTP 网关时出错: {e}")

        # Stop RpcServer last
        if is_rpc_active:
            try:
                 super().stop() # RpcServer.stop() closes sockets
                 logger.info("RPC 服务器已停止。")
            except Exception as e:
                 logger.exception(f"停止 RPC 服务器时出错: {e}")

        logger.info(f"订单执行网关服务(RPC模式) for [{self.environment_name}] 已停止。")
