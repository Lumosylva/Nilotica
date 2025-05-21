import os
import queue
import sys
import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import msgpack
import zmq

from config.constants.path import GlobalPath
from vnpy.trader.utility import load_json

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.config_manager import ConfigManager
from utils.converter import convert_order_data_to_dict, dict_to_order_request
from utils.i18n import get_translator
from utils.logger import logger

# VNPY imports
from vnpy.event import Event, EventEngine
from vnpy.rpc import RpcServer
from vnpy.trader.event import EVENT_ACCOUNT, EVENT_CONTRACT, EVENT_LOG, EVENT_ORDER, EVENT_POSITION, EVENT_TRADE
from vnpy.trader.object import (
    AccountData,
    CancelRequest,
    ContractData,
    OrderData,
    OrderRequest,
    PositionData,
    TradeData,
)
from vnpy_ctp import CtpGateway

HEARTBEAT_INTERVAL_S = 5.0 # Send heartbeat every 5 seconds


# --- Order Execution Gateway Service (RPC Mode) ---
class OrderExecutionGatewayService(RpcServer):
    """
    使用 CtpGateway 和 RpcServer 的订单执行网关服务。
    通过 RPC 调用处理订单发送/取消，并发布订单/交易更新。
    使用专用发布线程确保线程安全。

    Order execution gateway service using CtpGateway and RpcServer.
    Handles order sending/canceling via RPC calls and publishes order/trade updates.
    Uses a dedicated publisher thread for thread-safety.
    """
    def __init__(self, config_manager: ConfigManager, environment_name: str):
        super().__init__()
        # +++ Use passed ConfigManager instance +++
        self.config_service = config_manager

        self._ = get_translator()

        logger.info(self._("正在初始化 OrderExecutionGatewayService for environment: [{}] using provided ConfigManager.").format(environment_name))
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
        # +++ Add for CTP reconnection +++
        self._stored_ctp_setting: Optional[Dict[str, Any]] = None
        self._last_reconnect_attempt_time: float = 0.0
        self._reconnect_interval_seconds: int = 30 # Minimum interval between reconnect attempts
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
                logger.info(self._("已加载环境 [{}] 的 CTP 设置").format(environment_name))
            else:
                logger.error(self._("在 connect_ctp.json 中未找到环境 '{}'！无法连接 CTP。").format(environment_name))
                self.ctp_setting = None # Ensure it's None if not found
        except FileNotFoundError:
            logger.error(self._("未找到 connect_ctp.json！无法连接 CTP。"))
            self.ctp_setting = None # Ensure it's None
        except Exception as err:
            logger.exception(self._("加载或解析 connect_ctp.json 时出错: {}").format(err))
            self.ctp_setting = None # Ensure it's None
        # --- End Load and Select --- 

        self.contracts: Dict[str, ContractData] = {}
        self.last_account_data: AccountData | None = None

        # Load product info for commission calculation
        logger.info(self._("尝试从 {} 加载产品信息...").format(GlobalPath.product_info_filepath))
        self.commission_rules, self.contract_multipliers = self.config_service.load_product_info(GlobalPath.product_info_filepath)
        if not self.commission_rules or not self.contract_multipliers:
             logger.warning(self._("警告：未能成功加载手续费规则或合约乘数，手续费计算可能不准确！"))

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

        logger.info(self._("订单执行网关服务(RPC模式) for [{}] 初始化完成。").format(environment_name))

    # --- RPC Handlers ---
    def send_order(self, req_dict: Dict[str, Any]) -> str | None:
        """
        用于发送订单请求的 RPC 处理程序。
        成功时返回 vt_orderid，失败时返回 None。

        RPC handler for sending an order request.
        Returns vt_orderid on success, None on failure.
        """
        logger.info(self._("RPC: 收到 send_order 请求: {}").format(req_dict))
        order_request: OrderRequest | None = dict_to_order_request(req_dict)

        if not order_request:
            logger.error(self._("RPC: send_order - 无法将请求转换为 OrderRequest。"))
            return None

        try:
            vt_orderid = self.gateway.send_order(order_request)
            if vt_orderid:
                logger.info(self._("RPC: send_order - 订单已发送至 CTP: {}, VT_OrderID: {}").format(order_request.symbol, vt_orderid))
                self.active_order_ids.add(vt_orderid)
                return vt_orderid
            else:
                # Log if the gateway itself failed to send without raising an exception
                logger.error(self._("RPC: send_order - CTP 网关未能发送订单 (gateway.send_order 返回 None): {}").format(order_request.symbol))
                # Potentially an issue, maybe try reconnecting?
                self._attempt_ctp_reconnect(reason=self._("对于 {}，gateway.send_order 返回 None").format(order_request.symbol))
                return None
        except Exception as e:
            # Log exceptions raised by the gateway's send_order
            logger.exception(self._("RPC: send_order - 发送订单时 CTP 网关出错: {}").format(e))
            self._attempt_ctp_reconnect(reason=self._("send_order 异常：{}").format(e))
            return None

    def cancel_order(self, req_dict: Dict[str, Any]) -> Dict[str, str]:
        """
        用于取消订单的 RPC 处理程序。
        需要请求字典中包含"vt_orderid"。
        返回状态字典。

        RPC handler for cancelling an order.
        Requires 'vt_orderid' in the request dictionary.
        Returns a status dictionary.
        """
        vt_orderid = req_dict.get('vt_orderid')
        logger.info(self._("RPC: 收到 cancel_order 请求 for {}").format(vt_orderid))
        if not vt_orderid:
            logger.error(self._("RPC: cancel_order - 收到无效的撤单指令：缺少 'vt_orderid' 字段。"))
            return {"status": "error", "message": "Missing vt_orderid"}

        try:
            # +++ Replace self.gateway.get_order with cache lookup +++
            # order_to_cancel = self.gateway.get_order(vt_orderid) # Original line - Error
            order_to_cancel = self.order_cache.get(vt_orderid)
            # +++ End Replace +++

            if not order_to_cancel:
                # logger.error(f"RPC: cancel_order - 尝试撤销失败：网关未找到订单 {vt_orderid}") # Original error message
                logger.warning(self._("RPC: cancel_order - 尝试撤销失败：在内部缓存中未找到活动订单 {} (可能已完成或不存在)").format(vt_orderid)) # New warning
                return {f"status": "error", "message": "Order {vt_orderid} not found in active cache"}

            req = CancelRequest(
                orderid=vt_orderid, # Use the vt_orderid directly
                symbol=order_to_cancel.symbol,
                exchange=order_to_cancel.exchange,
            )
            self.gateway.cancel_order(req)
            logger.info(self._("RPC: cancel_order - 撤单请求已发送 for {}").format(vt_orderid))
            return {f"status": "ok", "message": "Cancel request sent for {vt_orderid}"}

        except Exception as e:
            logger.exception(self._("RPC: cancel_order - 处理撤单指令时出错 (vt_orderid: {}): {}").format(vt_orderid, e))
            return {f"status": "error", "message": "Error processing cancel command: {e}"}

    def query_contracts(self) -> Dict[str, Dict]:
        """
        RPC 处理程序用于查询可用的合约。

        RPC handler to query available contracts.
        """
        logger.info(self._("RPC: 收到 query_contracts 请求"))
        return {vt_symbol: contract.__dict__ for vt_symbol, contract in self.contracts.items()}

    def query_account(self) -> Dict | None:
        """
        RPC 处理程序用于查询最新的帐户数据。

        RPC handler to query the latest account data.
        """
        logger.info(self._("RPC: 收到 query_account 请求"))
        if self.last_account_data:
            return self.last_account_data.__dict__
        else:
            return None

    @staticmethod
    def ping() -> str:
        """
        处理来自客户端的 ping 请求。

        Handles ping request from client.
        """
        return "pong"

    # +++ Add CTP Reconnect Method +++
    def _attempt_ctp_reconnect(self, reason: str = "generic error"):
        """
        如果最近没有尝试过，则尝试重新连接到 CTP。

        Attempts to reconnect to CTP if not recently attempted.
        """
        current_time = time.time()
        if not self._stored_ctp_setting:
            logger.error(self._("无法尝试 CTP 重连：未存储 CTP 配置。"))
            return

        if current_time - self._last_reconnect_attempt_time < self._reconnect_interval_seconds:
            logger.info(self._("CTP 重连请求过于频繁，已跳过。上次尝试在 {} 秒内。原因: {}").format(self._reconnect_interval_seconds, reason))
            return

        logger.warning(self._("检测到 CTP 连接问题 (原因: {})，尝试重新连接 CTP...").format(reason))
        self._last_reconnect_attempt_time = current_time
        try:
            # Ensure gateway object exists
            if not hasattr(self, 'gateway') or not self.gateway:
                 logger.error(self._("无法重连 CTP：网关对象不存在。"))
                 return

            # It's good practice to close before reconnecting if the API supports it well,
            # but simple connect might also work or be required by vnpy's gateway.
            # self.gateway.close() # Optional: and if close is idempotent
            # time.sleep(1) # Brief pause before reconnect
            
            logger.info(self._("调用 self.gateway.connect 使用已存储的配置进行 CTP 重连..."))
            self.gateway.connect(self._stored_ctp_setting)
            logger.info(self._("CTP 重新连接请求已发送。请监控后续日志确认连接状态。"))
            # Note: Confirmation of successful reconnect would typically come via
            # CTP's OnFrontConnected and OnRspUserLogin callbacks, which vnpy_ctp
            # CtpGateway should handle and potentially log.
        except Exception as e:
            logger.exception(self._("尝试 CTP 重新连接时发生错误: {}").format(e))
    # +++ End Add CTP Reconnect Method +++

    # --- Event Processing (Restored with Filtering) ---
    def process_vnpy_event(self, event: Event):
        """
        处理来自 EventEngine 的事件并将*过滤后的*事件放入发布队列。

        Processes events from the EventEngine and puts *filtered* events onto the publishing queue.
        """
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
                 logger.debug(self._("事件处理器: 过滤掉的事件类型: {} (总事件计数: {})").format(event_type, self._event_counter))
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
                        logger.debug(self._("订单 {} 状态变为非活动 ({}), 已从 active_order_ids 移除。").format(order.vt_orderid, order.status.value))
                # +++ End remove order ID +++

                # +++ Add Order Status Logging (Conditional on active_order_ids) +++
                if order.vt_orderid in self.active_order_ids or not order.is_active():
                    # --- FIX: Add check for None datetime before formatting ---
                    time_str = order.datetime.strftime('%H:%M:%S') if order.datetime else "NoTime"
                    logger.info(
                        self._("订单状态更新: [{}] VTOrderID={}, 状态={}, 已成交={}/{}, 价格={}, 代码={}, 方向={}, 开平={}").format(
                            time_str,
                            order.vt_orderid,
                            order.status.value,
                            order.traded,
                            order.volume,
                            order.price,
                            order.symbol,
                            order.direction.value,
                            order.offset.value
                        )
                    )
                    # --- End FIX ---
                # --- End Order Status Logging ---
                topic_bytes = f"order.{order.vt_orderid}".encode('utf-8')
                # Convert object to dict THEN msgpack
                dict_data = convert_order_data_to_dict(order)
                data_bytes = msgpack.packb(dict_data, use_bin_type=True)
            elif event_type == EVENT_TRADE:
                trade: TradeData = data_obj
                # +++ 无条件记录收到成交事件 +++
                logger.info(self._("收到 EVENT_TRADE: VTOrderID={}, Symbol={}").format(
                    getattr(trade, 'vt_orderid', 'N/A'),
                    getattr(trade, 'vt_symbol', 'N/A')
                ))
                # +++ 结束无条件记录 +++

                topic_bytes = f"trade.{trade.vt_symbol}".encode('utf-8')
                # Convert object to dict THEN msgpack
                dict_data = convert_order_data_to_dict(trade)
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
                        self._("账户资金更新 (每5分钟): AccountID={}, Balance={:.2f}, Available={:.2f}, Frozen={:.2f}").format(
                            accountid,
                            account.balance,
                            account.available,
                            account.frozen
                        )
                    )
                    self._last_account_log_time = current_time
                # --- End Time Check and Logging Logic ---

                topic_bytes = f"account.{accountid}".encode('utf-8')
                # Convert object to dict THEN msgpack
                dict_data = convert_order_data_to_dict(account)
                data_bytes = msgpack.packb(dict_data, use_bin_type=True)
            elif event_type == EVENT_POSITION:
                 position: PositionData = data_obj
                 # +++ Change Position Update Logging to DEBUG +++
                 logger.debug(
                     self._("持仓更新: 代码={}, 方向={}, 数量={}, 昨仓={}, 冻结={}, 均价={:.3f}").format(
                         position.vt_symbol,
                         position.direction.value,
                         position.volume,
                         position.yd_volume,
                         position.frozen,
                         position.price
                     )
                 )
                 # --- End Position Update Logging ---
                 topic_bytes = f"position.{position.vt_symbol}".encode('utf-8')
                 # Convert object to dict THEN msgpack
                 dict_data = convert_order_data_to_dict(position)
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
                    logger.debug(self._("事件处理器: 将事件放入发布队列: Topic={}").format(topic_bytes.decode('utf-8', 'ignore')))

        except Exception as e:
             # Log error during processing/pickling *before* queueing
             logger.exception(self._("处理 VNPY 事件 {} (类型: {}) 并准备放入队列时出错: {}").format(
                 event_type,
                 type(data_obj).__name__,
                 e
             ))

    # --- Publisher Thread Method (Simplified) ---
    def _run_publish(self):
        """
        在单独的线程中运行以从发布队列中获取消息并通过 ZMQ PUB 套接字发送。

        Runs in a separate thread to get messages from the publishing queue and send via ZMQ PUB socket.
        """
        logger.info(self._("发布线程启动 (批处理模式)。"))
        socket_ready_logged = False
        last_queue_size_log_time = time.time()
        queue_size_log_interval = 30 # Log queue size every 30 seconds (Increased interval)
        log_counter = 0
        hwm_warning_logged_recently = False
        last_hwm_warning_time = 0
        hwm_warning_interval = 10
        loop_count = 0

        # +++ Get PUBLISH_BATCH_SIZE from ConfigManager +++
        publish_batch_size = self.config_service.get_global_config("service_settings.publish_batch_size", 1000)

        while self._publisher_active.is_set():
            loop_count += 1
            current_time = time.time()

            # --- Log queue size periodically (Change to DEBUG) ---
            if current_time - last_queue_size_log_time >= queue_size_log_interval:
                 publish_qsize = self._publish_queue.qsize()
                 logger.debug(self._("发布线程: 当前发布队列大小 (self._publish_queue): {}").format(publish_qsize)) # DEBUG
                 with self._counter_lock:
                     current_event_count = self._event_counter
                 logger.debug(self._("发布线程: 处理的 VNPY 事件总数 (计数器): {}").format(current_event_count)) # DEBUG
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
                for _ in range(publish_batch_size - 1): # Use the fetched publish_batch_size
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
                     logger.warning(self._("发布线程：MAIN PUB socket 不可用或已关闭。本批次消息将被丢弃。"))
                 # Mark tasks done since we can't send
                 for _ in batch:
                     self._publish_queue.task_done()
                 time.sleep(0.2)
                 continue

            if not socket_ready_logged:
                logger.info(self._("发布线程：检测到 MAIN PUB socket 可用。"))
                logger.info(self._("订单执行网关服务(RPC模式)准备就绪。按 Ctrl+C 停止主程序。"))
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
                    logger.warning(self._("发布线程: [SEND HWM/Again] ZMQ发送缓冲区满或无订阅者，消息被丢弃. Topic: {}. 当前队列大小: {}").format(topic_str_for_log, qsize_on_hwm))
                    items_processed_in_batch += 1 # Count as processed even if dropped
                    # Throttle HWM warnings
                    if not hwm_warning_logged_recently or (current_time - last_hwm_warning_time > hwm_warning_interval):
                        hwm_warning_logged_recently = True
                        last_hwm_warning_time = current_time
                except zmq.ZMQError as send_err:
                    logger.error(self._("发布线程: [SEND ZMQERR] ZMQ 发送错误: {}. Topic: {}").format(send_err, topic_str_for_log))
                    items_processed_in_batch += 1 # Count as processed to mark task done
                except Exception as send_gen_err:
                    logger.exception(self._("发布线程: [SEND GENERR] 未知发送错误: {}. Topic: {}").format(send_gen_err, topic_str_for_log))
                    items_processed_in_batch += 1 # Count as processed
                finally:
                    # Ensure task_done is called for every item attempted from the queue
                    try:
                        self._publish_queue.task_done()
                        task_done_called = True
                    except Exception as td_err:
                         # This should not happen with standard queue
                         logger.error(self._("发布线程: CRITICAL! 调用 task_done 时出错: {}").format(td_err))
            log_counter += items_processed_in_batch # Increment log counter based on processed items
            # --- End Process Batch ---

        logger.info(self._("发布线程停止。"))
    # --- End Publisher Thread Method ---

    # --- Add Heartbeat Thread Method ---
    def _run_heartbeat(self):
        """
        在单独的线程中运行以定期发送心跳消息。

        Runs in a separate thread to periodically send heartbeat messages.
        """
        logger.info(self._("心跳线程启动 (间隔: {}s, 主题: {})。").format(HEARTBEAT_INTERVAL_S, self._heartbeat_topic.decode()))
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
                        logger.debug(self._("发送心跳: Timestamp={}").format(datetime.fromtimestamp(current_timestamp).isoformat()))
                except zmq.Again:
                     # Should not happen often with NOBLOCK on PUB if HWM not reached, but log if it does
                     logger.warning(self._("发送心跳时遇到 ZMQ EAGAIN (缓冲区可能已满？)"))
                except Exception as e:
                     logger.exception(self._("发送心跳时出错: {}").format(e))
            else:
                # Log if socket is not ready (less frequently)
                if loop_count % 6 == 1:
                    logger.warning(self._("心跳线程：MAIN PUB socket 不可用或已关闭，无法发送心跳。"))

            # Wait for the next interval, checking the active flag periodically
            # This allows faster shutdown than a single sleep(INTERVAL)
            wait_interval = 0.5 # Check every 0.5 seconds
            remaining_wait = HEARTBEAT_INTERVAL_S
            while remaining_wait > 0 and self._heartbeat_active.is_set():
                time.sleep(min(wait_interval, remaining_wait))
                remaining_wait -= wait_interval

        logger.info(self._("心跳线程停止。"))
    # --- End Add Heartbeat Thread Method ---

    # --- Lifecycle Management (Simplified) ---
    def start(self, rep_address=None, pub_address=None):
        """
        启动 RpcServer、EventEngine、Gateway、Publisher 和 Heartbeat 线程。

        Starts RpcServer, EventEngine, Gateway, Publisher, and Heartbeat threads.
        """
        if self.is_active():
            logger.warning(self._("订单执行网关服务(RPC模式) for [{}] 已在运行中。").format(self.environment_name))
            return

        logger.info(self._("启动订单执行网关服务(RPC模式) for [{}]...").format(self.environment_name))

        # +++ Get ZMQ addresses from ConfigManager +++
        order_gateway_rep_address = self.config_service.get_global_config("zmq_addresses.order_gateway_rep", "tcp://*:5558")
        order_gateway_pub_address = self.config_service.get_global_config("zmq_addresses.order_gateway_pub", "tcp://*:5557")

        if not order_gateway_rep_address or not order_gateway_pub_address:
            logger.error(self._("错误：未能从配置中获取订单网关 REP 或 PUB 地址。服务无法启动。"))
            return

        # Start RpcServer first
        try:
            super().start(
                rep_address=order_gateway_rep_address, # Use fetched address
                pub_address=order_gateway_pub_address  # Use fetched address
            )
            logger.info(self._("RPC 服务器已启动。 REP: {}, PUB: {}").format(order_gateway_rep_address, order_gateway_pub_address))
            # Set socket options...
            if hasattr(self, '_socket_pub') and self._socket_pub:
                try:
                    sndhwm = 1000
                    self._socket_pub.setsockopt(zmq.SNDHWM, sndhwm)
                    logger.info(self._("为 MAIN PUB socket (_socket_pub) 设置了 ZMQ.SNDHWM 选项: {}").format(sndhwm))
                except Exception as opt_err:
                     logger.error(self._("为 MAIN PUB socket (_socket_pub) 设置 ZMQ 选项时出错: {}").format(opt_err))
            else:
                 logger.warning(self._("无法设置 ZMQ 选项：RpcServer 未能成功创建 _socket_pub。"))
        except Exception as e:
            logger.exception(self._("启动 RPC 服务器时失败: {}").format(e))
            return

        # Start Publisher Thread
        if not self._publisher_thread or not self._publisher_thread.is_alive():
             self._publisher_active.set()
             self._publisher_thread = threading.Thread(target=self._run_publish, daemon=True)
             self._publisher_thread.start()
             logger.info(self._("发布线程已启动。"))
        else:
             logger.warning(self._("发布线程似乎已在运行。"))

        # --- Start Heartbeat Thread --- 
        if not self._heartbeat_thread or not self._heartbeat_thread.is_alive():
            self._heartbeat_active.set()
            self._heartbeat_thread = threading.Thread(target=self._run_heartbeat, daemon=True)
            self._heartbeat_thread.start()
            logger.info(self._("心跳线程已启动。"))
        else:
             logger.warning(self._("心跳线程似乎已在运行。"))
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
            logger.info(self._("事件引擎已启动，并注册了事件处理器。"))
        except Exception as e:
             logger.exception(self._("启动或注册事件引擎时出错: {}").format(e))
             self.stop()
             return

        # Connect CTP Gateway
        if not self.ctp_setting:
            logger.error(self._("CTP 设置未加载或环境无效，无法连接 CTP 网关。"))
            # Optionally stop the service here if CTP is essential
            # self.stop()
            return
        
        self._stored_ctp_setting = self.ctp_setting # Store for potential reconnection

        try:
            logger.info(self._("CTP 连接配置 (Env: {}): UserID={}, BrokerID={}, TD={}").format(
                self.environment_name,
                self.ctp_setting.get('userid'),
                self.ctp_setting.get('broker_id'),
                self.ctp_setting.get('td_address')
            ))
            self.gateway.connect(self.ctp_setting) # Uses the selected setting
            logger.info(self._("CTP 连接请求已发送。"))
        except Exception as err:
            logger.exception(self._("连接 CTP 交易网关时发生严重错误 (Env: {}): {}").format(self.environment_name, err))
            self.stop()
            return

        logger.info(self._("订单执行网关服务(RPC模式) for [{}] 启动完成。").format(self.environment_name))

    def stop(self):
        """
        停止所有组件。

        Stops all components.
        """
        # Check if already stopped or never started properly
        is_rpc_active = self.is_active()

        logger.info(self._("停止订单执行网关服务(RPC模式)..."))

        # --- Stop Heartbeat Thread --- 
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            logger.info(self._("正在停止心跳线程..."))
            self._heartbeat_active.clear()
            self._heartbeat_thread.join(timeout=2.0) # Wait a bit for it to finish
            if self._heartbeat_thread.is_alive():
                logger.error(self._("无法正常停止心跳线程。"))
            else:
                logger.info(self._("心跳线程已停止。"))
            self._heartbeat_thread = None
        # --- End Stop Heartbeat --- 

        # Stop Publisher Thread first
        if self._publisher_thread and self._publisher_thread.is_alive():
             logger.info(self._("正在停止发布线程..."))
             self._publisher_active.clear()
             self._publisher_thread.join(timeout=2.0)
             if self._publisher_thread.is_alive():
                  logger.error(self._("无法正常停止发布线程。"))
             else:
                  logger.info(self._("发布线程已停止。"))
             self._publisher_thread = None
        # elif was_publisher_running: # Log if thread was running but RpcServer wasn't active
        #      logger.warning("服务未激活，但发布线程仍在运行，已尝试停止。")

        # Stop Event Engine
        try:
            # Check if event engine was actually started and is active
            if hasattr(self.event_engine, '_active') and self.event_engine._active:
                self.event_engine.stop()
                logger.info(self._("事件引擎已停止。"))
            # else: # Optional: Log if never started/already stopped
            #     logger.info("事件引擎未运行或已停止。")
        except Exception as e:
            logger.exception(self._("停止事件引擎时出错: {}").format(e))

        # Close CTP Gateway
        if self.gateway:
            try:
                self.gateway.close()
                logger.info(self._("CTP 交易网关已关闭。"))
            except Exception as e:
                logger.exception(self._("关闭 CTP 网关时出错: {}").format(e))

        # Stop RpcServer last
        if is_rpc_active:
            try:
                 super().stop() # RpcServer.stop() closes sockets
                 logger.info(self._("RPC 服务器已停止。"))
            except Exception as e:
                 logger.exception(self._("停止 RPC 服务器时出错: {}").format(e))

        logger.info(self._("订单执行网关服务(RPC模式) for [{}] 已停止。").format(self.environment_name))
