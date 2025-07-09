import importlib
import logging
import os
import queue
import sys
import threading
import time
from datetime import datetime
from typing import Any, Dict, Optional, Tuple

import msgpack
import zmq

from config import global_vars  # 添加导入global_vars
from config.constants.params import Params
from config.constants.path import GlobalPath
from utils.file_helper import load_product_info
from utils.service_common import get_gateway
from vnpy.event.dedup_engine import DedupEventEngine
from vnpy.trader.utility import load_json
from vnpy_tts import TtsGateway
from vnpy.rpc.common import HEARTBEAT_TOPIC  # 添加导入HEARTBEAT_TOPIC常量

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.config_manager import ConfigManager
from utils.converter import convert_order_data_to_dict, dict_to_order_request, convert_vnpy_obj_to_dict
from utils.i18n import _
from utils.logger import logger, setup_logging, INFO, get_level_name

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
    TradeData, LogData,
)
from vnpy_ctp import CtpGateway

HEARTBEAT_INTERVAL_S = 5.0 # Send heartbeat every 5 seconds
CTP_CONNECTION_TIMEOUT_S = 30.0  # 与行情网关一致的超时设置


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
        
        # +++ 添加连接状态跟踪变量 +++
        self._td_connected: bool = False
        self._td_connection_event = threading.Event()
        # +++ 结束添加 +++

        # VNPY setup
        self.event_engine = DedupEventEngine()

        # --- Load and Select CTP Settings ---
        self.ctp_config_filename = Params.brokers_config_filename
        self.ctp_setting: dict | None = None
        self.product_info_filepath = GlobalPath.product_info_filepath
        self._last_logged_balance = None

        try:
            self.ctp_config_setting = load_json(self.ctp_config_filename)
            if self.environment_name != "":
                # +++ Use passed environment name +++
                self.ctp_setting = self.ctp_config_filename['brokers'][self.environment_name]
                logger.info(_("已加载环境 [{}] 的 CTP 设置").format(self.environment_name))
            elif self.ctp_config_setting['default_broker'] != "":
                # +++ If you specify a default value in the configuration file, use +++
                self.environment_name = self.ctp_config_setting['default_broker']
                self.ctp_setting = self.ctp_config_setting['brokers'][self.environment_name]
                logger.info(_("已加载环境 [{}] 的 CTP 设置").format(self.environment_name))
            else:
                logger.error(_("未找到环境 '{}'！无法连接 CTP。").format(self.environment_name))
                self.ctp_setting = None # Ensure it's None if not found
        except FileNotFoundError:
            logger.error(_("未找到 {}！无法连接 CTP。").format(self.ctp_config_filename))
            self.ctp_setting = None # Ensure it's None
        except Exception as err:
            logger.exception(_("加载或解析 {} 时出错: {}").format(self.ctp_config_filename, str(err)))
            self.ctp_setting = None # Ensure it's None
        # --- End Load and Select ---
        setup_logging(service_name=f"{__class__.__name__}[{self.environment_name}]", level=INFO)

        # 改为动态导入 gateway 模块
        self.gateway: CtpGateway | TtsGateway = get_gateway(self.event_engine, self.environment_name)
        self.contracts: Dict[str, ContractData] = {}
        self.last_account_data: AccountData | None = None

        # Load product info for commission calculation
        logger.info(_("尝试从 {} 加载产品信息...").format(self.product_info_filepath))
        self.commission_rules, self.contract_multipliers = load_product_info(self.product_info_filepath)
        if not self.commission_rules or not self.contract_multipliers:
             logger.warning(_("警告：未能成功加载手续费规则或合约乘数，手续费计算可能不准确！"))

        # Register RPC handlers
        self.register(self.send_order)
        self.register(self.cancel_order)
        self.register(self.query_contracts)
        self.register(self.query_account)
        self.register(self.ping)

        # --- Add Heartbeat Thread Variables ---
        self._heartbeat_active = threading.Event()
        self._heartbeat_thread = None
        self._heartbeat_topic = b"heartbeat.ordergw" # Define topic as bytes
        # --- End Add ---

        logger.info(_("订单执行网关服务(RPC模式) for [{}] 初始化完成。").format(environment_name))

    # --- RPC Handlers ---
    def send_order(self, req_dict: Dict[str, Any]) -> str | None:
        """
        用于发送订单请求的 RPC 处理程序。
        成功时返回 vt_orderid，失败时返回 None。

        RPC handler for sending an order request.
        Returns vt_orderid on success, None on failure.
        """
        logger.info(_("RPC: 收到 send_order 请求: {}").format(req_dict))
        order_request: OrderRequest | None = dict_to_order_request(req_dict)

        if not order_request:
            logger.error(_("RPC: send_order - 无法将请求转换为 OrderRequest。"))
            return None

        try:
            vt_orderid = self.gateway.send_order(order_request)
            if vt_orderid:
                logger.info(_("RPC: send_order - 订单已发送至 CTP: {}, VT_OrderID: {}").format(order_request.symbol, vt_orderid))
                self.active_order_ids.add(vt_orderid)
                return vt_orderid
            else:
                # Log if the gateway itself failed to send without raising an exception
                logger.error(_("RPC: send_order - CTP 网关未能发送订单 (gateway.send_order 返回 None): {}").format(order_request.symbol))
                # Potentially an issue, maybe try reconnecting?
                self._attempt_ctp_reconnect(reason=_("对于 {}，gateway.send_order 返回 None").format(order_request.symbol))
                return None
        except Exception as e:
            # Log exceptions raised by the gateway's send_order
            logger.exception(_("RPC: send_order - 发送订单时 CTP 网关出错: {}").format(e))
            self._attempt_ctp_reconnect(reason=_("send_order 异常：{}").format(e))
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
        logger.info(_("RPC: 收到 cancel_order 请求 for {}").format(vt_orderid))
        if not vt_orderid:
            logger.error(_("RPC: cancel_order - 收到无效的撤单指令：缺少 'vt_orderid' 字段。"))
            return {"status": "error", "message": "Missing vt_orderid"}

        try:
            # +++ Replace self.gateway.get_order with cache lookup +++
            # order_to_cancel = self.gateway.get_order(vt_orderid) # Original line - Error
            order_to_cancel = self.order_cache.get(vt_orderid)
            # +++ End Replace +++

            if not order_to_cancel:
                # logger.error(f"RPC: cancel_order - 尝试撤销失败：网关未找到订单 {vt_orderid}") # Original error message
                logger.warning(_("RPC: cancel_order - 尝试撤销失败：在内部缓存中未找到活动订单 {} (可能已完成或不存在)").format(vt_orderid)) # New warning
                return {f"status": "error", "message": "Order {vt_orderid} not found in active cache"}

            req = CancelRequest(
                orderid=vt_orderid, # Use the vt_orderid directly
                symbol=order_to_cancel.symbol,
                exchange=order_to_cancel.exchange,
            )
            self.gateway.cancel_order(req)
            logger.info(_("RPC: cancel_order - 撤单请求已发送 for {}").format(vt_orderid))
            return {f"status": "ok", "message": "Cancel request sent for {vt_orderid}"}

        except Exception as e:
            logger.exception(_("RPC: cancel_order - 处理撤单指令时出错 (vt_orderid: {}): {}").format(vt_orderid, e))
            return {f"status": "error", "message": "Error processing cancel command: {e}"}

    def query_contracts(self) -> Dict[str, Dict]:
        """
        RPC 处理程序用于查询可用的合约。

        RPC handler to query available contracts.
        """
        logger.info(_("RPC: 收到 query_contracts 请求"))
        return {vt_symbol: contract.__dict__ for vt_symbol, contract in self.contracts.items()}

    def query_account(self) -> Dict | None:
        """
        RPC 处理程序用于查询最新的帐户数据。

        RPC handler to query the latest account data.
        """
        logger.info(_("RPC: 收到 query_account 请求"))
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
            logger.error(_("无法尝试 CTP 重连：未存储 CTP 配置。无法获取行情服务器地址。")) # 更新错误信息
            return

        if current_time - self._last_reconnect_attempt_time < self._reconnect_interval_seconds:
            logger.info(_("CTP 重连请求过于频繁，已跳过。上次尝试在 {} 秒内。原因: {}").format(self._reconnect_interval_seconds, reason))
            return

        logger.warning(_("检测到 CTP 连接问题 (原因: {})，尝试重新连接 CTP交易接口...").format(reason)) # 更新日志
        self._last_reconnect_attempt_time = current_time
        try:
            # Ensure gateway object exists
            if not hasattr(self, 'gateway') or not self.gateway:
                 logger.error(_("无法重连 CTP：网关对象不存在。"))
                 return

            logger.info(_("调用 self.gateway.connect_td 使用已存储的配置进行 CTP交易接口 重连...").format(self._stored_ctp_setting)) # 更新日志和调用
            self.gateway.connect_td(self._stored_ctp_setting) # 修改为 connect_td
            logger.info(_("CTP 重新连接请求已发送。请监控后续日志确认连接状态。"))
        except Exception as e:
            logger.exception(_("尝试 CTP 重新连接时发生错误: {}").format(e))
    # +++ End Add CTP Reconnect Method +++

    # --- Event Processing (Restored with Filtering) ---
    def process_event(self, event: Event):
        """
        处理来自 EventEngine 的事件并将*过滤后的*事件放入发布队列。

        Processes events from the EventEngine and puts *filtered* events onto the publishing queue.
        """
        event_type = event.type
        data_obj = event.data

        # Increment counter for ALL received events
        with self._counter_lock:
            self._event_counter += 1

        # +++ 检测交易接口登录成功 +++
        if event_type == EVENT_LOG:
            log: LogData = data_obj
            # 检查是否是交易接口登录成功的日志
            if not self._td_connected and global_vars.td_login_success:
                logger.info(_("检测到交易接口登录成功信号 (via global_vars.td_login_success)！"))
                self._td_connected = True
                self._td_connection_event.set()
                
            # 其他日志处理逻辑...
            log_level_value = getattr(log, 'level', logging.INFO)
            logger_level = get_level_name(log_level_value)
            event_msg = log.msg
            logger.log(logger_level, _("[Event Logs - TDGW Processed] {}".format(event_msg)))
            
            # 注意：RpcServer.publish和_socket_pub.send的格式不同，这里我们直接使用父类的publish方法
            # RpcClient期望解包[topic, data]格式
            try:
                # 简化日志内容
                simple_log = {
                    "msg": event_msg,
                    "level": int(log_level_value),
                    "time": datetime.now().isoformat()
                }
                
                # 使用父类的publish方法，它会正确格式化数据
                super().publish("log", simple_log)
            except Exception as e:
                logger.error(_("使用RpcServer.publish发布日志时出错: {}").format(e))
            
            # 早期返回，因为已处理
            return
        # +++ 结束添加 +++

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
                 logger.debug(_("事件处理器: 过滤掉的事件类型: {} (总事件计数: {})").format(event_type, self._event_counter))
            # --- Handle side effects for non-published types ---
            if event_type == EVENT_CONTRACT:
                # Still store contract data locally for query_contracts RPC
                contract: ContractData = data_obj
                self.contracts[contract.vt_symbol] = contract
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
                        logger.debug(_("订单 {} 状态变为非活动 ({}), 已从 active_order_ids 移除。").format(order.vt_orderid, order.status.value))
                # +++ End remove order ID +++

                # +++ Add Order Status Logging (Conditional on active_order_ids) +++
                if order.vt_orderid in self.active_order_ids or not order.is_active():
                    # --- FIX: Add check for None datetime before formatting ---
                    time_str = order.datetime.strftime('%H:%M:%S') if order.datetime else "NoTime"
                    logger.info(
                        _("订单状态更新: [{}] VTOrderID={}, 状态={}, 已成交={}/{}, 价格={}, 代码={}, 方向={}, 开平={}").format(
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
                
                # 转换为正确格式并使用RpcServer.publish
                order_dict = convert_vnpy_obj_to_dict(order)
                super().publish(f"order.{order.vt_orderid}", order_dict)
                
            elif event_type == EVENT_TRADE:
                trade: TradeData = data_obj
                # +++ 无条件记录收到成交事件 +++
                logger.info(_("收到 EVENT_TRADE: VTOrderID={}, Symbol={}").format(
                    getattr(trade, 'vt_orderid', 'N/A'),
                    getattr(trade, 'vt_symbol', 'N/A')
                ))
                # +++ 结束无条件记录 +++

                # 转换为正确格式并使用RpcServer.publish
                trade_dict = convert_vnpy_obj_to_dict(trade)
                super().publish(f"trade.{trade.vt_symbol}", trade_dict)
                
            elif event_type == EVENT_ACCOUNT:
                account: AccountData = data_obj
                self.last_account_data = account # Update local cache
                accountid = account.accountid

                # 仅当Balance发生变化时才打印日志
                current_balance = account.balance
                if not hasattr(self, '_last_logged_balance'):
                    self._last_logged_balance = None
                if self._last_logged_balance != current_balance:
                    logger.info(
                        _("账户资金更新: AccountID={}, Balance={:.2f}, Available={:.2f}, Frozen={:.2f}").format(
                            accountid,
                            account.balance,
                            account.available,
                            account.frozen
                        )
                    )
                    self._last_logged_balance = current_balance
                
                # 转换为正确格式并使用RpcServer.publish
                account_dict = convert_vnpy_obj_to_dict(account)
                super().publish(f"account.{accountid}", account_dict)
                
            elif event_type == EVENT_POSITION:
                 position: PositionData = data_obj
                 # +++ Change Position Update Logging to DEBUG +++
                 logger.debug(
                     _("持仓更新: 代码={}, 方向={}, 数量={}, 昨仓={}, 冻结={}, 均价={:.3f}").format(
                         position.vt_symbol,
                         position.direction.value,
                         position.volume,
                         position.yd_volume,
                         position.frozen,
                         position.price
                     )
                 )
                 # --- End Position Update Logging ---
                 
                 # 转换为正确格式并使用RpcServer.publish
                 position_dict = convert_vnpy_obj_to_dict(position)
                 super().publish(f"position.{position.vt_symbol}", position_dict)

        except Exception as e:
             # Log error during processing/pickling *before* queueing
             logger.exception(_("处理 VNPY 事件 {} (类型: {}) 并准备放入队列时出错: {}").format(
                 event_type,
                 type(data_obj).__name__,
                 e
             ))

    # --- Heartbeat Thread Method ---
    def _run_heartbeat(self):
        """
        在单独的线程中运行以定期发送心跳消息。

        Runs in a separate thread to periodically send heartbeat messages.
        """
        logger.info(_("心跳线程启动 (间隔: {}s, 主题: {})。").format(HEARTBEAT_INTERVAL_S, self._heartbeat_topic.decode()))
        loop_count = 0
        while self._heartbeat_active.is_set():
            loop_count += 1
            # Check if publisher socket is ready
            socket_available = hasattr(self, '_socket_pub') and self._socket_pub and not self._socket_pub.closed
            if socket_available:
                try:
                    current_timestamp = time.time()
                    # 使用RpcServer.publish方法来发送心跳
                    self.publish(HEARTBEAT_TOPIC, current_timestamp)
                    # Log heartbeat sending occasionally
                    if loop_count % 12 == 1: # e.g., log every minute if interval is 5s
                        logger.debug(_("发送心跳: Timestamp={}").format(datetime.fromtimestamp(current_timestamp).isoformat()))
                except Exception as e:
                     logger.exception(_("发送心跳时出错: {}").format(e))
            else:
                # Log if socket is not ready (less frequently)
                if loop_count % 6 == 1:
                    logger.warning(_("心跳线程：MAIN PUB socket 不可用或已关闭，无法发送心跳。"))

            # Wait for the next interval, checking the active flag periodically
            # This allows faster shutdown than a single sleep(INTERVAL)
            wait_interval = 0.5 # Check every 0.5 seconds
            remaining_wait = HEARTBEAT_INTERVAL_S
            while remaining_wait > 0 and self._heartbeat_active.is_set():
                time.sleep(min(wait_interval, remaining_wait))
                remaining_wait -= wait_interval

        logger.info(_("心跳线程停止。"))
    # --- End Heartbeat Thread Method ---

    # --- Lifecycle Management (Simplified) ---
    def start(self, rep_address=None, pub_address=None):
        """
        启动 RpcServer、EventEngine、Gateway、Publisher 和 Heartbeat 线程。

        Starts RpcServer, EventEngine, Gateway, Publisher, and Heartbeat threads.
        """
        if self.is_active():
            logger.warning(_("订单执行网关服务(RPC模式) for [{}] 已在运行中。").format(self.environment_name))
            return

        logger.info(_("启动订单执行网关服务(RPC模式) for [{}]...").format(self.environment_name))

        # +++ Get ZMQ addresses from ConfigManager +++
        order_gateway_rep_address = self.config_service.get_global_config("zmq_addresses.order_gateway_rep", "tcp://*:5558")
        order_gateway_pub_address = self.config_service.get_global_config("zmq_addresses.order_gateway_pub", "tcp://*:5557")

        if not order_gateway_rep_address or not order_gateway_pub_address:
            logger.error(_("错误：未能从配置中获取订单网关 REP 或 PUB 地址。服务无法启动。"))
            return

        # Start RpcServer first
        try:
            super().start(
                rep_address=order_gateway_rep_address, # Use fetched address
                pub_address=order_gateway_pub_address  # Use fetched address
            )
            logger.info(_("RPC 服务器已启动。 REP: {}, PUB: {}").format(order_gateway_rep_address, order_gateway_pub_address))
            # Set socket options...
            if hasattr(self, '_socket_pub') and self._socket_pub:
                try:
                    sndhwm = 1000
                    self._socket_pub.setsockopt(zmq.SNDHWM, sndhwm)
                    logger.info(_("为 MAIN PUB socket (_socket_pub) 设置了 ZMQ.SNDHWM 选项: {}").format(sndhwm))
                except Exception as opt_err:
                     logger.error(_("为 MAIN PUB socket (_socket_pub) 设置 ZMQ 选项时出错: {}").format(opt_err))
            else:
                 logger.warning(_("无法设置 ZMQ 选项：RpcServer 未能成功创建 _socket_pub。"))
        except Exception as e:
            logger.exception(_("启动 RPC 服务器时失败: {}").format(e))
            return

        # --- Start Heartbeat Thread --- 
        if not self._heartbeat_thread or not self._heartbeat_thread.is_alive():
            self._heartbeat_active.set()
            self._heartbeat_thread = threading.Thread(target=self._run_heartbeat, daemon=True)
            self._heartbeat_thread.start()
            logger.info(_("心跳线程已启动。"))
        else:
             logger.warning(_("心跳线程似乎已在运行。"))
        # --- End Start Heartbeat --- 

        # Start Event Engine and Register Handlers
        try:
            self.event_engine.register(EVENT_ORDER, self.process_event)
            self.event_engine.register(EVENT_TRADE, self.process_event)
            self.event_engine.register(EVENT_LOG, self.process_event) # Still need to process for counter/side-effects if any
            self.event_engine.register(EVENT_CONTRACT, self.process_event) # Still need to process for side-effects
            self.event_engine.register(EVENT_ACCOUNT, self.process_event)
            self.event_engine.register(EVENT_POSITION, self.process_event) # Register handler for Position events
            # Register for any other events you might want to count or have side effects for
            self.event_engine.start()
            logger.info(_("事件引擎已启动，并注册了事件处理器。"))
        except Exception as e:
             logger.exception(_("启动或注册事件引擎时出错: {}").format(e))
             self.stop()
             return

        # Connect CTP Gateway
        if not self.ctp_setting:
            logger.error(_("CTP 设置未加载或环境无效，无法连接 CTP 网关。"))
            # Optionally stop the service here if CTP is essential
            self.stop()
            return
        
        self._stored_ctp_setting = self.ctp_setting # Store for potential reconnection

        try:
            logger.info(_("CTP 连接配置 (Env: {}): UserID={}, BrokerID={}, TD={}").format(
                self.environment_name,
                self.ctp_setting.get('userid'),
                self.ctp_setting.get('broker_id'),
                self.ctp_setting.get('td_address')
            ))
            
            # +++ 重置连接状态变量 +++
            global_vars.td_login_success = False
            self._td_connection_event.clear()
            self._td_connected = False
            # +++ 结束重置 +++
            
            self.gateway.connect_td(self.ctp_setting) # 只连接交易接口
            logger.info(_("CTP 连接请求已发送。等待 {} 秒连接...").format(CTP_CONNECTION_TIMEOUT_S))
            
            # +++ 添加连接超时处理 +++
            if self._td_connection_event.wait(timeout=CTP_CONNECTION_TIMEOUT_S):
                if self._td_connected:
                    logger.info(_("交易网关连接并登录成功 (环境: {})!").format(self.environment_name))
                else:
                    # 这种情况理论上不应该发生
                    logger.error(_("交易网关连接事件触发但未成功连接 (环境: {}).").format(self.environment_name))
                    self.stop()
                    return
            else:
                logger.error(_("交易网关连接超时 ({} 秒) (环境: {}). 请检查网络和配置.").format(CTP_CONNECTION_TIMEOUT_S, self.environment_name))
                # 记录当前global_vars.td_login_success状态，用于诊断
                logger.info(f"超时诊断: global_vars.td_login_success = {global_vars.td_login_success}")
                if hasattr(self.gateway, 'td_api') and self.gateway.td_api:
                    logger.info(f"超时诊断: td_api.connect_status = {self.gateway.td_api.connect_status}, td_api.login_status = {self.gateway.td_api.login_status}")
                self.stop()
                return
            # +++ 结束添加 +++
        except Exception as err:
            logger.exception(_("连接 CTP 交易网关时发生严重错误 (Env: {}): {}").format(self.environment_name, err))
            self.stop()
            return

        logger.info(_("订单执行网关服务(RPC模式) for [{}] 启动完成。").format(self.environment_name))

    def stop(self):
        """
        停止所有组件。

        Stops all components.
        """
        logger.info(_("停止订单执行网关服务(RPC模式)..."))

        # 1. Signal RpcServer's run loop to stop and wait for thread to join
        #    We assume super().stop() sets an internal _active flag to False
        #    and potentially tries to join the RpcServer's worker thread.
        #    It should NOT close sockets or terminate the context itself.
        if self.is_active(): # is_active() usually checks the RpcServer's _active flag
            try:
                logger.info(_("正在调用 RpcServer.stop()..."))
                super().stop() 
                logger.info(_("RpcServer.stop() 调用完成。")) # Indicates RpcServer thread should be stopping/stopped
            except Exception as e:
                logger.exception(_("调用 RpcServer.stop() 时出错: {}").format(e))
        
        # 2. Stop Heartbeat Thread
        self._td_connection_event.set()  # 唤醒任何等待者
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            logger.info(_("正在停止心跳线程..."))
            self._heartbeat_active.clear()
            self._heartbeat_thread.join(timeout=2.0) 
            if self._heartbeat_thread.is_alive():
                logger.error(_("无法正常停止心跳线程。"))
            else:
                logger.info(_("心跳线程已停止。"))
            self._heartbeat_thread = None

        # 3. Stop Event Engine
        try:
            if hasattr(self.event_engine, '_active') and self.event_engine._active:
                self.event_engine.stop()
                logger.info(_("事件引擎已停止。"))
            else:
                logger.info(_("事件引擎未运行或已停止。"))
        except Exception as e:
            logger.exception(_("停止事件引擎时出错: {}").format(e))

        # 4. Close CTP Gateway (This will call td_api.close() -> td_api.exit())
        if self.gateway:
            try:
                logger.info(_("正在关闭 CTP 网关..."))
                self.gateway.close()
                logger.info(_("CTP 网关已关闭。"))
            except Exception as e:
                # Log the error, but continue with ZMQ cleanup
                logger.exception(_("关闭 CTP 网关时出错: {}").format(e)) 

        # 5. Close ZMQ Sockets owned by this service (RpcServer's sockets)
        #    RpcServer.stop() should ideally not close these itself.
        #    If RpcServer.stop() DOES close them, these calls might error but are harmless.
        logger.info(_("正在关闭 OrderExecutionGatewayService ZMQ Sockets..."))
        sockets_to_close = []
        if hasattr(self, '_socket_rep') and self._socket_rep and not self._socket_rep.closed:
            sockets_to_close.append(self._socket_rep)
        if hasattr(self, '_socket_pub') and self._socket_pub and not self._socket_pub.closed:
            sockets_to_close.append(self._socket_pub)

        for sock in sockets_to_close:
            try:
                logger.debug(_("正在关闭 ZMQ Socket: {}").format(sock))
                sock.close(linger=0)
                logger.debug(_("ZMQ Socket {} 已关闭。").format(sock))
            except Exception as e_sock_close:
                logger.error(_("关闭 ZMQ Socket {} 时出错: {}").format(sock, e_sock_close))
        
        # 6. Terminate ZMQ Context (managed by RpcServer, usually RpcServer.terminate())
        #    However, if RpcServer doesn't expose terminate, or if we want explicit control:
        #    This step assumes the ZMQ context is accessible via self.context from RpcServer
        if hasattr(self, 'context') and self.context and not self.context.closed:
            try:
                logger.info(_("正在终止 ZMQ Context..."))
                self.context.term()
                logger.info(_("ZMQ Context 已成功终止。"))
            except Exception as e_term:
                logger.error(_("终止 ZMQ Context 时出错: {}").format(e_term))
        else:
            logger.info(_("ZMQ Context 未找到或已关闭，跳过终止。"))

        logger.info(_("订单执行网关服务(RPC模式) for [{}] 已停止。").format(self.environment_name))
