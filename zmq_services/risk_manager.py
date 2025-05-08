from typing import Dict, Any
import zmq
import time
import sys
import os
import pickle
import msgpack # Add msgpack import
from datetime import datetime, time as dt_time, timedelta # Add timedelta
from decimal import Decimal # Add Decimal for potential future use
from collections import defaultdict, deque

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# +++ Import ConfigManager +++
from utils.config_manager import ConfigManager
from utils.logger import logger
# --- Remove old config import --- 
# from config import zmq_config as config

# Import necessary vnpy constants and objects
try:
    from vnpy.trader.constant import Direction, Status, Exchange, Offset, OrderType, Product, OptionType
    from vnpy.trader.object import PositionData, TradeData, OrderData, AccountData, LogData, ContractData # Import ContractData
except ImportError:
    print("无法导入 vnpy 模块，请确保 vnpy 已安装。Risk Manager 功能可能受限。")
    # Define fallback constants/classes if needed for basic operation
    class Direction: LONG = "多"; SHORT = "空"
    class Status: SUBMITTING="提交中"; NOTTRADED="未成交"; PARTTRADED="部分成交"; ALLTRADED="全部成交"; CANCELLED="已撤销"; REJECTED="拒单"
    class Offset: OPEN="开"; CLOSE="平"; CLOSETODAY="平今"; CLOSEYESTERDAY="平昨"
    class OrderData: pass
    class TradeData: pass
    class AccountData: pass
    class LogData: pass
    class ContractData: pass

# --- Remove CHINA_TZ specific import/fallback --- 
# try:
#     from config.zmq_config import CHINA_TZ
# except ImportError:
#     # Fallback if not defined in config (requires zoneinfo package)
#     try:
#         from zoneinfo import ZoneInfo
#         CHINA_TZ = ZoneInfo("Asia/Shanghai")
#         logger.warning("CHINA_TZ not found in config, using zoneinfo fallback.")
#     except ImportError:
#         logger.error("zoneinfo package not found and CHINA_TZ not in config. Trading hours check might be inaccurate.")
#         # Define a dummy TZ or raise error if critical
#         # from datetime import timedelta # Need timedelta for DummyTZ (already imported)
#         class DummyTZ: # Basic placeholder, WILL NOT handle DST correctly
#             def utcoffset(self, dt): return timedelta(hours=8)
#             def dst(self, dt): return timedelta(0)
#             def tzname(self, dt): return "Asia/Shanghai_Fallback"
#         CHINA_TZ = DummyTZ()

# --- Remove module-level constants, will be fetched via ConfigManager ---
# MARKET_DATA_TIMEOUT = 30.0 # seconds - Timeout for considering market data stale
# PING_INTERVAL = 5.0  # seconds
# PING_TIMEOUT_MS = 2500 # milliseconds

# --- Helper Function for Active Order Check --- 
def is_order_active_dict(order_dict: dict) -> bool:
    """Checks if an order dictionary represents an active order based on its status string."""
    status_str = order_dict.get('status')
    # Compare against string values of active Status enums
    return status_str in (
        Status.SUBMITTING.value,
        Status.NOTTRADED.value,
        Status.PARTTRADED.value
    )

# --- Risk Manager Service ---
class RiskManagerService:
    def __init__(self, config_manager: ConfigManager):
        """Initializes the risk manager service."""
        # +++ Use passed ConfigManager instance +++
        self.config_service = config_manager

        # +++ Load configuration using ConfigManager +++
        # ZMQ Addresses
        md_pub_addr = self.config_service.get_global_config("zmq_addresses.market_data_pub", "tcp://*:5555")
        order_report_pub_addr = self.config_service.get_global_config("zmq_addresses.order_gateway_pub", "tcp://*:5557")
        order_gateway_rep_addr = self.config_service.get_global_config("zmq_addresses.order_gateway_rep", "tcp://*:5558")
        self.alert_pub_url = self.config_service.get_global_config("zmq_addresses.risk_alert_pub") # Optional, might be None
        
        # Convert bind addresses to connect addresses for client sockets
        self.market_data_url = self._get_connect_url(md_pub_addr)
        self.order_report_url = self._get_connect_url(order_report_pub_addr)
        self._command_connect_url = self._get_connect_url(order_gateway_rep_addr)

        # Risk Parameters
        self.position_limits = self.config_service.get_global_config("risk_management.max_position_limits", {})
        self.max_pending_per_contract = self.config_service.get_global_config("risk_management.max_pending_orders_per_contract", 5)
        self.global_max_pending = self.config_service.get_global_config("risk_management.global_max_pending_orders", 20)
        self.margin_limit_ratio = self.config_service.get_global_config("risk_management.margin_usage_limit", 0.8)
        self.subscribed_symbols = set(self.config_service.get_global_config("default_subscribe_symbols", []))
        self.market_data_timeout = self.config_service.get_global_config("engine_communication.market_data_timeout_seconds", 30.0) # Borrow from engine settings
        self.ping_interval = self.config_service.get_global_config("engine_communication.ping_interval_seconds", 5.0)
        # +++ Fetch ping timeout +++
        self.ping_timeout_ms = self.config_service.get_global_config("engine_communication.ping_timeout_ms", 2500)

        # Log fetched config
        logger.info(f"Risk Manager Config: MD URL={self.market_data_url}, Report URL={self.order_report_url}, Command URL={self._command_connect_url}")
        logger.info(f"Risk Manager Config: Alert URL={self.alert_pub_url}")
        logger.info(f"Risk Manager Config: Position Limits={self.position_limits}")
        logger.info(f"Risk Manager Config: Max Pending/Contract={self.max_pending_per_contract}, Global Max Pending={self.global_max_pending}")
        logger.info(f"Risk Manager Config: Margin Limit Ratio={self.margin_limit_ratio}")
        logger.info(f"Risk Manager Config: Symbols to Check Timeout={self.subscribed_symbols}")

        # Initialize ZMQ components
        self.context = zmq.Context()
        self.command_socket = None # Initialized in _setup_command_socket
        self._setup_command_socket() # Setup initial command socket

        # Subscriber socket
        self.subscriber = self.context.socket(zmq.SUB)

        # Connect to both publishers
        self.subscriber.connect(self.market_data_url)
        self.subscriber.connect(self.order_report_url)
        logger.info(f"数据订阅器连接到: {self.market_data_url}")
        logger.info(f"数据订阅器连接到: {self.order_report_url}")

        # Subscribe to relevant topics (lowercase prefixes)
        prefixes_to_subscribe = [
            "tick.",
            "trade.",
            "order.",     # Correct prefix for order updates
            "account.",   # Correct prefix for account updates
            "log.",       # Subscribe to logs as well
            "contract."   # Subscribe to contracts
        ]
        for prefix in prefixes_to_subscribe:
            self.subscriber.subscribe(prefix.encode('utf-8'))
            logger.info(f"订阅主题前缀: {prefix}")

        # --- State ---
        self.positions: Dict[str, float] = {} # vt_symbol -> net position (using float for now)
        self.account_data: Dict[str, Any] | None = None # Store latest account data as dict
        self.active_orders: Dict[str, Dict[str, Any]] = {} # vt_orderid -> Order Dictionary
        self.last_order_status: Dict[str, str] = {} # vt_orderid -> last logged status string
        logger.info(f"加载持仓限制: {self.position_limits}")

        # Market Data Status
        self.last_tick_time: Dict[str, float] = {} # vt_symbol -> last reception timestamp
        self.market_data_ok: bool = True
        # Assume we need ticks for symbols we might have limits for or defined in config
        # A more robust way might be to dynamically get subscribed symbols if possible
        # self.subscribed_symbols: set = set(config.SUBSCRIBE_SYMBOLS) # Moved loading to above
        # logger.info(f"将监控以下合约行情超时: {self.subscribed_symbols}") # Moved logging to above

        # Gateway Connection Status
        self.gateway_connected = True # Assume connected initially
        self.last_ping_time = time.time()

        # +++ Initialize Account Log Throttle Variables +++
        self.account_log_interval: float = 60.0 # Log every 60 seconds
        self.last_account_log_time: float = 0.0
        self.last_logged_account_key_info: tuple = ('0.0', '0.0', '0.0', '0.0') # Store as strings
        # +++ End Initialization +++

        self.running = False

        # --- Add recently processed trade IDs --- 
        self.processed_trade_ids: deque[str] = deque(maxlen=1000) # Store last 1000 trade IDs
        # --- End Add ---

        logger.info("风险管理器初始化完成。")

    def _get_connect_url(self, base_url: str) -> str:
        """Replaces wildcard address with localhost for connection."""
        if base_url.startswith("tcp://*"):
            return base_url.replace("tcp://*", "tcp://localhost", 1)
        elif base_url.startswith("tcp://0.0.0.0"):
            return base_url.replace("tcp://0.0.0.0", "tcp://localhost", 1)
        else:
            return base_url

    def _setup_command_socket(self):
        """Sets up or resets the command REQ socket."""
        if hasattr(self, 'command_socket') and self.command_socket:
            logger.info("尝试关闭旧的指令 Socket...")
            try:
                self.command_socket.close(linger=0)
            except Exception as e_close:
                 logger.warning(f"关闭旧指令 Socket 时出错: {e_close}")
        
        logger.info(f"正在创建并连接指令 Socket 到: {self._command_connect_url}")
        self.command_socket = self.context.socket(zmq.REQ)
        self.command_socket.setsockopt(zmq.LINGER, 0)
        try:
             self.command_socket.connect(self._command_connect_url)
             # Connection might not happen immediately, ping will verify
        except Exception as e_conn:
             logger.error(f"连接指令 Socket 时出错: {e_conn}")
             # Mark as disconnected immediately if connection fails
             if self.gateway_connected:
                 logger.error("与订单执行网关的连接丢失 (Connection Error)! ")
                 self.gateway_connected = False

    def _is_trading_hours(self) -> bool:
        """
        Checks if the current time (in China Standard Time) is within any defined trading session,
        handling overnight sessions and skipping weekends.
        """
        try:
            # +++ Import ZoneInfo here for timezone calculation +++
            try:
                from zoneinfo import ZoneInfo
                china_tz = ZoneInfo("Asia/Shanghai")
            except ImportError:
                logger.error("zoneinfo package not found. Trading hours check might be inaccurate.")
                # Define a dummy TZ or return True (fail open)
                class DummyTZ: 
                    def utcoffset(self, dt): return timedelta(hours=8)
                    def dst(self, dt): return timedelta(0)
                    def tzname(self, dt): return "Asia/Shanghai_Fallback"
                china_tz = DummyTZ()
            # +++ End Import ZoneInfo +++

            # Get current time in China Standard Time
            now_dt_aware = datetime.now(china_tz)
            current_time = now_dt_aware.time()
            current_weekday = now_dt_aware.weekday() # Monday is 0 and Sunday is 6

            # Skip weekends (Saturday: 5, Sunday: 6)
            if current_weekday >= 5:
                # Log occasionally if skipping checks due to weekend
                # Use a simple counter or time-based throttle if needed
                # logger.debug("Skipping trading hours check: Weekend.")
                return False

            # Check against defined sessions
            # +++ Get sessions from config_service +++
            futures_trading_sessions = self.config_service.get_global_config("risk_management.futures_trading_sessions", [])
            for start_str, end_str in futures_trading_sessions:
                start_time = dt_time.fromisoformat(start_str) # HH:MM
                end_time = dt_time.fromisoformat(end_str)

                # Case 1: Same-day session (e.g., 09:00 - 11:30)
                if start_time <= end_time:
                    if start_time <= current_time < end_time:
                        # logger.debug(f"Trading hours check: In session {start_str}-{end_str}")
                        return True
                # Case 2: Overnight session (e.g., 21:00 - 02:30)
                else: # start_time > end_time
                    if current_time >= start_time or current_time < end_time:
                        # logger.debug(f"Trading hours check: In overnight session {start_str}-{end_str}")
                        return True

            # If no sessions match
            # logger.debug("Trading hours check: Outside all defined sessions.")
            return False

        except Exception as e:
            logger.error(f"检查交易时间时出错: {e}")
            return True # Fail open: assume trading hours if config is wrong or time conversion fails

    def update_position(self, trade_dict: dict):
        """Updates position based on trade dictionary data."""
        # Access fields using .get()
        vt_symbol = trade_dict.get('vt_symbol')
        direction_str = trade_dict.get('direction') # Direction string (e.g., "多", "空")
        volume = trade_dict.get('volume')         # Should be number (int/float)
        offset_str = trade_dict.get('offset')           # Offset string (e.g., "开", "平")

        if not all([vt_symbol, direction_str is not None, volume is not None, offset_str is not None]):
            logger.error(f"错误：成交回报字典缺少关键字段 (vt_symbol, direction, volume, offset)。Trade Dict: {trade_dict}")
            return None, None

        # --- Position Update Logic using string comparisons --- 
        previous_pos = self.positions.get(vt_symbol, 0.0)
        pos_change = 0.0
        # Ensure volume is float for calculation
        try:
            volume = float(volume)
        except (TypeError, ValueError):
             logger.error(f"错误：成交回报字典中的 volume 不是有效数字。Trade Dict: {trade_dict}")
             return None, None

        if direction_str == Direction.LONG.value: # Buy
            if offset_str == Offset.OPEN.value:
                pos_change = volume
            else: # Buy to close short (Offset can be CLOSE, CLOSETODAY, CLOSEYESTERDAY)
                pos_change = volume
        elif direction_str == Direction.SHORT.value: # Sell
            if offset_str == Offset.OPEN.value:
                pos_change = -volume
            else: # Sell to close long (Offset can be CLOSE, CLOSETODAY, CLOSEYESTERDAY)
                pos_change = -volume
        else:
             logger.warning(f"未知的成交方向: {direction_str}. Trade Dict: {trade_dict}")
             return None, None
        # --- End Logic --- 

        # Update position map (using float)
        new_pos = previous_pos + pos_change
        self.positions[vt_symbol] = new_pos
        logger.info(f"持仓更新: {vt_symbol} | Prev={previous_pos:.4f} | Change={pos_change:.4f} | New={new_pos:.4f} | Trade(Dir={direction_str}, Off={offset_str}, Vol={volume})")

        return vt_symbol, new_pos

    def update_active_orders(self, order_dict: dict):
        """Updates the dictionary of active orders using order dictionary."""
        vt_orderid = order_dict.get('vt_orderid')
        if not vt_orderid:
            logger.warning("接收到缺少 vt_orderid 的订单更新，无法处理。 Order Dict: {order_dict}")
            return

        if is_order_active_dict(order_dict):
            self.active_orders[vt_orderid] = order_dict
            # logger.debug(f"Active order dict added/updated: {vt_orderid} Status: {order_dict.get('status')}")
        elif vt_orderid in self.active_orders:
            del self.active_orders[vt_orderid]
            # logger.debug(f"Inactive order dict removed: {vt_orderid} Status: {order_dict.get('status')}")

        # Update last known status string
        status_str = order_dict.get('status')
        if status_str:
            self.last_order_status[vt_orderid] = status_str

    def check_risk(self, vt_symbol: str = None, trigger_event: str = "UNKNOWN", current_position: float = None):
        """Checks various risk limits based on current state (uses dictionaries). Logs market data status."""
        # Log market data status
        if not self.market_data_ok:
            logger.warning("[Risk Check] 行情数据流异常，部分依赖市价的检查可能不准确或已跳过。")

        # --- 1. Position Limit Check (Per Symbol) --- 
        if vt_symbol:
            # self.positions stores floats, comparison is fine
            position = self.positions.get(vt_symbol, 0.0)
            limit = self.position_limits.get(vt_symbol)
            if limit is not None and abs(position) > limit:
                logger.warning(f"[风险告警] 合约 {vt_symbol}: 持仓 {position:.4f} 超出限制 {limit}! (Trigger: {trigger_event})")
                # Potential Action: Send closing orders

        # --- 2. Pending Order Limit Check --- 
        global_pending_count = 0
        pending_per_contract = defaultdict(int)
        active_order_dicts = [] # Store dicts of active orders
        orders_to_potentially_cancel = defaultdict(list)
        
        # Iterate through stored order dictionaries
        for order_dict in self.active_orders.values():
            if is_order_active_dict(order_dict):
                global_pending_count += 1
                symbol = order_dict.get('vt_symbol')
                if symbol:
                     pending_per_contract[symbol] += 1
                     orders_to_potentially_cancel[symbol].append(order_dict)
                active_order_dicts.append(order_dict)

        # --- 2a. Global Pending Order Limit --- 
        if global_pending_count > self.global_max_pending:
            logger.warning(f"[风险告警] 全局活动订单数 {global_pending_count} 超出限制 {self.global_max_pending}! (Trigger: {trigger_event})")
            # Action: Cancel the oldest pending order globally
            oldest_order_dict = None
            min_datetime = None
            for o_dict in active_order_dicts:
                 dt_str = o_dict.get('datetime')
                 if dt_str:
                      try:
                          current_dt = datetime.fromisoformat(dt_str)
                          if min_datetime is None or current_dt < min_datetime:
                               min_datetime = current_dt
                               oldest_order_dict = o_dict
                      except ValueError:
                           logger.warning(f"无法解析订单时间戳: {dt_str} for order {o_dict.get('vt_orderid')}")
           
            if oldest_order_dict:
                 oldest_vt_orderid = oldest_order_dict.get('vt_orderid')
                 if oldest_vt_orderid:
                      logger.warning(f"尝试撤销最旧的全局挂单: {oldest_vt_orderid}")
                      self._send_cancel_request(oldest_vt_orderid)
                 else:
                      logger.error("找到最旧订单但缺少 vt_orderid")

        # --- 2b. Per-Contract Pending Order Limit --- 
        symbols_to_check = [vt_symbol] if vt_symbol else list(pending_per_contract.keys())
        for symbol in symbols_to_check:
             if symbol is None: continue
             count = pending_per_contract.get(symbol, 0)
             limit_per = self.max_pending_per_contract
             if count > limit_per:
                 logger.warning(f"[风险告警] 合约 {symbol}: 活动订单数 {count} 超出限制 {limit_per}! (Trigger: {trigger_event})")
                 # Action: Cancel the oldest active order for this specific symbol
                 symbol_orders = orders_to_potentially_cancel.get(symbol, [])
                 oldest_symbol_order_dict = None
                 min_symbol_datetime = None
                 for o_dict in symbol_orders:
                    dt_str = o_dict.get('datetime')
                    if dt_str:
                        try:
                            current_dt = datetime.fromisoformat(dt_str)
                            if min_symbol_datetime is None or current_dt < min_symbol_datetime:
                                min_symbol_datetime = current_dt
                                oldest_symbol_order_dict = o_dict
                        except ValueError:
                            logger.warning(f"无法解析订单时间戳: {dt_str} for order {o_dict.get('vt_orderid')}")
                
                 if oldest_symbol_order_dict:
                     order_to_cancel_id = oldest_symbol_order_dict.get('vt_orderid')
                     if order_to_cancel_id:
                         logger.warning(f"尝试撤销合约 {symbol} 最旧的挂单: {order_to_cancel_id}")
                         self._send_cancel_request(order_to_cancel_id)
                     else:
                         logger.error(f"找到合约 {symbol} 最旧订单但缺少 vt_orderid")

        # --- 3. Margin Usage Check --- 
        if self.account_data: # Check if account dictionary exists
            try:
                # Get string representations and convert to float for calculation
                balance_str = self.account_data.get('balance', '0.0')
                available_str = self.account_data.get('available', '0.0')
                balance = float(balance_str if balance_str is not None else '0.0')
                available = float(available_str if available_str is not None else '0.0')
                
                required_margin = balance - available
                
                # Calculate margin ratio, handle division by zero
                margin_ratio = required_margin / balance if balance > 0 else 0.0
                if margin_ratio > self.margin_limit_ratio:
                    # Log using floats
                    logger.warning(f"[风险告警] 保证金占用率 {margin_ratio:.2%} 超出限制 {self.margin_limit_ratio:.2%}! (Balance={balance:.2f}, Available={available:.2f}, Trigger={trigger_event})")
                    # Potential Action: Cancel orders or liquidate positions (complex)
            except (TypeError, ValueError) as e:
                 logger.error(f"保证金检查计算错误：无法将 balance/available 转换为数字。Account data: {self.account_data}. Error: {e}")
            except Exception as e_margin:
                 logger.exception(f"保证金检查时发生意外错误: {e_margin}")

    def _send_cancel_request(self, vt_orderid: str):
        """Sends a cancel order request to the Order Execution Gateway."""
        if not vt_orderid:
            logger.error("尝试发送空 vt_orderid 的撤单请求。")
            return # Don't proceed if no order ID

        # Check gateway connection status before sending
        if not self.gateway_connected:
            logger.error(f"无法发送撤单指令 ({vt_orderid})：与订单执行网关失去连接。")
            return

        logger.info(f"发送撤单指令给网关: {vt_orderid}")
        # Format according to vnpy.rpc: (method_name, args_tuple, kwargs_dict)
        # cancel_order expects one positional argument: a dictionary
        req_data = {"vt_orderid": vt_orderid}
        req_tuple = ("cancel_order", (req_data,), {})

        try:
            # Use msgpack for REQ/REP communication
            packed_request = msgpack.packb(req_tuple, use_bin_type=True)
            self.command_socket.send(packed_request)

            # Wait for the reply with a timeout
            # Use poll for non-blocking wait with timeout
            poller = zmq.Poller()
            poller.register(self.command_socket, zmq.POLLIN)
            timeout_ms = 5000 # 5 seconds timeout
            events = dict(poller.poll(timeout_ms))

            if self.command_socket in events:
                packed_reply = self.command_socket.recv()
                # Use msgpack for REQ/REP communication
                reply = msgpack.unpackb(packed_reply, raw=False)
                logger.info(f"收到撤单指令回复 ({vt_orderid}): {reply}")
            else:
                logger.error(f"撤单指令 ({vt_orderid}) 请求超时 ({timeout_ms}ms)。")
                # Handle timeout: maybe reconnect or log error persistently
                # Recreating socket on timeout might be necessary
                # self.command_socket.close()
                # self.command_socket = self.context.socket(zmq.REQ)
                # self.command_socket.connect(...) 

        except zmq.ZMQError as e:
            logger.error(f"发送撤单指令 ({vt_orderid}) 时 ZMQ 错误: {e}")
            # Consider reconnecting or handling specific errors
        except Exception as e:
            logger.exception(f"发送或处理撤单指令 ({vt_orderid}) 回复时出错：{e}")

    def _send_ping(self):
        """Sends a PING request to the Order Gateway and handles the reply."""
        # Determine log prefix based on current assumed state
        log_prefix = "[Ping]" if self.gateway_connected else "[Ping - Attempting Reconnect]"
        logger.debug(f"{log_prefix} Sending...")

        # Format according to vnpy.rpc: ("ping", (), {})
        req_tuple = ("ping", (), {})
        current_time = time.time()
        self.last_ping_time = current_time # Update last ping attempt time

        try:
            # Use Poller for non-blocking send with timeout check
            poller = zmq.Poller()
            poller.register(self.command_socket, zmq.POLLOUT | zmq.POLLIN) # Check if writable and readable

            # Send PING using msgpack with the correct format
            packed_request = msgpack.packb(req_tuple, use_bin_type=True)
            self.command_socket.send(packed_request) # Use blocking send, rely on timeout/error for issues

            # Wait for PONG reply with timeout
            readable = dict(poller.poll(self.ping_timeout_ms))
            if self.command_socket in readable and readable[self.command_socket] & zmq.POLLIN:
                packed_reply = self.command_socket.recv(zmq.NOBLOCK)
                # Decode PONG using msgpack - RpcServer's ping returns "pong"
                reply = msgpack.unpackb(packed_reply, raw=False)
                # Check for the RpcServer success format [True, "pong"]
                if isinstance(reply, (list, tuple)) and len(reply) == 2 and reply[0] is True and reply[1] == "pong":
                    logger.debug("Received PONG successfully.")
                    if not self.gateway_connected:
                         logger.info("与订单执行网关的连接已恢复。")
                         self.gateway_connected = True # Mark as connected
                else:
                    logger.warning(f"Received unexpected reply to PING: {reply}")
                    if self.gateway_connected:
                         logger.error("与订单执行网关的连接可能存在问题 (Unexpected PING reply)! ")
                         self.gateway_connected = False
            else:
                # Timeout waiting for reply
                logger.warning(f"{log_prefix} PING request timed out after {self.ping_timeout_ms}ms.")
                # Mark as disconnected (if not already) and trigger reconnection
                if self.gateway_connected: # Log error only on first detection
                    logger.error(f"与订单执行网关的连接丢失 (PING timeout)!")
                self.gateway_connected = False
                logger.info("尝试重置指令 Socket (因 PING 超时)...")
                self._setup_command_socket()
                return # Exit after attempting reconnect

        except zmq.ZMQError as e:
            # Handle errors during send or recv
            logger.error(f"{log_prefix} 发送 PING 或接收 PONG 时 ZMQ 错误: {e}")
            # Mark as disconnected (if not already) and trigger reconnection
            if self.gateway_connected: # Log error only on first detection
                logger.error(f"与订单执行网关的连接丢失 ({e})! ")
            self.gateway_connected = False
            logger.info("尝试重置指令 Socket (因 ZMQ 错误)...")
            self._setup_command_socket()
            return # Exit after attempting reconnect

        except Exception as e:
            # Handle other unexpected errors
            logger.exception(f"{log_prefix} 发送或处理 PING/PONG 时发生未知错误：{e}")
            # Mark as disconnected (if not already)
            if self.gateway_connected: # Log error only on first detection
                logger.error("与订单执行网关的连接丢失 (Unknown Error)!")
                self.gateway_connected = False
            # Optional: Trigger reconnection on unknown errors too?

    def start(self):
        """Starts the risk manager service loop."""
        if self.running:
            logger.warning("风险管理器已在运行中。")
            return

        logger.info("启动风险管理器服务...")
        self.running = True
        # Start processing thread if needed (example implementation)
        # self.processing_thread = threading.Thread(target=self._run_processing_loop)
        # self.processing_thread.daemon = True
        # self.processing_thread.start()

        logger.info("风险管理器服务已启动，开始监听消息...")

        while self.running:
            # Poll subscriber socket with a timeout
            poller = zmq.Poller()
            poller.register(self.subscriber, zmq.POLLIN)
            poll_timeout_ms = 1000 # Check every second
            socks = dict(poller.poll(poll_timeout_ms))

            try:
                # --- Process incoming PUB/SUB messages ---
                if self.subscriber in socks and socks[self.subscriber] == zmq.POLLIN:
                    # Receive multipart message
                    parts = self.subscriber.recv_multipart(zmq.NOBLOCK)
                    logger.debug(f"Received Raw multipart: Count={len(parts)}")

                    if len(parts) == 2:
                        topic_bytes, data_bytes = parts
                    else:
                        logger.warning(f"收到包含意外部分数量 ({len(parts)}) 的消息: {parts}")
                        continue # Skip processing

                    if not self.running: break

                    try:
                        topic_str = topic_bytes.decode('utf-8', errors='ignore')
                        # Use msgpack.Unpacker for more robust decoding
                        # data_obj = msgpack.unpackb(data_bytes, raw=False)
                        unpacker = msgpack.Unpacker(raw=False) # Create unpacker
                        unpacker.feed(data_bytes)             # Feed the received bytes
                        data_obj = next(unpacker)             # Unpack the first (and expected only) object
                        
                        # Optional: Check for unexpected extra data - might be noisy
                        # try:
                        #    next(unpacker)
                        #    logger.warning(f"[RM] Unexpected extra data found after unpacking msgpack object. Topic: {topic_str}")
                        # except StopIteration:
                        #    pass # This is the expected case
                            
                        logger.debug(f"[RM] Received Correct: Topic='{topic_str}', Type='{type(data_obj)}'")

                        # Process based on topic (data_obj is now a dictionary)
                        if topic_str.startswith("tick."):
                            vt_symbol = topic_str[len("tick."):]
                            self.process_tick(vt_symbol, data_obj)
                        elif topic_str.startswith("order."):
                            self.process_order_update(data_obj)
                        elif topic_str.startswith("trade."):
                            self.process_trade_update(data_obj)
                        elif topic_str.startswith("account."):
                            self.process_account_update(data_obj)
                        elif topic_str == "log": # Match exact 'log' topic
                            self.process_log(data_obj)
                        elif topic_str.startswith("contract."):
                            self.process_contract(data_obj)
                        else:
                             logger.warning(f"未知的消息主题: {topic_str}")

                    # Update exception handling for msgpack errors
                    except StopIteration: # Add StopIteration catch for unpacker
                         logger.error(f"Msgpack Unpacker 错误: 无法从数据中解包对象. Topic: {topic_str}")
                    except (msgpack.UnpackException, msgpack.exceptions.ExtraData, TypeError, ValueError) as e_msgpack:
                        logger.error(f"Msgpack 解码错误: {e_msgpack}. Topic: {topic_str}") # Use topic_str here
                    except Exception as msg_proc_e:
                        logger.exception(f"处理订阅消息时出错 (Topic: {topic_str}): {msg_proc_e}") # Use topic_str here

                # --- Periodic Health Checks (moved from _run_processing_loop) ---
                current_time = time.time()
                if current_time - self.last_ping_time >= self.ping_interval:
                     self._send_ping()
                # --- End Heartbeat Check --- 

                # --- Periodic Market Data Timeout Check --- 
                # Check if any subscribed symbol hasn't received a tick recently
                # TODO: Add trading session check to avoid false alarms outside trading hours
                # Only check for stale data during defined trading hours
                if self._is_trading_hours():
                    found_stale_symbol = False
                    stale_symbols = []
                    check_time = time.time()
                    for symbol in self.subscribed_symbols:
                        last_ts = self.last_tick_time.get(symbol)
                        if last_ts is None:
                            # If a subscribed symbol NEVER received a tick, consider it stale after a grace period
                            if check_time - self.last_ping_time > self.ping_interval * 2: # Allow some time after start
                                found_stale_symbol = True
                                stale_symbols.append(f"{symbol} (no tick received)")
                        elif check_time - last_ts > self.market_data_timeout:
                            found_stale_symbol = True
                            stale_symbols.append(f"{symbol} (last {check_time - last_ts:.1f}s ago)")

                    if found_stale_symbol:
                        if self.market_data_ok: # Log only when status changes to False
                            logger.warning(f"[交易时段内] 行情数据可能中断或延迟! 超时合约: {', '.join(stale_symbols)}")
                            self.market_data_ok = False
                    elif not self.market_data_ok:
                        # If no symbols are stale now, but status was False, means it recovered
                        logger.info("所有监控合约的行情数据流已恢复。")
                        self.market_data_ok = True
                # --- End Market Data Timeout Check --- 

            except zmq.ZMQError as err:
                # Check if the error occurred because we are stopping
                if not self.running or err.errno == zmq.ETERM:
                    logger.info(f"ZMQ 错误 ({err.errno}) 发生在服务停止期间或Context终止，正常退出循环。")
                    break # Exit loop cleanly
                else:
                    logger.error(f"意外的 ZMQ 错误: {err}")
                    self.running = False # Stop on other ZMQ errors
            except KeyboardInterrupt:
                logger.info("检测到中断信号，开始停止...")
                self.running = False
            except Exception as err:
                logger.exception(f"主循环处理消息时发生未知错误：{err}")
                # Avoid rapid looping on persistent errors
                time.sleep(1) 

        logger.info("风险管理器主循环结束。")
        self.stop() # Call stop which closes sockets/context

    def stop(self):
        """Stops the service and cleans up resources."""
        if not self.running:
            logger.warning("风险管理器未运行。")
            return

        # Prevent starting new processing if stop is called concurrently
        if not self.running:
            return 

        logger.info("正在停止风险管理器服务...")
        self.running = False

        # Close sockets and context
        logger.info("关闭 ZMQ sockets 和 context...")
        if self.subscriber:
            self.subscriber.close()
            logger.info("ZeroMQ 订阅器已关闭。")
        if self.command_socket:
            self.command_socket.close()
            logger.info("ZeroMQ 指令发送器已关闭。")
        if self.context:
            try:
                self.context.term()
                logger.info("ZeroMQ Context 已终止。")
            except zmq.ZMQError as e:
                 logger.error(f"终止 ZeroMQ Context 时出错 (可能已终止): {e}")
        logger.info("风险管理器已停止。")

    def process_tick(self, vt_symbol, tick_dict: dict):
        """Processes tick dictionary."""
        # Update last tick time
        self.last_tick_time[vt_symbol] = time.time()
        # Add specific market risk logic here if needed, using tick_dict.get(...)
        # e.g., last_price = tick_dict.get('last_price')
        # logger.debug(f"Tick received for {vt_symbol}: {tick_dict.get('last_price')}")

    def process_order_update(self, order_dict: dict):
        """Processes order dictionary."""
        self.update_active_orders(order_dict)
        symbol = order_dict.get('vt_symbol')
        if symbol:
             self.check_risk(vt_symbol=symbol, trigger_event="ORDER_UPDATE")
        else:
             logger.warning("Order update received without vt_symbol, skipping symbol-specific risk check.")

    def process_trade_update(self, trade_dict: dict):
        """Processes trade dictionary."""
        # --- Check for duplicate trade --- 
        vt_tradeid = trade_dict.get('vt_tradeid')
        if vt_tradeid:
            if vt_tradeid in self.processed_trade_ids:
                logger.debug(f"Ignoring duplicate trade event: {vt_tradeid}")
                return # Skip processing duplicate
            else:
                self.processed_trade_ids.append(vt_tradeid)
        else:
            logger.warning("Trade update received without vt_tradeid, cannot check for duplicates.")
            # Decide whether to process or skip trades without ID? Process for now.
        # --- End Check for duplicate --- 

        symbol, updated_pos = self.update_position(trade_dict) # Pass dict
        # Only check risk if update_position was successful (returned symbol)
        if symbol:
            self.check_risk(vt_symbol=symbol, trigger_event="TRADE", current_position=updated_pos)

    def process_account_update(self, account_dict: dict):
        """Processes account dictionary."""
        # Update internal state first for risk checks
        self.account_data = account_dict
        self.check_risk(trigger_event="ACCOUNT_UPDATE") # Perform risk check

        # Log periodically
        current_time = time.time()
        accountid = account_dict.get('accountid', 'N/A')
        
        # Get string values and convert to float for logging/comparison
        try:
            balance_str = account_dict.get('balance', '0.0')
            available_str = account_dict.get('available', '0.0')
            margin_str = account_dict.get('margin', '0.0')
            frozen_str = account_dict.get('frozen', '0.0')
            
            balance = float(balance_str if balance_str is not None else '0.0')
            available = float(available_str if available_str is not None else '0.0')
            margin = float(margin_str if margin_str is not None else '0.0')
            frozen = float(frozen_str if frozen_str is not None else '0.0')

            # Using floats for key info tuple now for easier comparison/logging
            current_key_info_float = (balance, available, margin, frozen)

            # --- Simplified Logging Logic: Check Time First --- 
            if current_time - self.last_account_log_time >= 300: # Log every 5 minutes
                # Log using float formatting
                logger.info(f"账户资金更新 (每5分钟): ID={accountid}, Avail={available:.2f}, Margin={margin:.2f}, Frozen={frozen:.2f}")
                self.last_account_log_time = current_time # Update log time
                # Optionally update last logged info if change detection is needed later
                # self.last_logged_account_key_info = current_key_info_float 
            # --- End Simplified Logging Logic --- 

        except (ValueError, TypeError) as e:
             logger.error(f"处理账户更新日志时无法转换数值: {e}. Account Dict: {account_dict}")

    def process_log(self, log_dict: dict):
        """Processes log dictionary."""
        # Access fields using .get()
        gateway_name = log_dict.get('gateway_name', 'UnknownGW')
        msg = log_dict.get('msg', '')
        # Optionally get level: level = log_dict.get('level', 'INFO')
        logger.debug(f"[RM GW LOG - {gateway_name}] {msg}") # Log as debug for now

    def process_contract(self, contract_dict: dict):
        """Processes contract dictionary."""
        # Access vt_symbol using .get()
        vt_symbol = contract_dict.get('vt_symbol')
        if vt_symbol:
            logger.debug(f"Received contract data dict for {vt_symbol}")
            # Potentially update a local contract cache if needed
        else:
            # Keep warning if vt_symbol is missing, might indicate upstream issue
            logger.warning(f"Received contract data dict without vt_symbol: {contract_dict}")

