import zmq
import msgpack
import time
import sys
import os
from datetime import datetime, time as dt_time
import threading
from collections import defaultdict

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import new config location
from config import zmq_config as config
from logger import getLogger

# Import necessary vnpy constants
try:
    from vnpy.trader.constant import Direction, Status, Exchange, Offset, OrderType
except ImportError:
    print("无法导入 vnpy.trader.constant.Direction，请确保 vnpy 已安装。")
    # Define fallback constants if vnpy is not fully available in this environment
    class Direction:
        LONG = "多"
        SHORT = "空"

# VNPY imports (potentially needed for position/trade data structures)
from vnpy.trader.object import PositionData, TradeData, OrderData, AccountData # Import as needed

# Constants for Heartbeat
MARKET_DATA_TIMEOUT = 30.0 # seconds - Timeout for considering market data stale
PING_INTERVAL = 5.0  # seconds
PING_TIMEOUT_MS = 2500 # milliseconds

# --- Risk Manager Service ---
class RiskManagerService:
    def __init__(self, market_data_url: str, order_report_url: str, position_limits: dict):
        """Initializes the risk manager service."""
        self.logger = getLogger(__name__) # Get logger for this instance
        # Store config/parameters
        self.market_data_url = market_data_url
        self.order_report_url = order_report_url
        self.position_limits = position_limits
        # Load additional risk parameters from config
        self.max_pending_per_contract = getattr(config, 'MAX_PENDING_ORDERS_PER_CONTRACT', 5) # Default 5
        self.global_max_pending = getattr(config, 'GLOBAL_MAX_PENDING_ORDERS', 20) # Default 20
        self.margin_limit_ratio = getattr(config, 'MARGIN_USAGE_LIMIT', 0.8) # Default 80%

        self.context = zmq.Context()
        self.command_socket = self.context.socket(zmq.REQ)
        self._command_connect_url = self._get_connect_url(config.ORDER_GATEWAY_COMMAND_URL)
        self._setup_command_socket() # Setup initial command socket

        # Subscriber socket
        self.subscriber = self.context.socket(zmq.SUB)

        # Connect to both publishers
        self.subscriber.connect(market_data_url)
        self.subscriber.connect(order_report_url)
        self.logger.info(f"数据订阅器连接到: {market_data_url}") # Generic name
        self.logger.info(f"数据订阅器连接到: {order_report_url}") # Generic name

        # Subscribe to TICKs (optional, for market risk later), TRADEs, and Order Status/Account Data
        tick_prefix = "TICK."
        trade_prefix = "TRADE."
        self.subscriber.subscribe(tick_prefix.encode('utf-8'))
        self.subscriber.subscribe(trade_prefix.encode('utf-8'))
        # Subscribe to Order Status and Account Data
        order_status_prefix = "ORDER_STATUS."
        account_prefix = "ACCOUNT_DATA."
        self.subscriber.subscribe(order_status_prefix.encode('utf-8'))
        self.subscriber.subscribe(account_prefix.encode('utf-8'))
        self.logger.info(f"订阅主题前缀: {tick_prefix}, {trade_prefix}, {order_status_prefix}, {account_prefix}")

        # --- State ---
        self.positions = {} # vt_symbol -> net position (int)
        self.account_data: AccountData | None = None # Store latest account data
        self.active_orders: Dict[str, OrderData] = {} # vt_orderid -> OrderData object
        self.last_order_status: Dict[str, Status] = {} # vt_orderid -> last logged status
        self.logger.info(f"加载持仓限制: {self.position_limits}")

        # Market Data Status
        self.last_tick_time: Dict[str, float] = {} # vt_symbol -> last reception timestamp
        self.market_data_ok: bool = True
        # Assume we need ticks for symbols we might have limits for or defined in config
        # A more robust way might be to dynamically get subscribed symbols if possible
        self.subscribed_symbols: set = set(config.SUBSCRIBE_SYMBOLS) 
        self.logger.info(f"将监控以下合约行情超时: {self.subscribed_symbols}")

        # Gateway Connection Status
        self.gateway_connected = True # Assume connected initially
        self.last_ping_time = time.time()

        self.running = False
        self.logger.info("风险管理器初始化完成。")

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
            self.logger.info("尝试关闭旧的指令 Socket...")
            try:
                self.command_socket.close(linger=0)
            except Exception as e_close:
                 self.logger.warning(f"关闭旧指令 Socket 时出错: {e_close}")
        
        self.logger.info(f"正在创建并连接指令 Socket 到: {self._command_connect_url}")
        self.command_socket = self.context.socket(zmq.REQ)
        self.command_socket.setsockopt(zmq.LINGER, 0)
        try:
             self.command_socket.connect(self._command_connect_url)
             # Connection might not happen immediately, ping will verify
        except Exception as e_conn:
             self.logger.error(f"连接指令 Socket 时出错: {e_conn}")
             # Mark as disconnected immediately if connection fails
             if self.gateway_connected:
                 self.logger.error("与订单执行网关的连接丢失 (Connection Error)! ")
                 self.gateway_connected = False

    def _is_trading_hours(self) -> bool:
        """Checks if the current time is within any defined trading session."""
        # TODO: This assumes sessions are for the current day and doesn't handle cross-day sessions well (e.g., night to next morning)
        # TODO: Consider timezone awareness if server/client timezones differ significantly.
        # TODO: Specific contracts might have slightly different hours.
        now_dt = datetime.now()
        current_time = now_dt.time()
        # Optional: Consider day of week check if needed
        # if now_dt.weekday() >= 5: # Skip weekends
        #     return False 

        try:
            for start_str, end_str in config.FUTURES_TRADING_SESSIONS:
                start_time = dt_time.fromisoformat(start_str) # HH:MM
                end_time = dt_time.fromisoformat(end_str)
                
                # Simple case: session within the same day (e.g., 09:00-11:00)
                if start_time <= end_time:
                    if start_time <= current_time < end_time:
                        return True
                # Case: session crosses midnight (e.g., 21:00-02:30) - simplified handling
                # This simple check might not be robust enough for all cross-midnight scenarios
                # For 21:00-23:00, the above simple case works.
                # If a session was 21:00-01:00, we'd need: current_time >= start_time or current_time < end_time
                # else: # Assuming end_time < start_time means overnight
                #     if current_time >= start_time or current_time < end_time:
                #          return True
        except Exception as e:
            self.logger.error(f"检查交易时间时出错: {e}")
            return True # Fail open: assume trading hours if config is wrong
        
        return False # Not in any session

    def _process_message(self, topic_bytes: bytes, message: dict):
        """Processes a received message (Tick or Trade)."""
        try:
            topic_str = topic_bytes.decode('utf-8', errors='ignore')
            msg_type = message.get('type', 'UNKNOWN')
            msg_data = message.get('data', {})
            timestamp_ns = message.get('timestamp', 0)
            timestamp_sec = timestamp_ns / 1_000_000_000
            dt_object = datetime.fromtimestamp(timestamp_sec)
            pretty_time = dt_object.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

            if msg_type == "TRADE":
                self.logger.info(f"[{pretty_time}] 收到成交回报: {msg_data.get('vt_symbol')}")
                symbol, updated_pos = self.update_position(msg_data)
                self.check_risk(vt_symbol=msg_data.get('vt_symbol'), trigger_event="TRADE")

            elif msg_type == "TICK":
                # Optional: Process ticks for market risk checks later
                # self.logger.debug(f"[{pretty_time}] 收到行情: {msg_data.get('vt_symbol')}") # Use debug level
                pass # Placeholder for market data risk checks
            
            elif msg_type == "ACCOUNT_DATA":
                # --- Reconstruct AccountData carefully --- 
                try:
                    gateway_name = msg_data.get("gateway_name", "UnknownGW")
                    accountid = msg_data.get("accountid")

                    if not accountid:
                        self.logger.error(f"重建 AccountData 时缺少关键字段: accountid。 Data Keys: {list(msg_data.keys())}")
                        return

                    # Create AccountData instance (assuming gateway_name and accountid are sufficient for init)
                    # If init requires more/different fields, this needs adjustment based on vnpy definition.
                    account = AccountData(gateway_name=gateway_name, accountid=accountid)

                    # Populate other fields
                    for key, value in msg_data.items():
                        if key in ["gateway_name", "accountid"]:
                            continue
                        if hasattr(account, key):
                             try:
                                 # Attempt type conversion for known numeric fields if they are strings
                                 # This might happen depending on serialization
                                 if key in ['balance', 'available', 'commission', 'margin', 'frozen'] and isinstance(value, str):
                                     try:
                                         value = float(value)
                                     except ValueError:
                                         self.logger.warning(f"无法将账户字段 '{key}' 的值 '{value}' 转换为 float。")
                                         continue # Skip setting if conversion fails
                                 
                                 setattr(account, key, value)
                             except Exception as set_e:
                                 self.logger.error(f"设置属性 {key}={value} on AccountData 时出错: {set_e}")
                        # else:
                        #     self.logger.debug(f"重建 AccountData 时跳过未知键：{key}")

                    # --- Compare key fields to determine significant change --- 
                    has_key_fields_changed = False
                    if self.account_data is None:
                        has_key_fields_changed = True
                    else:
                        # Compare margin, frozen, commission - adjust fields as needed
                        if (getattr(account, 'margin', None) != getattr(self.account_data, 'margin', None) or
                            getattr(account, 'frozen', None) != getattr(self.account_data, 'frozen', None) or
                            getattr(account, 'commission', None) != getattr(self.account_data, 'commission', None)):
                             has_key_fields_changed = True
                    # --- End key field comparison --- 
                     
                    # Only update, log, and check risk if key fields have changed
                    if has_key_fields_changed:
                        self.account_data = account # Update stored data
                        self.logger.info(f"[{pretty_time}] 账户关键信息更新: AccountID={account.accountid}, Balance={account.balance:.2f}, Available={account.available:.2f}, Margin={getattr(account, 'margin', 0.0):.2f}, Frozen={getattr(account, 'frozen', 0.0):.2f}")
                        self.check_risk(trigger_event="ACCOUNT_UPDATE")

                except KeyError as e:
                    self.logger.error(f"重建 AccountData 时缺少关键字段: {e}。 Data Keys: {list(msg_data.keys())}")
                except Exception as e_rec:
                    self.logger.exception(f"重建 AccountData 对象时发生未知错误。")
                    self.logger.error(f"原始数据: {msg_data}")

            elif msg_type == "ORDER_STATUS":
                # --- Reconstruct OrderData carefully --- 
                try:
                    # Extract known fields and handle Enum conversions
                    gateway_name = msg_data.get("gateway_name", "UnknownGW")
                    symbol = msg_data.get("symbol")
                    exchange_str = msg_data.get("exchange")
                    orderid = msg_data.get("orderid")
                    direction_str = msg_data.get("direction")
                    offset_str = msg_data.get("offset")
                    order_type_str = msg_data.get("type") 
                    status_str = msg_data.get("status")

                    # Basic validation
                    if not all([gateway_name, symbol, exchange_str, orderid, direction_str, offset_str, order_type_str, status_str]):
                        self.logger.error(f"重建 OrderData 时缺少关键字段。 Data Keys: {list(msg_data.keys())}")
                        return # Skip processing this message

                    # Convert Enums
                    exchange = Exchange(exchange_str)
                    direction = Direction(direction_str)
                    offset = Offset(offset_str)
                    order_type = OrderType(order_type_str)
                    status = Status(status_str)

                    # Create a default OrderData or use known mandatory fields
                    # It's often safer to create an empty instance then populate
                    # However, OrderData might require some args. Let's use known ones.
                    order = OrderData(
                        gateway_name=gateway_name,
                        symbol=symbol,
                        exchange=exchange,
                        orderid=orderid,
                        direction=direction,
                        offset=offset,
                        type=order_type,
                        price=msg_data.get('price', 0.0), # Get price or default
                        volume=msg_data.get('volume', 0.0), # Get volume or default
                        status=status # Use converted status
                    )

                    # Populate remaining fields from the dictionary
                    for key, value in msg_data.items():
                        # Skip fields already handled or problematic ones like vt_symbol
                        if key in ["gateway_name", "symbol", "exchange", "orderid", "direction", "offset", "type", "status", "price", "volume", "vt_symbol"]:
                             continue
                        
                        # Handle datetime conversion 
                        if key == "datetime" and value and isinstance(value, str):
                            try:
                                value = datetime.fromisoformat(value)
                            except (ValueError, TypeError):
                                self.logger.warning(f"无法将订单日期时间 '{value}' 转换为 datetime 对象。")
                                value = None # Or keep as string?

                        # Set attribute if it exists on the object
                        if hasattr(order, key):
                            try:
                                setattr(order, key, value)
                            except Exception as set_e:
                                self.logger.error(f"设置属性 {key}={value} on OrderData 时出错: {set_e}")
                        # else: # Optional: log unknown keys
                        #     self.logger.debug(f"重建 OrderData 时跳过未知键：{key}")

                    # --- Log only if status has changed --- 
                    last_status = self.last_order_status.get(order.vt_orderid)
                    if order.status != last_status:
                        self.last_order_status[order.vt_orderid] = order.status # Update last known status
                        self.logger.info(f"[{pretty_time}] 订单状态更新: ID={order.vt_orderid}, Status={order.status.value}, Traded={order.traded}")
                    # else: 
                    #     self.logger.debug(f"[{pretty_time}] 收到订单回报 (状态未变): ID={order.vt_orderid}, Status={order.status.value}")

                    self.update_active_orders(order)
                    self.check_risk(vt_symbol=order.vt_symbol, trigger_event="ORDER_UPDATE")

                except KeyError as e:
                    self.logger.error(f"重建 OrderData 时缺少关键字段: {e}。 Data Keys: {list(msg_data.keys())}")
                except ValueError as e: # Handle Enum conversion errors
                    self.logger.error(f"重建 OrderData 时枚举转换错误: {e}。 Data: {msg_data}")
                except Exception as e_rec:
                    self.logger.exception(f"重建 OrderData 对象时发生未知错误。")
                    self.logger.error(f"原始数据: {msg_data}")

        except Exception as e:
            self.logger.exception(f"处理消息时出错 (Topic: {topic_str})")
            # Log only essential parts to avoid large log entries
            self.logger.error(f"出错消息内容 (部分): {{'type': msg_type, 'data_keys': list(msg_data.keys())}}")

    def update_position(self, trade_data: dict):
        """Updates position based on trade data."""
        vt_symbol = trade_data.get('vt_symbol')
        direction = trade_data.get('direction')
        volume = trade_data.get('volume') # Should be positive
        offset = trade_data.get('offset') # Useful for more precise position logic (e.g., handling close today)

        if not all([vt_symbol, direction, volume]):
            self.logger.error("错误：成交回报缺少关键字段 (vt_symbol, direction, volume)")
            return None, None

        # Calculate position change
        pos_change = 0
        if direction == Direction.LONG.value:
            pos_change = volume
        elif direction == Direction.SHORT.value:
            pos_change = -volume
        else:
            self.logger.error(f"错误：未知的成交方向 '{direction}'")
            return None, None

        # Update position map
        current_pos = self.positions.get(vt_symbol, 0)
        new_pos = current_pos + pos_change
        self.positions[vt_symbol] = new_pos
        self.logger.info(f"持仓更新: {vt_symbol} | 旧: {current_pos} | 变动: {pos_change} | 新: {new_pos}")

        return vt_symbol, new_pos

    def update_active_orders(self, order: OrderData):
        """Updates the dictionary of active orders."""
        if order.is_active():
            self.active_orders[order.vt_orderid] = order
            # self.logger.debug(f"Active order added/updated: {order.vt_orderid} Status: {order.status.value}")
        elif order.vt_orderid in self.active_orders:
            del self.active_orders[order.vt_orderid]
            # self.logger.debug(f"Inactive order removed: {order.vt_orderid} Status: {order.status.value}")

    def check_risk(self, vt_symbol: str = None, trigger_event: str = "UNKNOWN", current_position: int = None):
        """Checks various risk limits based on current state. Logs market data status."""

        # Log market data status at the beginning of check
        if not self.market_data_ok:
            self.logger.warning("[Risk Check] 行情数据流异常，部分依赖市价的检查可能不准确或已跳过。")
        # else:
        #     self.logger.debug("[Risk Check] Market data stream OK.") # Optional debug log

        # 1. Position Limit Check (only if vt_symbol is provided)
        if vt_symbol:
            position = self.positions.get(vt_symbol, 0)
            limit = self.position_limits.get(vt_symbol)
            if limit is not None and abs(position) > limit:
                self.logger.warning(f"[风险告警] 合约 {vt_symbol}: 持仓 {position} 超出限制 {limit}!")
                # Action: Could try to send closing orders, but needs careful logic

        # 2. Pending Order Limit Check
        # Count pending orders globally and per contract
        global_pending_count = len(self.active_orders)
        pending_per_contract = defaultdict(int)
        orders_to_potentially_cancel = defaultdict(list)
        for order in self.active_orders.values():
             if order.is_active(): # Double check, though active_orders should only contain active ones
                 pending_per_contract[order.vt_symbol] += 1
                 orders_to_potentially_cancel[order.vt_symbol].append(order)

        # Check global limit
        if global_pending_count > self.global_max_pending:
            self.logger.warning(f"[风险告警] 全局活动订单数 {global_pending_count} 超出限制 {self.global_max_pending}!")
            # Action: Find the oldest pending order globally and try to cancel it
            oldest_order = min(self.active_orders.values(), key=lambda o: o.datetime, default=None)
            if oldest_order:
                 self.logger.warning(f"  尝试撤销最旧的全局挂单: {oldest_order.vt_orderid}")
                 self._send_cancel_request(oldest_order.vt_orderid)

        # Check per-contract limit (only if vt_symbol triggered the check or for all)
        # If vt_symbol is None (e.g., account update trigger), check all contracts
        symbols_to_check = [vt_symbol] if vt_symbol else list(pending_per_contract.keys())
        for symbol in symbols_to_check:
             if symbol is None: continue
             count = pending_per_contract.get(symbol, 0)
             limit_per = self.max_pending_per_contract
             if count > limit_per:
                 self.logger.warning(f"[风险告警] 合约 {symbol}: 活动订单数 {count} 超出限制 {limit_per}!")
                 # Action: Cancel the oldest active order for this specific symbol
                 symbol_orders = sorted(orders_to_potentially_cancel.get(symbol, []), key=lambda o: o.datetime)
                 if symbol_orders:
                     order_to_cancel = symbol_orders[0]
                     self.logger.warning(f"  尝试撤销合约 {symbol} 最旧的挂单: {order_to_cancel.vt_orderid}")
                     self._send_cancel_request(order_to_cancel.vt_orderid)

        # 3. Margin Usage Check (requires account_data)
        if self.account_data:
             # Simplified check: available < (1 - limit_ratio) * balance
             # More accurate check needs margin calculation based on positions/orders
             required_margin = self.account_data.balance - self.account_data.available
             margin_ratio = required_margin / self.account_data.balance if self.account_data.balance > 0 else 0
             if margin_ratio > self.margin_limit_ratio:
                  self.logger.warning(f"[风险告警] 保证金占用率 {margin_ratio:.2%} 超出限制 {self.margin_limit_ratio:.2%}!")
                  # Action: Could cancel orders or liquidate positions (complex)

    def _send_cancel_request(self, vt_orderid: str):
        """Sends a cancel order request to the Order Execution Gateway."""
        if not vt_orderid:
            self.logger.error("尝试发送空 vt_orderid 的撤单请求。")
            return # Don't proceed if no order ID

        # Check gateway connection status before sending
        if not self.gateway_connected:
            self.logger.error(f"无法发送撤单指令 ({vt_orderid})：与订单执行网关失去连接。")
            return

        self.logger.info(f"发送撤单指令给网关: {vt_orderid}")
        command_msg = {
            "type": "CANCEL_ORDER",
            "data": {"vt_orderid": vt_orderid}
        }

        try:
            packed_request = msgpack.packb(command_msg, use_bin_type=True)
            self.command_socket.send(packed_request)

            # Wait for the reply with a timeout
            # Use poll for non-blocking wait with timeout
            poller = zmq.Poller()
            poller.register(self.command_socket, zmq.POLLIN)
            timeout_ms = 5000 # 5 seconds timeout
            events = dict(poller.poll(timeout_ms))

            if self.command_socket in events:
                packed_reply = self.command_socket.recv()
                reply = msgpack.unpackb(packed_reply, raw=False)
                self.logger.info(f"收到撤单指令回复 ({vt_orderid}): {reply}")
            else:
                self.logger.error(f"撤单指令 ({vt_orderid}) 请求超时 ({timeout_ms}ms)。")
                # Handle timeout: maybe reconnect or log error persistently
                # Recreating socket on timeout might be necessary
                # self.command_socket.close()
                # self.command_socket = self.context.socket(zmq.REQ)
                # self.command_socket.connect(...) 

        except zmq.ZMQError as e:
            self.logger.error(f"发送撤单指令 ({vt_orderid}) 时 ZMQ 错误: {e}")
            # Consider reconnecting or handling specific errors
        except Exception as e:
            self.logger.exception(f"发送或处理撤单指令 ({vt_orderid}) 回复时出错")

    def _send_ping(self):
        """Sends a PING request to the Order Gateway and handles the reply."""
        # Determine log prefix based on current assumed state
        log_prefix = "[Ping]" if self.gateway_connected else "[Ping - Attempting Reconnect]"
        self.logger.debug(f"{log_prefix} Sending...")

        ping_msg = {"type": "PING", "data": {}}
        current_time = time.time()
        self.last_ping_time = current_time # Update last ping attempt time

        try:
            # Use Poller for non-blocking send with timeout check
            poller = zmq.Poller()
            poller.register(self.command_socket, zmq.POLLOUT | zmq.POLLIN) # Check if writable and readable

            # Send PING
            packed_request = msgpack.packb(ping_msg, use_bin_type=True)
            self.command_socket.send(packed_request) # Use blocking send, rely on timeout/error for issues

            # Wait for PONG reply with timeout
            readable = dict(poller.poll(PING_TIMEOUT_MS))
            if self.command_socket in readable and readable[self.command_socket] & zmq.POLLIN:
                packed_reply = self.command_socket.recv(zmq.NOBLOCK)
                reply = msgpack.unpackb(packed_reply, raw=False)
                if reply.get("reply") == "PONG":
                    self.logger.debug("Received PONG successfully.")
                    if not self.gateway_connected:
                         self.logger.info("与订单执行网关的连接已恢复。")
                         self.gateway_connected = True # Mark as connected
                else:
                    self.logger.warning(f"Received unexpected reply to PING: {reply}")
                    if self.gateway_connected:
                         self.logger.error("与订单执行网关的连接可能存在问题 (Unexpected PING reply)! ")
                         self.gateway_connected = False
            else:
                # Timeout waiting for reply
                self.logger.warning(f"{log_prefix} PING request timed out after {PING_TIMEOUT_MS}ms.")
                # Mark as disconnected (if not already) and trigger reconnection
                if self.gateway_connected: # Log error only on first detection
                    self.logger.error(f"与订单执行网关的连接丢失 (PING timeout)!")
                self.gateway_connected = False
                self.logger.info("尝试重置指令 Socket (因 PING 超时)...")
                self._setup_command_socket()
                return # Exit after attempting reconnect

        except zmq.ZMQError as e:
            # Handle errors during send or recv
            self.logger.error(f"{log_prefix} 发送 PING 或接收 PONG 时 ZMQ 错误: {e}")
            # Mark as disconnected (if not already) and trigger reconnection
            if self.gateway_connected: # Log error only on first detection
                self.logger.error(f"与订单执行网关的连接丢失 ({e})! ")
            self.gateway_connected = False
            self.logger.info("尝试重置指令 Socket (因 ZMQ 错误)...")
            self._setup_command_socket()
            return # Exit after attempting reconnect

        except Exception as e:
            # Handle other unexpected errors
            self.logger.exception(f"{log_prefix} 发送或处理 PING/PONG 时发生未知错误")
            # Mark as disconnected (if not already)
            if self.gateway_connected: # Log error only on first detection
                self.logger.error("与订单执行网关的连接丢失 (Unknown Error)! ")
                self.gateway_connected = False
            # Optional: Trigger reconnection on unknown errors too?

    def start(self):
        """Starts listening for messages and performing risk checks."""
        if self.running:
            self.logger.warning("风险管理器已在运行中。")
            return

        self.logger.info("启动风险管理器服务...")
        self.running = True        
        self.logger.info("风险管理器服务已启动，开始监听消息...")
        
        # --- Main Loop --- 
        while self.running:
            # Use Poller for non-blocking receive on subscriber
            poller = zmq.Poller()
            poller.register(self.subscriber, zmq.POLLIN)
            # Poll with a timeout (e.g., 1 second) to allow for other checks
            poll_timeout_ms = 1000 
            socks = dict(poller.poll(poll_timeout_ms)) 

            try:
                # Process incoming subscription messages if any
                if self.subscriber in socks and socks[self.subscriber] == zmq.POLLIN:
                    topic, packed_message = self.subscriber.recv_multipart(zmq.NOBLOCK)

                    # Check running flag *after* potentially blocking recv returns
                    if not self.running:
                        break 

                    try:
                        message = msgpack.unpackb(packed_message, raw=False)
                        self._process_message(topic, message)
                    except msgpack.UnpackException as e:
                        self.logger.error(f"Msgpack 解码错误: {e}. Topic: {topic.decode('utf-8', errors='ignore')}")

                # --- Periodic Heartbeat Check --- 
                current_time = time.time()
                if current_time - self.last_ping_time >= PING_INTERVAL:
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
                            if check_time - self.last_ping_time > PING_INTERVAL * 2: # Allow some time after start
                                found_stale_symbol = True
                                stale_symbols.append(f"{symbol} (no tick received)")
                        elif check_time - last_ts > MARKET_DATA_TIMEOUT:
                            found_stale_symbol = True
                            stale_symbols.append(f"{symbol} (last {check_time - last_ts:.1f}s ago)")

                    if found_stale_symbol:
                        if self.market_data_ok: # Log only when status changes to False
                            self.logger.warning(f"[交易时段内] 行情数据可能中断或延迟! 超时合约: {', '.join(stale_symbols)}")
                            self.market_data_ok = False
                    elif not self.market_data_ok:
                        # If no symbols are stale now, but status was False, means it recovered
                        self.logger.info("所有监控合约的行情数据流已恢复。")
                        self.market_data_ok = True
                # --- End Market Data Timeout Check --- 

            except zmq.ZMQError as e:
                # Check if the error occurred because we are stopping
                if not self.running or e.errno == zmq.ETERM:
                    self.logger.info(f"ZMQ 错误 ({e.errno}) 发生在服务停止期间或Context终止，正常退出循环。")
                    break # Exit loop cleanly
                else:
                    self.logger.error(f"意外的 ZMQ 错误: {e}")
                    self.running = False # Stop on other ZMQ errors
            except KeyboardInterrupt:
                self.logger.info("检测到中断信号，开始停止...")
                self.running = False
            except Exception as e:
                self.logger.exception("主循环处理消息时发生未知错误")
                # Avoid rapid looping on persistent errors
                time.sleep(1) 

        self.logger.info("风险管理器主循环结束。")
        # Cleanup (closing sockets/context) is handled in stop()

    def stop(self):
        """Stops the service and cleans up resources."""
        if not self.running:
            self.logger.warning("风险管理器未运行。")
            return

        # Prevent starting new processing if stop is called concurrently
        if not self.running:
            return 

        self.logger.info("正在停止风险管理器服务...")
        self.running = False

        # Close sockets and context
        self.logger.info("关闭 ZMQ sockets 和 context...")
        if self.subscriber:
            self.subscriber.close()
            self.logger.info("ZeroMQ 订阅器已关闭。")
        if self.command_socket:
            self.command_socket.close()
            self.logger.info("ZeroMQ 指令发送器已关闭。")
        if self.context:
            try:
                self.context.term()
                self.logger.info("ZeroMQ Context 已终止。")
            except zmq.ZMQError as e:
                 self.logger.error(f"终止 ZeroMQ Context 时出错 (可能已终止): {e}")
        self.logger.info("风险管理器已停止。")

# --- Main execution block (for testing) ---
if __name__ == "__main__":
    # Connect to localhost publishers
    md_url = config.MARKET_DATA_PUB_URL.replace("*", "localhost")
    report_url = config.ORDER_REPORT_PUB_URL.replace("*", "localhost")
    limits = config.MAX_POSITION_LIMITS

    risk_manager = RiskManagerService(md_url, report_url, limits)

    try:
        risk_manager.start()
    except KeyboardInterrupt:
        print("\n主程序接收到中断信号。")
    finally:
        if risk_manager.running:
            risk_manager.stop()
        print("风险管理器测试运行结束。")
