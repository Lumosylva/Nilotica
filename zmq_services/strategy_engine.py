from typing import Dict, Any, Set, Optional, List
from collections import deque
import zmq
import time
import sys
import os
import logging
import msgpack
from utils.logger import logger, setup_logging, DEBUG, INFO
from utils.i18n import get_translator
import importlib

from utils.config_manager import ConfigManager
from zmq_services.strategy_base import BaseLiveStrategy

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from vnpy.trader.constant import Direction, OrderType, Exchange, Offset, Status
from vnpy.trader.object import TickData, OrderData, TradeData, AccountData, LogData

from vnpy.trader.utility import extract_vt_symbol

from datetime import datetime, time as dt_time # For trading hours check

class StrategyEngine: # Renamed class
    def __init__(self,
                 config_manager: ConfigManager,
                 gateway_pub_url: str,
                 order_gw_rep_url: str,
                 order_report_url: str,
                 strategies_config: Dict[str, Dict[str, Any]],
                 strategy_context_name: Optional[str] = None,
                 log_level_for_strat_engine: str = "INFO"
                 ):
        """
        初始化策略引擎，加载策略并建立通信。

        参数：
            config_manager：用于加载配置的 ConfigManager 实例。
            gateway_pub_url：用于市场数据的 ZMQ PUB URL。
            order_gw_rep_url：用于发送订单请求/RPC（至订单网关 REP）的 ZMQ REQ URL。
            order_report_url：用于接收订单/交易报告的 ZMQ PUB URL。
            strategies_config：用于加载策略的配置。
            strategy_context_name：用于记录上下文的主要策略名称（可选）。
            log_level_for_strat_engine：此 StrategyEngine 实例的日志级别。

        Initializes the strategy engine, loads strategies, and sets up communication.

        Args:
            config_manager: ConfigManager instance for loading configuration.
            gateway_pub_url: ZMQ PUB URL for market data.
            order_gw_rep_url: ZMQ REQ URL for sending order requests/RPC (to Order Gateway REP).
            order_report_url: ZMQ PUB URL for receiving order/trade reports.
            strategies_config: Configuration for strategies to load.
            strategy_context_name: Optional name of the primary strategy for logging context.
            log_level_for_strat_engine: Log level for this StrategyEngine instance.
        """
        self._ = get_translator()
        self.config_service = config_manager # Assign early for config_env access
        service_name_base = self.__class__.__name__
        final_service_name = f"{service_name_base}[{strategy_context_name}]" if strategy_context_name else service_name_base
        # Use the passed log_level and config_env from its own config_manager
        setup_logging(service_name=final_service_name, level=log_level_for_strat_engine,
                      config_env=self.config_service._environment)

        self.is_backtest_mode: bool = False # Default to False for live/normal mode

        self.context = zmq.Context()
        # self.config_service = config_manager # Moved up

        # +++ Load engine communication parameters from config_service +++
        self.market_data_timeout = self.config_service.get_global_config("engine_communication.market_data_timeout_seconds", 30.0)
        self.initial_tick_grace_period_multiplier = self.config_service.get_global_config("engine_communication.initial_tick_grace_period_multiplier", 2.0)
        self.ping_interval = self.config_service.get_global_config("engine_communication.ping_interval_seconds", 5.0)
        self.ping_timeout_ms = self.config_service.get_global_config("engine_communication.ping_timeout_ms", 2500)
        self.rpc_timeout_ms = self.config_service.get_global_config("engine_communication.rpc_timeout_ms", 3000)
        self.rpc_retries = self.config_service.get_global_config("engine_communication.rpc_retries", 1)
        self.heartbeat_timeout_s = self.config_service.get_global_config("engine_communication.heartbeat_timeout_seconds", 10.0)
        self.order_cool_down_seconds = self.config_service.get_global_config("order_submission.default_cool_down_seconds", 1)
        # For _is_trading_hours, we load it directly in the method to keep __init__ cleaner, or make it an attribute if frequently used.

        # --- ZMQ Socket Setup --- 
        self.subscriber = self.context.socket(zmq.SUB)
        self.subscriber.setsockopt(zmq.LINGER, 0)
        self.subscriber.connect(gateway_pub_url)
        self.subscriber.connect(order_report_url)
        logger.info(self._("策略引擎连接行情发布器: {}").format(gateway_pub_url))
        logger.info(self._("策略引擎连接回报发布器: {}").format(order_report_url))

        self.order_requester = self.context.socket(zmq.REQ)
        self.order_requester.setsockopt(zmq.LINGER, 0)
        self.order_requester.connect(order_gw_rep_url)
        logger.info(self._("策略引擎连接订单网关 REQ Socket (RPC): {}").format(order_gw_rep_url))
        self.order_pusher = None

        # --- Strategy Loading and Management ---
        self.strategies: Dict[str, BaseLiveStrategy] = {}
        self.symbol_strategy_map: Dict[str, List[BaseLiveStrategy]] = {} # Maps vt_symbol to a LIST of strategies
        self.subscribed_symbols: Set[str] = set() # Track all symbols needed by strategies
        self.class_path = None
        self.vt_symbol = None
        self.module_path = None
        self.class_name = None

        logger.info("开始加载策略...")
        for strategy_name, config_data in strategies_config.items():
            if strategy_name in self.strategies:
                 logger.error(self._("策略名称 '{}' 重复，跳过加载。").format(strategy_name))
                 continue
            try:
                self.class_path = config_data["strategy_class"]
                self.vt_symbol = config_data["vt_symbol"]
                setting = config_data.get("setting", {})

                # Validate vt_symbol format (simple check)
                if '.' not in self.vt_symbol:
                     logger.error(self._("策略 '{}' 的 vt_symbol '{}' 格式无效，应为 'symbol.exchange'。跳过加载。")
                                  .format(strategy_name, self.vt_symbol))
                     continue

                # Dynamically import the strategy class
                self.module_path, self.class_name = self.class_path.rsplit('.', 1)
                module = importlib.import_module(self.module_path)
                strategy_class = getattr(module, self.class_name)

                # Check inheritance
                if not issubclass(strategy_class, BaseLiveStrategy):
                     logger.error(self._("类 '{}' 不是 BaseLiveStrategy 的子类。跳过加载策略 '{}'。").format(self.class_path, strategy_name))
                     continue

                # Create strategy instance
                strategy_instance = strategy_class(
                    strategy_engine=self,       # Pass the engine instance itself
                    strategy_name=strategy_name,
                    vt_symbol=self.vt_symbol,
                    setting=setting
                )

                self.strategies[strategy_name] = strategy_instance
                # Map the specific vt_symbol to this strategy instance
                if self.vt_symbol not in self.symbol_strategy_map:
                    self.symbol_strategy_map[self.vt_symbol] = [] # Initialize list if first time
                self.symbol_strategy_map[self.vt_symbol].append(strategy_instance)
                self.subscribed_symbols.add(self.vt_symbol)

                logger.info(f"策略 '{strategy_name}' (类: {self.class_name}, 合约: {self.vt_symbol}) 加载成功。")

            except ImportError:
                logger.exception(f"导入策略类 '{self.class_path}' 失败。跳过加载策略 '{strategy_name}'。")
            except AttributeError:
                 logger.exception(f"在模块 '{self.module_path}' 中未找到策略类 '{self.class_name}'。跳过加载策略 '{strategy_name}'。")
            except KeyError as e:
                logger.error(f"策略 '{strategy_name}' 的配置缺少必要键: {e}。跳过加载。")
            except Exception as e:
                logger.exception(f"加载策略 '{strategy_name}' 时发生未知错误: {e}")
        logger.info(f"策略加载完成。共加载 {len(self.strategies)} 个策略实例，映射到 {len(self.symbol_strategy_map)} 个合约。")
        if not self.strategies:
             logger.error("错误：未成功加载任何策略。引擎无法运行。")
             # Consider exiting or handling this state appropriately
             # For now, allow continuing but log error.
        # --- End Strategy Loading ---

        # --- Original subscription logic (Restored) ---
        self.subscribe_topics = []
        self.topic_map = {
            "tick": "tick.", # <-- LOWERCASE
            "trade": "trade.", "order": "order.",
            "account": "account.", "contract": "contract.", "log": "log",
            "heartbeat_ordergw": "heartbeat.ordergw" # 添加心跳主题
        }
        # --- End FIX ---

        if not self.subscribed_symbols:
            logger.warning("没有加载任何策略或策略未指定合约，将不订阅任何特定合约数据！")
        else:
            for vt_symbol in self.subscribed_symbols:
                tick_topic = f"{self.topic_map['tick']}{vt_symbol}"
                contract_topic = f"{self.topic_map['contract']}{vt_symbol}"
                self.subscriber.subscribe(tick_topic.encode('utf-8'))
                self.subscriber.subscribe(contract_topic.encode('utf-8'))
                self.subscribe_topics.extend([tick_topic, contract_topic])
                logger.info(f"  订阅特定合约主题: {tick_topic}, {contract_topic}")

        # Subscribe to generic topics
        self.subscriber.subscribe(f"{self.topic_map['order']}".encode('utf-8'))
        self.subscriber.subscribe(f"{self.topic_map['trade']}".encode('utf-8'))
        self.subscriber.subscribe(f"{self.topic_map['account']}".encode('utf-8'))
        self.subscriber.subscribe(self.topic_map['log'].encode('utf-8'))
        self.subscriber.subscribe(self.topic_map['heartbeat_ordergw'].encode('utf-8')) # 订阅心跳
        self.subscribe_topics.extend([
            f"{self.topic_map['order']}", f"{self.topic_map['trade']}", f"{self.topic_map['account']}",
            self.topic_map['log'], self.topic_map['heartbeat_ordergw']
        ])
        logger.info(f"  订阅通用主题前缀: {self.topic_map['order']}*, {self.topic_map['trade']}*, {self.topic_map['account']}*, {self.topic_map['log']}, {self.topic_map['heartbeat_ordergw']}")

        logger.info(f"最终订阅 {len(self.subscribe_topics)} 个主题/前缀。")
        # +++ Log exact subscriptions +++
        # Logging the list is more reliable than getsockopt across versions
        logger.debug(f"Final SUB socket subscriptions list: {self.subscribe_topics}")
        # +++ End Log +++
        # --- End Original Subscription Logic ---

        # --- Health Status Flags & Timers (Mostly unchanged) ---
        self.market_data_ok = True
        self.gateway_connected = False # 初始状态设为 False，等待第一次心跳或 Ping 成功
        self.last_tick_time: Dict[str, float] = {} # Tracks last tick time per symbol
        self.last_ping_time = time.time()
        self.last_ordergw_heartbeat_time = 0.0 # 初始化上次心跳时间
        self.initial_connection_time: Optional[float] = None # +++ Time when connection first established +++
        # --- End Health Status ---

        self.running = False
        # Remove SA509 specific state - managed by strategy instances now
        self.last_order_time = {} # Cool down per symbol, keep at engine level
        # self.last_order_status = {} # Status tracking is now per-strategy + active_orders set
        self.trades = [] # Keep a global list of recent trades? Optional.

        # --- Add recently processed trade IDs --- 
        self.processed_trade_ids: deque[str] = deque(maxlen=1000) # Store last 1000 trade IDs
        # --- End Add ---

        # Initialize loaded strategies
        logger.info("正在初始化所有已加载的策略...")
        for strategy in self.strategies.values():
             strategy._init_strategy() # Call internal init method
        logger.info("所有策略初始化完成。")

        # Set a smaller high water mark for the subscriber socket
        # self.subscriber.set(zmq.RCVHWM, 5) # <-- REMOVED OLD VALUE
        # +++ Increase RCVHWM significantly +++
        new_rcvhwm = 5000 # Or 1000, or higher if needed
        try:
            self.subscriber.set(zmq.RCVHWM, new_rcvhwm)
            logger.info(f"为 SUB socket 设置了 ZMQ.RCVHWM: {new_rcvhwm}")
        except Exception as e:
             logger.error(f"为 SUB socket 设置 ZMQ.RCVHWM 时出错: {e}")
        # +++ End Increase +++

    # --- RPC Helper (Should only be called in Live mode) --- 
    def _send_rpc_request(self, request_tuple: tuple, timeout_ms: int = -1, retries: int = -1) -> Any | None:
        """
        Helper method to send an RPC request via REQ socket with timeout and retries.
        ONLY FOR LIVE TRADING.
        """
        # Use instance variables for default timeout and retries
        if timeout_ms == -1:
            timeout_ms = self.rpc_timeout_ms
        if retries == -1:
            retries = self.rpc_retries

        # +++ Add check for backtest mode +++
        if self.is_backtest_mode:
            logger.error("_send_rpc_request 不应在回测模式下调用！")
            return None
        # +++ End Add +++
        if not self.running:
             logger.warning("Engine not running, cannot send RPC request.")
             return None
        # Check if self.order_requester even exists or is correct type
        if not hasattr(self, 'order_requester') or not isinstance(self.order_requester, zmq.Socket) or self.order_requester.closed:
             logger.error("Order requester (REQ) socket is not available or closed for RPC.")
             self.gateway_connected = False # Mark as disconnected if socket issue
             return None

        method_name = request_tuple[0] if request_tuple else "Unknown"
        attempt = 0
        max_attempts = 1 + retries # Initial attempt + retries

        while attempt < max_attempts:
            attempt += 1
            log_prefix = f"[RPC-{method_name} Att.{attempt}/{max_attempts}]"
            try:
                # Use Poller for non-blocking send/recv with timeout check
                poller = zmq.Poller()
                poller.register(self.order_requester, zmq.POLLIN)

                # Serialize and send request
                packed_request = msgpack.packb(request_tuple, use_bin_type=True)
                logger.debug(f"{log_prefix} Sending request...")
                self.order_requester.send(packed_request)

                # Wait for reply with timeout
                readable = dict(poller.poll(timeout_ms))
                if self.order_requester in readable and readable[self.order_requester] & zmq.POLLIN:
                    packed_reply = self.order_requester.recv()
                    reply = msgpack.unpackb(packed_reply, raw=False)

                    # Check RpcServer reply format [True/False, result/traceback]
                    if isinstance(reply, (list, tuple)) and len(reply) == 2:
                        if reply[0] is True: # Success
                            logger.debug(f"{log_prefix} Received successful reply.")
                            # Mark gateway as connected on successful PING reply
                            if not self.gateway_connected and method_name == "ping":
                                 logger.info("与订单执行网关的连接已恢复 (RPC Ping). 设置初始连接时间。")
                                 self.gateway_connected = True
                                 if self.initial_connection_time is None:
                                     self.initial_connection_time = time.time()
                            return reply[1] # Return the actual result
                        else: # Application-level error reported by server
                            error_msg = reply[1]
                            logger.error(f"{log_prefix} RPC 调用失败 (网关回复): {error_msg}")
                            return None # Indicate failure
                    else:
                        logger.error(f"{log_prefix} 收到来自订单网关的意外回复格式: {reply}")
                        return None # Indicate failure (format error)
                else:
                    # Timeout waiting for reply
                    logger.warning(f"{log_prefix} 请求超时 ({timeout_ms}ms).")
                    if attempt >= max_attempts:
                         logger.error(f"与订单执行网关的连接丢失 (RPC Timeout after {max_attempts} attempts)!")
                         self.gateway_connected = False
                         return None # Indicate failure after all retries
                    # Continue to next retry attempt

            except zmq.ZMQError as e:
                logger.error(f"{log_prefix} 发送或接收 RPC 时 ZMQ 错误: {e}")
                # Assume connection is lost on ZMQ errors
                if self.gateway_connected:
                     logger.error(f"与订单执行网关的连接丢失 (ZMQ Error {e.errno})!")
                self.gateway_connected = False
                return None # Indicate failure
            except (msgpack.UnpackException, TypeError, ValueError) as e_unpack:
                 logger.exception(f"{log_prefix} Msgpack反序列化 RPC 回复时出错: {e_unpack}")
                 return None # Indicate failure
            except Exception as e:
                logger.exception(f"{log_prefix} 处理 RPC 时发生未知错误：{e}")
                # Assume connection might be compromised
                if self.gateway_connected:
                    logger.error("与订单执行网关的连接可能丢失 (未知 RPC 错误)!")
                self.gateway_connected = False
                return None # Indicate failure

        # Should not be reached if loop logic is correct, but as a safeguard
        return None

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
        from datetime import datetime, time as dt_time # Import here
        now_dt = datetime.now()
        current_time = now_dt.time()
        try:
            # +++ Get trading sessions from ConfigManager +++
            futures_trading_sessions = self.config_service.get_global_config("risk_management.futures_trading_sessions", [])
            for start_str, end_str in futures_trading_sessions:
                start_time = dt_time.fromisoformat(start_str) 
                end_time = dt_time.fromisoformat(end_str)
                if start_time <= end_time: # Simple case: session within the same day
                    if start_time <= current_time < end_time:
                        return True
                # else: # Handle overnight sessions if needed (add complex logic here if required)
                #     if current_time >= start_time or current_time < end_time:
                #          return True
        except Exception as e:
            logger.error(f"(Strategy) 检查交易时间时出错: {e}")
            return True # Fail open: assume trading hours if config is wrong
        return False # Not in any session
    # --- End add missing method ---

    # --- Update _send_ping to use order_requester and pickle/RPC format ---
    def _send_ping(self):
        """Sends a PING request to the Order Gateway via RPC to check/restore connectivity."""
        log_prefix = "[Ping OrderGW]"
        # logger.debug(f"{log_prefix} Sending...") # Reduce noise, debug log inside helper

        # Format according to vnpy.rpc: ("ping", (), {})
        req_tuple = ("ping", (), {})
        current_time = time.time()
        self.last_ping_time = current_time # Update last ping attempt time

        # Use the helper method with specific ping timeout and no retries for ping itself
        result = self._send_rpc_request(req_tuple, timeout_ms=self.ping_timeout_ms, retries=0) # Use instance variable

        if result == "pong":
            # Success is handled inside _send_rpc_request by setting gateway_connected=True
            # logger.debug("Received PONG successfully from OrderGW.")
             pass # No further action needed here
        elif result is None:
             # Failure (timeout or error) is handled inside _send_rpc_request
             # by setting gateway_connected=False and logging errors.
             pass # No further action needed here
        else:
            # Unexpected result content (should be caught by _send_rpc_request format check ideally)
             logger.warning(f"{log_prefix} Received unexpected result for PING: {result}")
             if self.gateway_connected:
                 logger.error("与订单执行网关的连接可能存在问题 (Unexpected PING result)! ")
                 self.gateway_connected = False

    # --- End update _send_ping ---

    # --- Trading Interface Methods (send_limit_order/cancel_limit_order for LIVE MODE ONLY) --- 
    # These methods in StrategyEngine are now PRIMARILY for LIVE trading RPC calls.
    # Backtesting order sending is handled directly in BaseLiveStrategy sending to self.order_pusher.
    def send_limit_order(self, symbol: str, exchange: Exchange, direction: Direction, price: float, volume: float, offset: Offset = Offset.NONE) -> Optional[str]:
        """(LIVE MODE ONLY) Sends a limit order request via RPC."""
        if self.is_backtest_mode:
            logger.error("StrategyEngine.send_limit_order should not be called directly in backtest mode.")
            return None
        # --- Connection Check ---
        if not self.gateway_connected:
            logger.error(f"无法发送订单 ({symbol} {direction.value} {volume} @ {price}): 与订单执行网关失去连接。")
            return None # Indicate failure

        # Apply cooling down period (logic remains the same)
        # +++ Use instance variable for cool_down_period +++
        last_sent = self.last_order_time.get(symbol, 0)
        if time.time() - last_sent < self.order_cool_down_seconds: # Use instance variable
            logger.warning(f"订单发送冷却中 ({symbol}): 距离上次发送不足 {self.order_cool_down_seconds} 秒。")
            return None

        logger.info(f"准备发送限价单: {symbol}.{exchange.value} {direction.value} {volume} @ {price} Offset: {offset.value}")

        # Construct the request dictionary expected by OrderExecutionGatewayService.send_order
        order_req_dict = {
            "symbol": symbol,
            "exchange": exchange.value, # Send enum value
            "direction": direction.value, # Send enum value
            "type": OrderType.LIMIT.value, # Send enum value
            "volume": float(volume),
            "price": float(price),
            "offset": offset.value, # Send enum value
            "reference": "StrategyEngine" # Updated reference
        }

        # Format according to vnpy.rpc: (method_name, args_tuple, kwargs_dict)
        req_tuple = ("send_order", (order_req_dict,), {})

        # +++ Add DEBUG log before sending RPC +++
        logger.debug(f"send_limit_order: Calling _send_rpc_request with tuple: {req_tuple}")
        # +++ End Add +++

        # --- Use RPC Helper ---
        # Use default timeout and retries from constants
        vt_orderid = self._send_rpc_request(req_tuple)

        if vt_orderid:
            # Ensure vt_orderid is a string before logging/returning
            if not isinstance(vt_orderid, str):
                 logger.error(f"从订单网关收到的 vt_orderid 不是字符串: {vt_orderid} (类型: {type(vt_orderid)})。订单可能已发送但ID无效。")
                 # Decide how to handle - maybe return None or a special value?
                 return None
            logger.info(f"订单发送成功 (RPC确认): {symbol}, VT_OrderID: {vt_orderid}")
            self.last_order_time[symbol] = time.time() # Update cool down timer
            return vt_orderid # Return the order ID
        else:
            # Failure logged within _send_rpc_request
            logger.error(f"订单发送失败 (RPC调用未成功或网关返回错误): {symbol}")
            return None
        # --- End Use RPC Helper ---

    def cancel_limit_order(self, vt_orderid: str) -> bool:
        """(LIVE MODE ONLY) Sends a cancel order request via RPC."""
        if self.is_backtest_mode:
            logger.error("StrategyEngine.cancel_limit_order should not be called directly in backtest mode.")
            return False
        # --- Connection Check ---
        if not self.gateway_connected:
            logger.error(f"无法发送撤单指令 ({vt_orderid}): 与订单执行网关失去连接。")
            return False # Indicate failure

        if not vt_orderid or not isinstance(vt_orderid, str):
            logger.error(f"尝试发送无效 vt_orderid ({vt_orderid}) 的撤单请求。")
            return False

        logger.info(f"准备发送撤单指令: {vt_orderid}")

        # Construct the request dictionary expected by OrderExecutionGatewayService.cancel_order
        cancel_req_dict = {"vt_orderid": vt_orderid}

        # Format according to vnpy.rpc: (method_name, args_tuple, kwargs_dict)
        req_tuple = ("cancel_order", (cancel_req_dict,), {})

        # --- Use RPC Helper ---
        # Use default timeout and retries
        result_dict = self._send_rpc_request(req_tuple)

        # Check the result structure carefully
        if isinstance(result_dict, dict) and result_dict.get("status") == "ok":
            logger.info(f"撤单指令发送成功 (RPC确认): {vt_orderid}. 回复: {result_dict}")
            return True
        else:
            # Failure (or unexpected result) logged within _send_rpc_request or here
            if result_dict is None: # RPC call failed
                 logger.error(f"撤单指令发送失败 (RPC调用未成功): {vt_orderid}")
            else: # Gateway returned an error status or unexpected format
                 logger.error(f"撤单指令发送失败 (网关回复错误或格式无效): {vt_orderid}. 回复: {result_dict}")
            return False
        # --- End Use RPC Helper ---

    def start(self):
        """Starts listening for messages and running the strategy logic."""
        if self.running:
            logger.warning("策略引擎已在运行中。")
            return
        if not self.strategies:
             logger.error("没有加载任何策略，引擎无法启动。")
             return

        logger.info("启动策略引擎服务...")
        self.running = True

        # Start all loaded strategies
        logger.info("正在启动所有已加载的策略...")
        for strategy in self.strategies.values():
            strategy._start_strategy() # Call internal start method
        logger.info("所有策略已启动。")

        # Set initial heartbeat time on start to avoid immediate timeout detection
        # Let initial_connection_time handle the grace period
        # self.last_ordergw_heartbeat_time = time.time() 

        logger.info("策略引擎服务已启动，开始监听消息...")

        # +++ Define poll_timeout_ms here +++
        poll_timeout_ms = 1000 # Check every second

        # +++ Force initial ping on start +++
        if not self.is_backtest_mode:
            logger.info("启动后立即尝试 Ping 订单网关以建立初始连接状态...")
            self._send_ping() 
            # Optional: Add a small sleep to allow ping reply processing if needed
            # time.sleep(0.1) 
        # +++ End Force initial ping +++

        while self.running:
            try:
                poller = zmq.Poller()
                poller.register(self.subscriber, zmq.POLLIN)
                socks = dict(poller.poll(poll_timeout_ms))

                if self.subscriber in socks and socks[self.subscriber] == zmq.POLLIN:
                    # +++ Add DEBUG log for raw received parts +++
                    try:
                        parts = self.subscriber.recv_multipart(zmq.NOBLOCK)
                        if len(parts) == 2:
                            topic_bytes, data_bytes = parts
                            # Log only the topic and data length for brevity
                            logger.debug(f"StrategyEngine Recv Loop: Received parts. Topic Bytes: {topic_bytes!r} (len={len(topic_bytes)}), Data Bytes Len: {len(data_bytes)}")
                        else:
                            logger.warning(f"StrategyEngine Recv Loop: Received unexpected multipart count: {len(parts)}. Parts: {parts}")
                            continue # Skip processing this message
                    except zmq.Again:
                         logger.debug("StrategyEngine Recv Loop: zmq.Again caught on recv_multipart (should be rare after poll)")
                         continue # No message available despite poll?
                    except Exception as recv_err:
                         logger.error(f"StrategyEngine Recv Loop: Error during recv_multipart: {recv_err}")
                         continue # Skip processing on receive error
                    # +++ End Add DEBUG log +++

                    if not self.running: break # Check running state again after recv

                    try:
                        topic_str = topic_bytes.decode('utf-8', errors='ignore')
                        # +++ Log decoded topic +++
                        logger.debug(f"Decoded topic: '{topic_str}'")
                        # +++ End Log topic +++

                        # Replace pickle with msgpack
                        data_obj = msgpack.unpackb(data_bytes, raw=False)
                        # +++ Log object type and module UNCONDITIONALLY +++
                        obj_type = type(data_obj)
                        obj_module = getattr(data_obj.__class__, '__module__', 'N/A') # Get module name
                        logger.debug(f"Msgpack decoded: Object Type='{obj_type.__name__}', Module='{obj_module}'")
                        # +++ End Log type/module +++

                        # --- Event Dispatching (using data_obj and topic_map) ---
                        # +++ Use topic_map for checking prefixes +++
                        if topic_str.startswith(self.topic_map["tick"]): # Check against 'TICK.'
                            if isinstance(data_obj, TickData): 
                                vt_symbol = getattr(data_obj, 'vt_symbol', None)
                                if vt_symbol:
                                    self.process_tick(vt_symbol, data_obj)
                                else:
                                    logger.warning(f"TickData object on topic {topic_str} is missing 'vt_symbol' attribute.")
                            # +++ FIX: Pass dict to reconstruction helper +++
                            elif isinstance(data_obj, dict): # Check if it's a dict
                                 # --- Try to reconstruct TickData from dict ---
                                 tick_object_from_dict = create_tick_from_dict(data_obj)
                                 if tick_object_from_dict:
                                     logger.debug(f"Successfully reconstructed TickData from dict for topic {topic_str}")
                                     vt_symbol = getattr(tick_object_from_dict, 'vt_symbol', None)
                                     if vt_symbol:
                                         self.process_tick(vt_symbol, tick_object_from_dict)
                                     else:
                                         logger.warning(f"Reconstructed TickData object from dict is missing 'vt_symbol'. Topic: {topic_str}")
                                 else:
                                     logger.warning(f"Failed to reconstruct TickData from dict received on tick topic {topic_str}. Dict: {data_obj}")
                                 # --- End Reconstruction ---
                            # +++ End FIX +++
                            else:
                                logger.warning(f"Received non-TickData/non-dict object on tick topic {topic_str}: Type='{obj_type.__name__}', Module='{obj_module}'")
                        elif topic_str.startswith(self.topic_map["order"]):
                            # +++ FIX: Handle dict or object +++
                            if isinstance(data_obj, OrderData):
                                self.process_order_update(data_obj) # Pass object
                            elif isinstance(data_obj, dict):
                                 order_object_from_dict = create_order_from_dict(data_obj)
                                 if order_object_from_dict:
                                     logger.debug(f"Successfully reconstructed OrderData from dict for topic {topic_str}")
                                     self.process_order_update(order_object_from_dict)
                                 else:
                                     logger.warning(f"Failed to reconstruct OrderData from dict received on order topic {topic_str}. Dict: {data_obj}")
                            # +++ End FIX +++
                            else:
                                logger.warning(f"Received non-OrderData/non-dict object on order topic {topic_str}: {type(data_obj)}")
                        elif topic_str.startswith(self.topic_map["trade"]):
                            # +++ FIX: Handle dict or object +++
                            if isinstance(data_obj, TradeData):
                                self.process_trade_update(data_obj) # Pass object
                            elif isinstance(data_obj, dict):
                                trade_object_from_dict = create_trade_from_dict(data_obj)
                                if trade_object_from_dict:
                                     logger.debug(f"Successfully reconstructed TradeData from dict for topic {topic_str}")
                                     self.process_trade_update(trade_object_from_dict)
                                else:
                                     logger.warning(f"Failed to reconstruct TradeData from dict received on trade topic {topic_str}. Dict: {data_obj}")
                            # +++ End FIX +++
                            else:
                                logger.warning(f"Received non-TradeData/non-dict object on trade topic {topic_str}: {type(data_obj)}")
                        elif topic_str.startswith(self.topic_map["account"]):
                            # +++ FIX: Handle dict or object +++
                            account_object = None
                            if isinstance(data_obj, AccountData):
                                account_object = data_obj
                            elif isinstance(data_obj, dict):
                                account_object = create_account_from_dict(data_obj)
                            # +++ End FIX +++
                            if account_object:
                                self.process_account_update(account_object) # Pass object
                            else:
                                logger.warning(f"Received non-AccountData/non-dict or failed reconstruction on account topic {topic_str}. Original Type: {type(data_obj)}")
                        elif topic_str == self.topic_map["log"]: # Exact match for log
                            # +++ FIX: Handle dict or object +++
                            log_object = None
                            if isinstance(data_obj, LogData):
                                log_object = data_obj
                            elif isinstance(data_obj, dict):
                                log_object = create_log_from_dict(data_obj)
                            # +++ End FIX +++
                            if log_object:
                                self.process_log(log_object) # Pass object
                            else:
                                logger.warning(f"Received non-LogData/non-dict or failed reconstruction on log topic {topic_str}. Original Type: {type(data_obj)}")
                        elif topic_str.startswith(self.topic_map["contract"]):
                             # +++ FIX: Handle dict or object (if needed) +++
                             # If strategies need contract info, reconstruct here
                             # contract_object = None
                             # if isinstance(data_obj, ContractData): contract_object = data_obj
                             # elif isinstance(data_obj, dict): contract_object = create_contract_from_dict(data_obj) # Assuming this helper exists
                             # if contract_object: self.process_contract_update(contract_object)
                             # else: logger...
                             # +++ End FIX +++
                             pass # Ignore contract data for now
                        elif topic_str == self.topic_map["heartbeat_ordergw"]: # Exact match for heartbeat
                             # --- NEW CHECK: Expecting dict --- 
                             if isinstance(data_obj, dict):
                                 self.process_ordergw_heartbeat(data_obj) # Pass the dict
                             else:
                                 logger.warning(f"Received non-dict object on heartbeat topic: {type(data_obj)}")
                        # +++ End Use topic_map +++
                        else:
                            logger.warning(f"未知的消息主题: {topic_str}")

                    except (msgpack.UnpackException, TypeError, ValueError) as e_unpack:
                        # +++ Add more details to unpack error log +++
                        logger.error(f"Msgpack 解码错误: {e_unpack}. Topic: '{topic_bytes.decode('utf-8', errors='ignore')}', Data (first 100 bytes): {data_bytes[:100]!r}")
                        # +++ End Add details +++
                    except Exception as msg_proc_e:
                        logger.exception(f"处理订阅消息时出错 (Topic: {topic_bytes.decode('utf-8', errors='ignore')}): {msg_proc_e}")

                # --- Periodic Health Checks (Add heartbeat check) ---
                current_time = time.time()
                
                # 1. Check Order Gateway Heartbeat (Live mode only? Or backtest reports heartbeat?)
                # Assume heartbeat only relevant in live mode for now.
                if not self.is_backtest_mode and self.last_ordergw_heartbeat_time > 0 and \
                   (current_time - self.last_ordergw_heartbeat_time > self.heartbeat_timeout_s): # Use instance variable
                    if self.gateway_connected:
                        logger.error(f"订单网关心跳超时 ...")
                        self.gateway_connected = False
                        self.initial_connection_time = None

                # 2. Send periodic PING via RPC (Live mode only)
                if not self.is_backtest_mode and current_time - self.last_ping_time >= self.ping_interval: # Use instance variable
                    self._send_ping() 

                # 3. Check market data timeout (Improved logic)
                if self._is_trading_hours():
                    found_stale_symbol = False
                    stale_symbols = []
                    check_time = time.time()
                    initial_grace_period = self.market_data_timeout * self.initial_tick_grace_period_multiplier # Use instance variables

                    for symbol in self.subscribed_symbols:
                        last_ts = self.last_tick_time.get(symbol)

                        if last_ts is None: # --- Check for symbols that never received a tick ---
                            # Only check if gateway is connected and grace period has passed
                            if self.gateway_connected and self.initial_connection_time is not None and \
                               (check_time > self.initial_connection_time + initial_grace_period):
                                # Check if any running strategy needs this symbol
                                strategies_for_symbol = self.symbol_strategy_map.get(symbol, [])
                                needs_check = any(s.trading for s in strategies_for_symbol)
                                if needs_check:
                                    found_stale_symbol = True
                                    stale_symbols.append(f"{symbol} (no tick since connect+{initial_grace_period:.0f}s)")
                        else: # --- Check for symbols that have received ticks before ---
                            if check_time - last_ts > self.market_data_timeout: # Use instance variable
                                found_stale_symbol = True
                                stale_symbols.append(f"{symbol} (last {check_time - last_ts:.1f}s ago)")

                    # --- Log status changes --- 
                    if found_stale_symbol:
                        if self.market_data_ok:
                            logger.warning(f"[交易时段内] 行情数据可能中断或延迟! 超时合约: {', '.join(stale_symbols)}")
                            self.market_data_ok = False
                    elif not self.market_data_ok:
                        logger.info("所有监控合约的行情数据流已恢复。")
                        self.market_data_ok = True

            except zmq.ZMQError as err:
                 # Check if error occurred during shutdown (running is False)
                 # or if it's a context termination (ETERM)
                 # or if a socket was closed underneath (ENOTSOCK)
                 if not self.running or err.errno in (zmq.ETERM, zmq.ENOTSOCK):
                     logger.info(f"ZMQ 错误 ({err.errno}: {err}) 发生在服务停止期间，正常退出。")
                     break
                 else:
                     logger.error(f"主循环中意外的 ZMQ 错误: {err}")
                     self.running = False # Stop on other ZMQ errors
            except KeyboardInterrupt:
                logger.info("策略引擎检测到中断信号，开始停止...")
                self.running = False
            except Exception as err:
                logger.exception(f"策略引擎主循环发生未知错误：{err}")
                time.sleep(1) # Avoid rapid looping

        logger.info("策略引擎主循环结束。")
        # --- Remove redundant self.stop() call --- 
        # self.stop() # Ensure cleanup is called <<< REMOVED

        # Context termination is handled by the caller (run_backtest.py) or by stop() method
        # if self.context and not self.context.closed: # Check if context exists and not already closed
        #     try:
        #         logger.info("正在终止策略引擎的 ZMQ Context...")
        #         self.context.term()
        #         logger.info("策略引擎的 ZMQ Context 已成功终止。")
        #     except zmq.ZMQError as e:
        #         logger.error(f"终止策略引擎的 ZMQ Context 时发生 ZMQ 错误: {e}")
        #     except Exception as e:
        #         logger.error(f"终止策略引擎的 ZMQ Context 时发生未知错误: {e}")
        # else:
        #     logger.warning("策略引擎的 ZMQ Context 未找到或已关闭，跳过终止。")

    # --- FIX: Update methods to handle dictionaries --- 
    # --- FIX: Update methods to handle OBJECTS (after reconstruction) --- 

    def process_tick(self, vt_symbol: str, tick_object: TickData):
        """Dispatch tick data (TickData object) to relevant strategies."""
        self.last_tick_time[vt_symbol] = time.time() # Update timeout tracker
        
        strategies_for_symbol = self.symbol_strategy_map.get(vt_symbol, [])
        if strategies_for_symbol:
            # --- REMOVED Object creation here, it's done in start loop --- 
            # try:
            #      tick_object = create_tick_from_dict(tick_dict)
            #      if not tick_object: return
            # except Exception as e:
            #      logger.error(f"Error creating TickData object from dict in process_tick: {e}")
            #      return

            for strategy in strategies_for_symbol:
                if strategy.trading: 
                    try:
                        # --- Log before calling strategy.on_tick --- 
                        logger.debug(f"process_tick: Calling strategy '{strategy.strategy_name}' on_tick for vt_symbol '{vt_symbol}'.")
                        # --- End Log ---
                        # Pass the object to the strategy
                        strategy.on_tick(tick_object) # PASS OBJECT
                    except Exception as e:
                         logger.exception(f"策略 {strategy.strategy_name} 在 on_tick 处理 {vt_symbol} 时出错: {e}")

    def process_order_update(self, order_object: OrderData):
        """Dispatch order update (OrderData object) to relevant strategies."""
        # Use object attributes
        vt_symbol = order_object.vt_symbol
        vt_orderid = order_object.vt_orderid
        # if not vt_symbol or not vt_orderid: # Redundant check if object creation succeeded
        #     logger.warning(f"Order object received without vt_symbol or vt_orderid: {order_object}")
        #     return
        
        status_str = order_object.status.value # Get status value
        logger.debug(f"process_order_update: Received OrderID={vt_orderid} Status={status_str}")

        # --- REMOVED Object creation here --- 
        # order_object = create_order_from_dict(order_dict)
        # if not order_object: return

        strategies_for_symbol = self.symbol_strategy_map.get(vt_symbol, [])
        if strategies_for_symbol:
            for strategy in strategies_for_symbol:
                 try:
                     strategy.on_order(order_object) # PASS OBJECT
                 except Exception as e:
                     logger.exception(f"策略 {strategy.strategy_name} 在 on_order 处理 {vt_orderid} 时出错: {e}")

    def process_trade_update(self, trade_object: TradeData):
        """Dispatch trade update (TradeData object) to relevant strategies, handling duplicates."""
        # Use object attributes
        vt_symbol = trade_object.vt_symbol
        vt_tradeid = trade_object.vt_tradeid
        vt_orderid = trade_object.vt_orderid

        # --- Duplicate Check --- 
        if vt_tradeid:
            if vt_tradeid in self.processed_trade_ids:
                logger.debug(f"Ignoring duplicate trade event: {vt_tradeid}")
                return
            else:
                self.processed_trade_ids.append(vt_tradeid)
        else:
            logger.warning("Trade update received without vt_tradeid, cannot check for duplicates.")
        # --- End Duplicate Check ---

        # if not vt_symbol: # Redundant check
        #     logger.warning(f"Trade update received without vt_symbol: {trade_object}")
        #     return

        # Log using object attributes
        offset_str = trade_object.offset.value
        price = trade_object.price
        volume = trade_object.volume
        logger.info(f"收到成交回报: {vt_symbol} OrderID: {vt_orderid} TradeID: {vt_tradeid} Offset: {offset_str} Price: {price} Vol: {volume}")
        # self.trades.append(trade_object) # Store object if needed

        # --- REMOVED Object creation here --- 
        # trade_object = create_trade_from_dict(trade_dict)
        # if not trade_object: return

        strategies_for_symbol = self.symbol_strategy_map.get(vt_symbol, [])
        if strategies_for_symbol:
            for strategy in strategies_for_symbol:
                 try:
                     strategy.on_trade(trade_object) # PASS OBJECT
                 except Exception as e:
                     logger.exception(f"策略 {strategy.strategy_name} 在 on_trade 处理 {vt_tradeid} 时出错: {e}")

    def process_account_update(self, account_object: AccountData):
        """Dispatch account data update (AccountData object) to relevant strategies."""
        # Use object attributes
        accountid = account_object.accountid
        logger.debug(f"process_account_update: Received update for AccountID: '{accountid}'")

        # --- REMOVED Object creation here --- 
        # account_object = create_account_from_dict(account_dict)
        # if not account_object: return

        for strategy in self.strategies.values():
             if hasattr(strategy, 'on_account'):
                 try:
                     strategy.on_account(account_object) # PASS OBJECT
                 except Exception as e:
                     logger.exception(f"策略 {strategy.strategy_name} 在 on_account 处理时出错: {e}")

    def process_log(self, log_object: LogData):
        """Processes log messages (LogData object) from gateways (log globally)."""
        # --- REMOVED Object creation here --- 
        # log_object = create_log_from_dict(log_dict)
        # if not log_object: return

        gateway_name = log_object.gateway_name
        msg = log_object.msg
        level_int = log_object.level # Already int

        # Use the engine's logger
        log_func = logger.info # Default
        if level_int == logging.DEBUG: log_func = logger.debug
        elif level_int == logging.WARNING: log_func = logger.warning
        elif level_int == logging.ERROR: log_func = logger.error
        elif level_int == logging.CRITICAL: log_func = logger.critical

        log_func(f"[GW LOG - {gateway_name}] {msg}")

    def process_ordergw_heartbeat(self, heartbeat_dict: dict):
        """Processes heartbeat message (dictionary) from OrderExecutionGateway."""
        # Assuming heartbeat message is now a dict like {'timestamp': float}
        heartbeat_ts = heartbeat_dict.get('timestamp')
        if heartbeat_ts is None or not isinstance(heartbeat_ts, (float, int)):
             logger.warning(f"Received invalid format for heartbeat: {heartbeat_dict}")
             return

        try:
            now_ts = time.time()
            # Log using ISO format for consistency
            heartbeat_dt_str = datetime.fromtimestamp(heartbeat_ts).isoformat()
            now_dt_str = datetime.fromtimestamp(now_ts).isoformat()
            logger.debug(f"(DEBUG HB Recv) TS={heartbeat_dt_str}, NOW={now_dt_str}")
            self.last_ordergw_heartbeat_time = now_ts # Record reception time

            # +++ Set initial connection time only once +++
            if not self.gateway_connected:
                logger.info("接收到订单网关心跳，标记为已连接。设置初始连接时间。")
                self.gateway_connected = True
                if self.initial_connection_time is None:
                     self.initial_connection_time = now_ts
            # +++ End Set initial connection time +++
            # self.gateway_connected = True # Set unconditionally? No, let initial_connection_time track first connect

            # Warn if received timestamp is too old (potential clock skew or major delay)
            # Use the global constant HEARTBEAT_TIMEOUT_S - Use correct parameter name
            if (now_ts - heartbeat_ts) > (self.heartbeat_timeout_s / 2):
                 logger.warning(
                     f"收到过期的 OrderGW 心跳 (Timestamp: {datetime.fromtimestamp(heartbeat_ts).isoformat()}, "
                     f"Now: {datetime.fromtimestamp(now_ts).isoformat()}) - 可能存在时钟不同步或延迟。"
                 )
        except Exception as e:
            logger.exception(f"处理 OrderGW 心跳消息时出错: {e}")

    def stop(self):
        """Stops the strategy engine and all strategies."""
        if not self.running:
            # +++ Add a log if stop is called when not running +++
            logger.info("StrategyEngine.stop() 被调用，但引擎未在运行或已被停止。")
            return

        logger.info("开始停止策略引擎服务...")
        self.running = False # Signal loops to stop

        # Stop all strategies
        logger.info("正在停止所有策略...")
        for strategy in self.strategies.values():
             try:
                 strategy._stop_strategy() # Call internal stop
             except Exception as e:
                 logger.exception(f"停止策略 {strategy.strategy_name} 时出错: {e}")
        logger.info("所有策略已停止。")

        # --- Add Socket Closing Logic ---
        logger.info("正在关闭策略引擎 ZMQ Sockets...")
        sockets_to_close = [
            self.subscriber, 
            self.order_requester
        ]
        # Add order_pusher to the list if it exists (for backtest mode)
        if hasattr(self, 'order_pusher') and self.order_pusher:
            sockets_to_close.append(self.order_pusher)

        for sock in sockets_to_close:
            if sock and not sock.closed:
                try:
                    logger.debug(f"正在关闭 Socket: {sock}...")
                    sock.close(linger=0) # Set linger to 0 to close immediately
                    logger.debug(f"Socket {sock} 已关闭。")
                except Exception as e:
                    logger.error(f"关闭策略引擎 Socket {sock} 时出错: {e}")
        logger.info("策略引擎 ZMQ Sockets 已关闭。")
        # --- End Add Socket Closing Logic ---

        # +++ Terminate context here in stop() method +++
        if self.context and not self.context.closed:
            try:
                logger.info("正在终止策略引擎的 ZMQ Context (from stop method)...")
                self.context.term()
                logger.info("策略引擎的 ZMQ Context 已成功终止 (from stop method)。")
            except zmq.ZMQError as e_term:
                logger.error(f"终止策略引擎的 ZMQ Context 时发生 ZMQ 错误: {e_term}")
            except Exception as e_term_general:
                logger.exception(f"终止策略引擎的 ZMQ Context 时发生未知错误: {e_term_general}")
        else:
            logger.info("策略引擎的 ZMQ Context 未找到或已关闭 (from stop method)，跳过终止。")
        # +++ End Terminate context +++

# Helper function to create OrderData from dict (handle potential errors)
def create_order_from_dict(d: dict) -> Optional[OrderData]:
    try:
        symbol, exchange = extract_vt_symbol(d['vt_symbol'])
        order = OrderData(
            gateway_name=d.get('gateway_name', 'GATEWAY'), # Use default if missing
            symbol=symbol,
            exchange=exchange,
            orderid=d['orderid'],
            direction=Direction(d['direction']), # Enum from value
            offset=Offset(d.get('offset', Offset.NONE.value)), # Enum from value
            type=OrderType(d['type']), # Enum from value
            price=float(d.get('price', 0.0)),
            volume=float(d['volume']),
            traded=float(d.get('traded', 0.0)),
            status=Status(d['status']), # Enum from value
            datetime=datetime.fromisoformat(d['datetime']) if d.get('datetime') else datetime.now()
        )
        order.vt_symbol = d['vt_symbol'] # Ensure vt_symbol is set
        order.vt_orderid = d['vt_orderid']
        return order
    except (KeyError, ValueError, TypeError) as e:
        logger.error(f"Error creating OrderData from dict: {e}. Dict: {d}")
        return None

# Helper function to create TradeData from dict
def create_trade_from_dict(d: dict) -> Optional[TradeData]:
    try:
        symbol, exchange = extract_vt_symbol(d['vt_symbol'])
        trade = TradeData(
            gateway_name=d.get('gateway_name', 'GATEWAY'),
            symbol=symbol,
            exchange=exchange,
            orderid=d['orderid'],
            tradeid=d['tradeid'],
            direction=Direction(d['direction']), # Enum from value
            offset=Offset(d.get('offset', Offset.NONE.value)), # Enum from value
            price=float(d['price']),
            volume=float(d['volume']),
            datetime=datetime.fromisoformat(d['datetime']) if d.get('datetime') else datetime.now()
        )
        trade.vt_symbol = d['vt_symbol']
        trade.vt_orderid = d['vt_orderid']
        trade.vt_tradeid = d['vt_tradeid']
        # Add commission if present in dict
        trade.commission = float(d.get('commission', 0.0))
        return trade
    except (KeyError, ValueError, TypeError) as e:
        logger.error(f"Error creating TradeData from dict: {e}. Dict: {d}")
        return None

# Helper function to create TickData from dict (simplified)
def create_tick_from_dict(d: dict) -> Optional[TickData]:
    try:
        symbol, exchange = extract_vt_symbol(d['vt_symbol'])
        # Create TickData with necessary fields, add more if strategies need them
        tick = TickData(
            gateway_name=d.get('gateway_name', 'GATEWAY'),
            symbol=symbol,
            exchange=exchange,
            datetime=datetime.fromisoformat(d['datetime']) if d.get('datetime') else datetime.now(),
            last_price=float(d.get('last_price', 0.0)),
            volume=float(d.get('volume', 0.0)), # Cumulative volume
            ask_price_1=float(d.get('ask_price_1', 0.0)),
            bid_price_1=float(d.get('bid_price_1', 0.0)),
            ask_volume_1=float(d.get('ask_volume_1', 0.0)),
            bid_volume_1=float(d.get('bid_volume_1', 0.0))
            # Add other fields like open, high, low, pre_close, open_interest etc. if needed
        )
        tick.vt_symbol = d['vt_symbol']
        return tick
    except (KeyError, ValueError, TypeError) as e:
        logger.error(f"Error creating TickData from dict: {e}. Dict: {d}")
        return None

# Helper function to create AccountData from dict
def create_account_from_dict(d: dict) -> Optional[AccountData]:
    try:
        account = AccountData(
            gateway_name=d.get('gateway_name', 'GATEWAY'),
            accountid=d['accountid'],
            balance=float(d.get('balance', 0.0)),
            frozen=float(d.get('frozen', 0.0))
            # Add other fields like available, commission, margin etc. if present
        )
        account.available = float(d.get('available', account.balance - account.frozen)) # Calculate if missing
        return account
    except (KeyError, ValueError, TypeError) as e:
        logger.error(f"Error creating AccountData from dict: {e}. Dict: {d}")
        return None

# Helper function to create LogData from dict
def create_log_from_dict(d: dict) -> Optional[LogData]:
    try:
        # --- FIX: Handle integer log level --- 
        level_input = d.get('level', logging.INFO) # Default to INFO if missing
        log_level = logging.INFO # Default
        if isinstance(level_input, int):
            # Map common logging level integers back to constants
            level_map_int = {
                logging.CRITICAL: logging.CRITICAL,
                logging.ERROR: logging.ERROR,
                logging.WARNING: logging.WARNING,
                logging.INFO: logging.INFO,
                logging.DEBUG: logging.DEBUG,
                logging.NOTSET: logging.NOTSET
            }
            log_level = level_map_int.get(level_input, logging.INFO) # Use map, default INFO
        elif isinstance(level_input, str):
            # If it's a string, try getting attribute after converting to upper
            log_level = getattr(logging, level_input.upper(), logging.INFO)
        # Else: Keep default INFO if type is unexpected
        # --- End FIX ---

        log = LogData(
            gateway_name=d.get('gateway_name', 'GATEWAY'),
            msg=d.get('msg', ''),
            level=log_level # Use the determined log_level
        )
        return log
    except (KeyError, ValueError, TypeError) as e:
        logger.error(f"Error creating LogData from dict: {e}. Dict: {d}")
        return None
