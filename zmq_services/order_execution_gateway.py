import zmq
import msgpack
import time
import threading
import sys
import os
from datetime import datetime
from typing import Dict, Tuple, Any
import configparser
import logging
import pickle

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
                                    LogData, ContractData, AccountData)
    from vnpy.trader.event import (EVENT_ORDER, EVENT_TRADE, EVENT_LOG, EVENT_CONTRACT, EVENT_ACCOUNT)
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

# --- 移除硬编码规则 ---
# COMMISSION_RULES = { ... }
# --- 结束移除 ---

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

# --- Order Execution Gateway Service (RPC Mode) ---
class OrderExecutionGatewayService(RpcServer):
    """
    Order execution gateway service using CtpGateway and RpcServer.
    Handles order sending/canceling via RPC calls and publishes order/trade updates.
    """
    def __init__(self):
        """Initializes the execution gateway service."""
        super().__init__()
        self.logger = getLogger(__name__)

        # VNPY setup
        self.event_engine = EventEngine()
        self.gateway: BaseGateway = CtpGateway(self.event_engine, "CTP_Execution_RPC")

        # CTP settings
        self.ctp_setting = {
            "userid": config.CTP_USER_ID,
            "password": config.CTP_PASSWORD,
            "broker_id": config.CTP_BROKER_ID,
            "td_address": config.CTP_TD_ADDRESS,
            "md_address": config.CTP_MD_ADDRESS,
            "appid": config.CTP_PRODUCT_INFO,
            "auth_code": config.CTP_AUTH_CODE,
            "env": config.CTP_ENV_TYPE
        }

        self.contracts: Dict[str, ContractData] = {}
        self.last_account_data: AccountData | None = None
        self.last_account_log_time: float = 0.0 # Time of last log print for ANY account
        self._last_account_key_values: Dict[str, Tuple[float, float]] = {} # Stores last logged (Balance, Available) per account
        self.account_log_interval: int = 60 # Log at least every 60 seconds

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

        self.logger.info("订单执行网关服务(RPC模式)初始化完成。")

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

        vt_symbol = f"{order_request.symbol}.{order_request.exchange.value}"
        contract = self.contracts.get(vt_symbol)
        if not contract:
            max_retries = 3
            retry_delay = 0.2
            for i in range(max_retries):
                self.logger.warning(f"RPC: send_order - 合约 {vt_symbol} 尚未缓存，等待 {retry_delay} 秒后重试 ({i+1}/{max_retries})...")
                time.sleep(retry_delay)
                contract = self.contracts.get(vt_symbol)
                if contract:
                    self.logger.info(f"RPC: send_order - 重试成功，找到合约 {vt_symbol}。")
                    break
            if not contract:
                self.logger.error(f"RPC: send_order - 错误: 合约 {vt_symbol} 信息不存在 (已重试)。订单无法发送。")
                return None

        try:
            vt_orderid = self.gateway.send_order(order_request)
            if vt_orderid:
                self.logger.info(f"RPC: send_order - 订单已发送至 CTP: {order_request.symbol}, VT_OrderID: {vt_orderid}")
                return vt_orderid
            else:
                self.logger.error(f"RPC: send_order - CTP 网关未能发送订单: {order_request.symbol}")
                return None
        except Exception as e:
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
        return {vt_symbol: contract.to_dict() for vt_symbol, contract in self.contracts.items()}

    def query_account(self) -> Dict | None:
        """RPC handler to query the latest account data."""
        self.logger.info("RPC: 收到 query_account 请求")
        if self.last_account_data:
            return self.last_account_data.to_dict()
        else:
            return None

    # +++ Add ping method +++
    def ping(self) -> str:
        """Handles ping request from client."""
        # self.logger.debug("Received ping request") # Optional: log ping
        return "pong"
    # +++ End add ping method +++

    # --- Event Processing ---
    def process_vnpy_event(self, event: Event):
        """Processes events from the EventEngine and publishes them via RPC."""
        event_type = event.type

        if event_type == EVENT_ORDER:
            order: OrderData = event.data
            # self.publish(f"order.{order.vt_orderid}", order) # <-- Comment out
            try:
                topic_bytes = f"order.{order.vt_orderid}".encode('utf-8')
                data_bytes = pickle.dumps(order)
                self._socket_pub.send_multipart([topic_bytes, data_bytes])
            except Exception as e:
                self.logger.exception(f"手动发送 multipart Order 时出错: {e}")

        elif event_type == EVENT_TRADE:
            trade: TradeData = event.data
            calculated_commission = 0.0
            contract = self.contracts.get(trade.vt_symbol)
            rules = self.commission_rules.get(trade.symbol)

            if contract and rules:
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
                if fixed > 0:
                    calculated_commission = trade.volume * fixed
                elif rate > 0:
                    calculated_commission = turnover * rate
                calculated_commission = max(calculated_commission, min_comm)
                trade.calculated_commission = calculated_commission
            elif not contract:
                self.logger.warning(f"未找到成交 {trade.vt_tradeid} 对应的合约信息 ({trade.vt_symbol})，手续费计为 0。")
            elif not rules:
                 self.logger.warning(f"未找到成交 {trade.vt_tradeid} 对应的手续费规则 ({trade.symbol})，手续费计为 0。")

            # self.publish(f"trade.{trade.vt_symbol}", trade) # <-- Comment out
            try:
                topic_bytes = f"trade.{trade.vt_symbol}".encode('utf-8')
                data_bytes = pickle.dumps(trade)
                self._socket_pub.send_multipart([topic_bytes, data_bytes])
            except Exception as e:
                self.logger.exception(f"手动发送 multipart Trade 时出错: {e}")

        elif event_type == EVENT_LOG:
            log: LogData = event.data
            # self.publish("log", log) # <-- Comment out
            level_map = { logging.DEBUG: logging.DEBUG, logging.INFO: logging.INFO, logging.WARNING: logging.WARNING, logging.ERROR: logging.ERROR, logging.CRITICAL: logging.CRITICAL }
            log_level_value = getattr(log, 'level', logging.INFO)
            if hasattr(log_level_value, 'value'): log_level_value = log_level_value.value
            if not isinstance(log_level_value, int): log_level_value = logging.INFO
            logger_level = level_map.get(log_level_value, logging.INFO)
            gateway_name = getattr(log, 'gateway_name', 'UnknownGateway')
            self.logger.log(logger_level, f"[VNPY LOG - ExecRPC] {gateway_name} - {log.msg}")

            # ... (local logging logic remains the same) ...
            try:
                topic_bytes = b"log"
                data_bytes = pickle.dumps(log)
                self._socket_pub.send_multipart([topic_bytes, data_bytes])
            except Exception as e:
                 self.logger.exception(f"手动发送 multipart Log 时出错: {e}")

        elif event_type == EVENT_ACCOUNT:
            account: AccountData = event.data
            self.last_account_data = account # Store the latest full data

            # --- Throttle Account Update Logging ---
            current_time = time.time()
            accountid = getattr(account, 'accountid', 'N/A')
            balance = getattr(account, 'balance', 0.0)
            available = getattr(account, 'available', 0.0)
            margin = getattr(account, 'margin', 0.0) # Keep for logging
            frozen = getattr(account, 'frozen', 0.0) # Keep for logging

            # Get last logged values for this specific account
            last_balance, last_available = self._last_account_key_values.get(accountid, (None, None))

            # Determine if logging is needed
            log_due_to_time = (current_time - self.last_account_log_time) >= self.account_log_interval
            log_due_to_change = (last_balance is None or balance != last_balance or available != last_available)

            if log_due_to_change or log_due_to_time:
                self.logger.info(
                    f"账户关键信息更新 (RPC): AccountID={accountid}, "
                    f"Balance={balance:.2f}, Available={available:.2f}, "
                    f"Margin={margin:.2f}, Frozen={frozen:.2f}"
                )
                # Update last logged values and time
                self._last_account_key_values[accountid] = (balance, available)
                self.last_account_log_time = current_time # Reset time threshold after logging for any account
            # --- End Throttle ---

            # Publish account data regardless of logging
            try:
                topic_bytes = f"account.{account.accountid}".encode('utf-8')
                data_bytes = pickle.dumps(account)
                self._socket_pub.send_multipart([topic_bytes, data_bytes])
            except Exception as e:
                 self.logger.exception(f"手动发送 multipart Account 时出错: {e}")

        elif event_type == EVENT_CONTRACT:
             contract: ContractData = event.data
             self.contracts[contract.vt_symbol] = contract
             # self.publish(f"contract.{contract.vt_symbol}", contract) # <-- Comment out
             try:
                 topic_bytes = f"contract.{contract.vt_symbol}".encode('utf-8')
                 data_bytes = pickle.dumps(contract)
                 self._socket_pub.send_multipart([topic_bytes, data_bytes])
             except Exception as e:
                 self.logger.exception(f"手动发送 multipart Contract 时出错: {e}")

    # --- Lifecycle Management ---
    def start(self):
        """Starts the RpcServer, EventEngine, and connects the gateway."""
        if self.is_active():
            self.logger.warning("订单执行网关服务(RPC模式)已在运行中。")
            return

        self.logger.info("启动订单执行网关服务(RPC模式)...")

        try:
            super().start(
                rep_address=config.ORDER_GATEWAY_REP_ADDRESS,
                pub_address=config.ORDER_GATEWAY_PUB_ADDRESS
            )
            self.logger.info(f"RPC 服务器已启动。 REP: {config.ORDER_GATEWAY_REP_ADDRESS}, PUB: {config.ORDER_GATEWAY_PUB_ADDRESS}")
        except Exception as e:
            self.logger.exception(f"启动 RPC 服务器失败: {e}")
            return

        self.event_engine.register(EVENT_ORDER, self.process_vnpy_event)
        self.event_engine.register(EVENT_TRADE, self.process_vnpy_event)
        self.event_engine.register(EVENT_LOG, self.process_vnpy_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_vnpy_event)
        self.event_engine.register(EVENT_ACCOUNT, self.process_vnpy_event)
        self.event_engine.start()
        self.logger.info("事件引擎已启动。")

        self.logger.info("连接 CTP 网关...")
        try:
            setting_json: dict = load_json("connect_ctp.json")
            self.logger.info(f"CTP 连接配置 (JSON): Env={setting_json.get('env', 'N/A')}, "
                             f"TD={setting_json.get('td_address', 'N/A')}, "
                             f"MD={setting_json.get('md_address', 'N/A')}")
            self.gateway.connect(self.ctp_setting)
            self.logger.info("CTP 交易网关连接请求已发送。等待连接成功...")
            # TODO: Implement event-based waiting for connection status
            time.sleep(10)
            self.logger.info("CTP 交易网关假定连接成功（基于延时）。")
            # Removed incorrect calls to query_contract and query_account
            # Contract and account data should arrive via events after connection
        except Exception as err:
            self.logger.exception(f"连接或查询 CTP 交易网关时发生严重错误: {err}")
            self.stop()
            return

        self.logger.info("订单执行网关服务(RPC模式)启动完成。")

    def stop(self):
        """Stops the EventEngine, CTP Gateway, and RpcServer."""
        if not self.is_active():
            self.logger.warning("订单执行网关服务(RPC模式)未运行。")
            return

        self.logger.info("停止订单执行网关服务(RPC模式)...")

        try:
            if hasattr(self.event_engine, '_active') and self.event_engine._active:
                self.event_engine.stop()
                self.logger.info("事件引擎已停止。")
            else:
                 self.logger.warning("事件引擎未运行或状态未知。")
        except Exception as e:
            self.logger.exception(f"停止事件引擎时出错: {e}")

        if self.gateway:
            try:
                self.gateway.close()
                self.logger.info("CTP 交易网关已关闭。")
            except Exception as e:
                self.logger.exception(f"关闭 CTP 网关时出错: {e}")

        super().stop()
        self.logger.info("RPC 服务器已停止。")

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

    gw_service = OrderExecutionGatewayService()
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
