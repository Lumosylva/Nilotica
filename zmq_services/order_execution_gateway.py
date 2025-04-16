import zmq
import msgpack
import time
import threading
import sys
import os
from datetime import datetime
from typing import Dict, Tuple
import configparser
# Import standard logging
import logging
# Import logger
from logger import getLogger

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# VNPY imports
try:
    from vnpy.event import EventEngine, Event
    from vnpy.trader.gateway import BaseGateway
    from vnpy.trader.object import (OrderData, TradeData, OrderRequest, CancelRequest,
                                    LogData, ContractData, AccountData) # Import necessary objects
    from vnpy.trader.event import (EVENT_ORDER, EVENT_TRADE, EVENT_LOG, EVENT_CONTRACT, EVENT_ACCOUNT,
                                   EVENT_TIMER) # Import necessary events
    from vnpy.trader.constant import (Direction, OrderType, Exchange, Offset, Status)
    from vnpy_ctp import CtpGateway # Import the specific gateway
except ImportError as e:
    print(f"Error importing vnpy modules: {e}")
    print("Please ensure vnpy and vnpy_ctp are installed and accessible.")
    # Log critical error if vnpy components fail to import
    # Logger might not be available yet, so print is a fallback
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
    logger = getLogger('load_product_info') # Get logger for this function

    parser = configparser.ConfigParser()
    if not os.path.exists(filepath):
        logger.error(f"错误：产品信息文件未找到 {filepath}")
        return {}, {}

    try:
        parser.read(filepath, encoding='utf-8')
    except Exception as e:
        logger.exception(f"错误：读取产品信息文件 {filepath} 时出错")
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
            # logger.debug(f"加载 {symbol}: Multiplier={multiplier}, Rules={rule}")
        except ValueError as e:
            logger.warning(f"警告：解析文件 {filepath} 中 [{symbol}] 的数值时出错: {e}，跳过此合约。")
        except Exception as e:
            logger.warning(f"警告：处理文件 {filepath} 中 [{symbol}] 时发生未知错误: {e}，跳过此合约。")
            
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
            print(f"Warning: Unhandled type in vnpy_data_to_dict: {type(obj)}. Converting to string.")
            return str(obj)

def dict_to_order_request(data_dict):
    """Converts a dictionary back into a vnpy OrderRequest object."""
    logger = getLogger('dict_to_order_request') # Get logger for this function
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
        logger.error(f"创建 OrderRequest 失败：缺少关键字段 {e}")
        logger.error(f"原始数据: {data_dict}")
        return None
    except ValueError as e:
        logger.error(f"创建 OrderRequest 失败：无效的枚举值 {e}")
        logger.error(f"原始数据: {data_dict}")
        return None
    except Exception as e:
        logger.exception(f"创建 OrderRequest 时发生未知错误")
        logger.error(f"原始数据: {data_dict}")
        return None

# --- Order Execution Gateway Service ---
class OrderExecutionGatewayService:
    def __init__(self):
        """Initializes the execution gateway service."""
        self.logger = getLogger(__name__) # Get logger for this class

        self.context = zmq.Context()

        # Socket to receive order requests from strategies
        self.order_puller = self.context.socket(zmq.PULL)
        self.order_puller.bind(config.ORDER_REQUEST_PULL_URL)
        self.logger.info(f"订单请求接收器绑定到: {config.ORDER_REQUEST_PULL_URL}")

        # Socket to publish order/trade updates
        self.report_publisher = self.context.socket(zmq.PUB)
        self.report_publisher.bind(config.ORDER_REPORT_PUB_URL)
        self.logger.info(f"订单/成交回报发布器绑定到: {config.ORDER_REPORT_PUB_URL}")

        # +++ Add Command Socket (REP) +++
        self.command_socket = self.context.socket(zmq.REP)
        self.command_socket.bind(config.ORDER_GATEWAY_COMMAND_URL)
        self.logger.info(f"指令接收器绑定到: {config.ORDER_GATEWAY_COMMAND_URL}")

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
        self.command_thread = None # Thread for handling commands
        self.last_account_data: AccountData | None = None # Store last account state

        # +++ 加载产品信息 +++
        # 修正路径：从 zmq_services 目录出发，向上一级到项目根目录，再进入 config
        config_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'config'))
        info_filepath = os.path.join(config_dir, 'project_files', 'product_info.ini')
        self.logger.info(f"尝试从 {info_filepath} 加载产品信息...")
        self.commission_rules, self.contract_multipliers = load_product_info(info_filepath)
        if not self.commission_rules or not self.contract_multipliers:
             self.logger.warning("警告：未能成功加载手续费规则或合约乘数，手续费计算可能不准确！")
        # +++ 结束加载 +++

        self.logger.info("订单执行网关服务初始化完成。")

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
                # else: # Assume default handling or log warning if offset is unexpected
                #     self.logger.warning(f"未知的开平仓方向 {trade.offset} 用于手续费计算，默认按开仓处理。")
                #     rate = rules.get('open_rate', 0.0)
                #     fixed = rules.get('open_fixed', 0.0)

                if fixed > 0:
                    calculated_commission = trade.volume * fixed
                elif rate > 0:
                    calculated_commission = turnover * rate
                
                calculated_commission = max(calculated_commission, min_comm)
                # print(f"收到成交回报: {trade.vt_tradeid}, 使用加载规则计算手续费: {calculated_commission:.2f}") # 更新日志

                # Log the trade information
                # self.logger.info(f"收到成交回报: TradeID={trade.vt_tradeid}, Symbol={trade.symbol}, Dir={trade.direction.value}, Off={trade.offset.value}, Px={trade.price}, Vol={trade.volume}, Commission={calculated_commission:.2f}")
            
            elif not contract:
                print(f"警告: 未找到成交 {trade.vt_tradeid} 对应的合约信息 ({trade.vt_symbol})，手续费计为 0。")
                self.logger.warning(f"未找到成交 {trade.vt_tradeid} 对应的合约信息 ({trade.vt_symbol})，手续费计为 0。")
            elif not rules:
                 print(f"警告: 未找到成交 {trade.vt_tradeid} 对应的手续费规则 ({trade.symbol})，手续费计为 0。")
                 self.logger.warning(f"未找到成交 {trade.vt_tradeid} 对应的手续费规则 ({trade.symbol})，手续费计为 0。")
            # +++ 结束计算 +++

            self.publish_report(trade, "TRADE", calculated_commission=calculated_commission)
        elif event_type == EVENT_LOG:
            log: LogData = event.data
            # Map vnpy log level
            level_map = {
                logging.DEBUG: logging.DEBUG,
                logging.INFO: logging.INFO,
                logging.WARNING: logging.WARNING,
                logging.ERROR: logging.ERROR,
                logging.CRITICAL: logging.CRITICAL,
            }
            log_level_attr = getattr(log, 'level', logging.INFO)
            log_level_value = log_level_attr
            if hasattr(log_level_attr, 'value') and isinstance(log_level_attr.value, int):
                log_level_value = log_level_attr.value
            elif not isinstance(log_level_attr, int):
                 log_level_value = logging.INFO
            logger_level = level_map.get(log_level_value, logging.INFO)

            gateway_name = getattr(log, 'gateway_name', 'UnknownGateway')
            # Add ExecGW prefix to distinguish from MarketDataGateway logs if needed
            self.logger.log(logger_level, f"[VNPY LOG - ExecGW] {gateway_name} - {log.msg}")
        elif event_type == EVENT_ACCOUNT:
             # --- Process AccountData only if key fields changed --- 
             account: AccountData = event.data
             has_key_fields_changed = False
             if self.last_account_data is None:
                 has_key_fields_changed = True
             else:
                 # Compare margin, frozen, commission - adjust fields as needed
                 if (getattr(account, 'margin', None) != getattr(self.last_account_data, 'margin', None) or
                     getattr(account, 'frozen', None) != getattr(self.last_account_data, 'frozen', None) or
                     getattr(account, 'commission', None) != getattr(self.last_account_data, 'commission', None)):
                      has_key_fields_changed = True

             if has_key_fields_changed:
                 self.last_account_data = account # Update stored data
                 pretty_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                 self.logger.info(f"[{pretty_time}] 账户关键信息更新 (GW): AccountID={account.accountid}, Balance={account.balance:.2f}, Available={account.available:.2f}, Margin={getattr(account, 'margin', 0.0):.2f}")
                 self.publish_report(account, "ACCOUNT_DATA")
             # --- End change check --- 
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
        elif isinstance(data_object, AccountData):
             topic_str = f"ACCOUNT_DATA.{data_object.accountid}"
        else:
            print(f"收到未知类型回报，无法发布: {type(data_object)}")
            self.logger.warning(f"收到未知类型回报，无法发布: {type(data_object)}")
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
            self.logger.exception(f"序列化或发布回报时出错 ({topic_str})")
            # Avoid logging full data object in production
            # self.logger.debug(f"Failed report data (partial): {{'topic': '{topic_str}', 'type': '{report_type}', ...}}")

    def _handle_cancel_order_command(self, command_data: dict) -> dict:
        """Handles a cancel order command received via ZMQ."""
        vt_orderid = command_data.get('vt_orderid')
        if not vt_orderid:
            self.logger.error("收到无效的撤单指令：缺少 'vt_orderid' 字段。")
            return {"status": "error", "message": "Missing vt_orderid"}

        # Find the original order request details (optional but good practice)
        # This requires storing active orders or querying the gateway if possible
        # For now, directly create CancelRequest using vt_orderid
        
        # We need symbol and exchange for CancelRequest. We might not have them readily.
        # Hacky approach: Extract from vt_orderid if format is consistent (e.g., "GatewayName.OrderID")
        # Better approach: Requires querying gateway for order details or storing order details locally.
        # Assuming we cannot easily get symbol/exchange, we might need to adjust CancelRequest handling
        # or how the Risk Manager sends the request.
        # Let's try to infer from vt_orderid (assuming format "Gateway.Exchange.Symbol.LocalID")
        # This is very fragile!
        parts = vt_orderid.split('.')
        if len(parts) < 3:
             self.logger.error(f"无法从 vt_orderid '{vt_orderid}' 推断 symbol/exchange 来创建撤单请求。")
             return {"status": "error", "message": f"Cannot parse vt_orderid: {vt_orderid}"}

        # Inferring symbol and exchange (VERY FRAGILE - NEEDS REVIEW)
        # Example: CTP.SHFE.rb2310.12345 -> exchange=SHFE, symbol=rb2310
        try:
            # We need the OrderData object associated with vt_orderid to get exact details.
            # Let's search the gateway's orders (assuming it has a get_order method)
            order_to_cancel = self.gateway.get_order(vt_orderid)
            if not order_to_cancel:
                self.logger.error(f"尝试撤销订单失败：在网关中未找到订单 {vt_orderid}")
                return {"status": "error", "message": f"Order {vt_orderid} not found in gateway"}

            req = CancelRequest(
                orderid=order_to_cancel.orderid, # Use the gateway's internal order ID
                symbol=order_to_cancel.symbol,
                exchange=order_to_cancel.exchange,
                # vt_orderid=vt_orderid # vt_orderid is usually not part of CancelRequest
            )
            self.logger.info(f"收到撤单指令，尝试撤销订单: {vt_orderid} (Symbol: {req.symbol}, Exchange: {req.exchange.value})")
            self.gateway.cancel_order(req)
            return {"status": "ok", "message": f"Cancel request sent for {vt_orderid}"}

        except AttributeError:
             self.logger.error("当前网关对象不支持 get_order 方法，无法获取撤单所需信息。")
             return {"status": "error", "message": "Gateway does not support get_order"}
        except Exception as e:
            self.logger.exception(f"处理撤单指令时发生错误 (vt_orderid: {vt_orderid})")
            return {"status": "error", "message": f"Error processing cancel command: {e}"}

    def process_commands(self):
        """Runs in a separate thread to process incoming ZMQ commands (REP socket)."""
        self.logger.info("指令处理线程已启动。")
        while self.running:
            try:
                # Blocking receive on REP socket
                packed_request = self.command_socket.recv()
                if not self.running: # Check again after recv returns
                    break

                command_msg = msgpack.unpackb(packed_request, raw=False)
                command_type = command_msg.get('type', 'UNKNOWN')
                command_data = command_msg.get('data', {})
                self.logger.info(f"收到指令: Type={command_type}, Data={command_data}")

                reply_data = {"status": "error", "message": "Unknown command type"}
                if command_type == "CANCEL_ORDER":
                    reply_data = self._handle_cancel_order_command(command_data)
                # Add handlers for other command types here
                elif command_type == "PING":
                    self.logger.debug("Received PING, sending PONG") # Use debug level for frequent messages
                    reply_data = {"status": "ok", "reply": "PONG"}

                # Send reply back via REP socket
                packed_reply = msgpack.packb(reply_data, use_bin_type=True)
                self.command_socket.send(packed_reply)

            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM or not self.running:
                    self.logger.info("指令处理线程：ZMQ Context 已终止或服务停止。")
                    break # Exit loop cleanly
                else:
                     self.logger.error(f"指令处理线程 ZMQ 错误: {e}")
                     time.sleep(1)
            except msgpack.UnpackException as e:
                 self.logger.error(f"指令消息解码错误: {e}")
            except Exception as e:
                 self.logger.exception("处理指令时发生未知错误")
                 # Send generic error reply if possible
                 try:
                     err_reply = {"status": "error", "message": "Internal server error"}
                     self.command_socket.send(msgpack.packb(err_reply, use_bin_type=True))
                 except Exception as send_err:
                     self.logger.error(f"发送错误回复失败: {send_err}")
                 time.sleep(1)

        self.logger.info("指令处理线程已停止。")

    def process_order_requests(self):
        """Runs in a separate thread to process incoming ZMQ order requests."""
        self.logger.info("订单请求处理线程已启动。")
        while self.running:
            try:
                # Blocking recv on the PULL socket
                packed_request = self.order_puller.recv()
                if not self.running: # Check again after recv returns
                    break

                request_msg = msgpack.unpackb(packed_request, raw=False)
                request_data = request_msg.get('data')
                # self.logger.debug(f"收到订单请求消息: {request_data}") # Use debug for potentially large data
                self.logger.info(f"收到订单请求: {request_data.get('symbol')}, {request_data.get('direction')}, Vol:{request_data.get('volume')}") # Log key info

                if not request_data:
                    print("错误：收到的订单请求消息缺少 'data' 字段。")
                    self.logger.error("错误：收到的订单请求消息缺少 'data' 字段。")
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
                            self.logger.warning(f"合约 {vt_symbol} 尚未缓存，等待 {retry_delay} 秒后重试 ({i+1}/{max_retries})...")
                            time.sleep(retry_delay)
                            contract = self.contracts.get(vt_symbol)
                            if contract:
                                self.logger.info(f"信息: 重试成功，找到合约 {vt_symbol}。")
                                break # 找到合约，跳出重试循环
                        
                        # 如果重试后仍然找不到
                        if not contract:
                            print(f"错误: 尝试下单的合约 {vt_symbol} 信息不存在 (已重试 {max_retries} 次)。订单无法发送。")
                            self.logger.error(f"错误: 尝试下单的合约 {vt_symbol} 信息不存在 (已重试 {max_retries} 次)。订单无法发送。")
                            # 可以选择性地发送拒绝回报给策略
                            # self.send_rejection_report(order_request, "合约不存在")
                            continue # 跳过此订单
                    # +++ 结束检查 +++

                    vt_orderid = self.gateway.send_order(order_request)
                    if vt_orderid:
                        print(f"  订单请求已发送至 CTP 网关: {order_request.symbol}, 本地ID: {vt_orderid}")
                        self.logger.info(f"订单请求已发送至 CTP 网关: {order_request.symbol}, 本地ID: {vt_orderid}")
                    else:
                        print(f"  错误: CTP 网关未能发送订单请求: {order_request.symbol}")
                        self.logger.error(f"CTP 网关未能发送订单请求: {order_request.symbol}")
                        # Optionally publish a rejection status back?
                else:
                    print("  错误: 无法将收到的消息转换为有效的 OrderRequest。")
                    self.logger.error("无法将收到的消息转换为有效的 OrderRequest。")
                    # Optionally publish a rejection status back?

            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    print("订单请求线程：ZMQ Context 已终止。")
                    self.logger.info("订单请求线程：ZMQ Context 已终止。")
                    break # Exit loop cleanly
                else:
                     print(f"订单请求线程 ZMQ 错误: {e}")
                     self.logger.error(f"订单请求线程 ZMQ 错误: {e}")
                     time.sleep(1)
            except msgpack.UnpackException as e:
                 print(f"订单请求消息解码错误: {e}")
                 self.logger.error(f"订单请求消息解码错误: {e}")
            except Exception as e:
                 print(f"处理订单请求时发生未知错误: {e}")
                 self.logger.exception("处理订单请求时发生未知错误")
                 # Log traceback here in production
                 time.sleep(1)

        self.logger.info("订单请求处理线程已停止。")

    def start(self):
        """Starts the event engine, connects the gateway, and starts the request thread."""
        if self.running:
            self.logger.warning("订单执行网关服务已在运行中。")
            return

        self.logger.info("启动订单执行网关服务...")
        self.running = True

        # Start vnpy event engine
        self.event_engine.register(EVENT_ORDER, self.process_vnpy_event)
        self.event_engine.register(EVENT_TRADE, self.process_vnpy_event)
        self.event_engine.register(EVENT_LOG, self.process_vnpy_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_vnpy_event)
        self.event_engine.register(EVENT_ACCOUNT, self.process_vnpy_event) # Register for account events
        self.event_engine.start()
        print("事件引擎已启动。")
        self.logger.info("事件引擎已启动。")

        # Connect CTP gateway
        try:
            self.gateway.connect(self.ctp_setting)
            self.logger.info("CTP 交易网关连接请求已发送。等待连接成功...")
            # Ideally, wait for a connection success log/event
            time.sleep(10) # Simple wait
            print("CTP 交易网关应已连接。")
            self.logger.info("CTP 交易网关假定连接成功（基于延时）。") # Log assumption
        except Exception as e:
            print(f"连接 CTP 交易网关时发生严重错误: {e}")
            self.stop()
            return

        # Start the ZMQ request processing thread
        self.req_thread = threading.Thread(target=self.process_order_requests)
        self.req_thread.daemon = True # Allow main thread to exit even if this thread is running
        self.req_thread.start()

        # Start the command processing thread
        self.command_thread = threading.Thread(target=self.process_commands)
        self.command_thread.daemon = True
        self.command_thread.start()

        self.logger.info("订单执行网关服务启动完成。")

    def stop(self):
        """Stops the service and cleans up resources."""
        if not self.running:
            self.logger.warning("订单执行网关服务未运行。")
            return

        self.logger.info("停止订单执行网关服务...")
        self.running = False

        # Stop ZMQ request thread first
        # Send a dummy message or close socket to unblock recv?
        # Or rely on self.running flag check within the loop
        # Closing sockets before thread join might be cleaner

        if self.order_puller:
             self.order_puller.close()
             print("ZeroMQ 订单请求接收器已关闭。")
             self.logger.info("ZeroMQ 订单请求接收器已关闭。")
        if self.report_publisher:
             self.report_publisher.close()
             self.logger.info("ZeroMQ 订单/成交回报发布器已关闭。")
        if self.command_socket:
             self.command_socket.close()
             self.logger.info("ZeroMQ 指令接收器已关闭。")

        if self.req_thread and self.req_thread.is_alive():
            print("等待订单请求处理线程退出...")
            self.logger.info("等待订单请求处理线程退出...")
            self.req_thread.join(timeout=5) # Wait for thread to finish
            if self.req_thread.is_alive():
                print("警告：订单请求处理线程未在超时内退出。")
                self.logger.warning("警告：订单请求处理线程未在超时内退出。")

        if self.command_thread and self.command_thread.is_alive():
            self.logger.info("等待指令处理线程退出...")
            self.command_thread.join(timeout=5)
            if self.command_thread.is_alive():
                self.logger.warning("警告：指令处理线程未在超时内退出。")

        # Stop vnpy components
        try:
            if self.event_engine.is_active():
                self.event_engine.stop()
                print("事件引擎已停止。")
                self.logger.info("事件引擎已停止。")
        except AttributeError:
            if hasattr(self.event_engine, '_active') and self.event_engine._active:
                self.event_engine.stop()
                print("事件引擎已停止。")
                self.logger.info("事件引擎已停止。")

        if self.gateway:
            self.gateway.close()
            print("CTP 交易网关已关闭。")
            self.logger.info("CTP 交易网关已关闭。")

        if self.context:
             # Check if context is already terminated before terminating again
             # if not self.context.closed:
             #    self.context.term()
             # Simplified: assume term() is idempotent or handles errors
             try:
                 self.context.term()
                 print("ZeroMQ Context 已终止。")
                 self.logger.info("ZeroMQ Context 已终止。")
             except zmq.ZMQError as e:
                  print(f"终止 ZeroMQ Context 时出错 (可能已终止): {e}")
                  self.logger.error(f"终止 ZeroMQ Context 时出错 (可能已终止): {e}")

        self.logger.info("订单执行网关服务已停止。")

# --- Main execution block (for testing) ---
if __name__ == "__main__":
    # Setup logging for direct execution test
    try:
        from logger import setup_logging, getLogger
        setup_logging(service_name="OrderExecutionGateway_DirectRun")
    except ImportError as log_err:
        print(f"CRITICAL: Failed to import or setup logger: {log_err}. Exiting.")
        sys.exit(1)

    logger_main = getLogger(__name__)

    logger_main.info("Starting direct test run...")
    gw_service = OrderExecutionGatewayService()
    gw_service.start()

    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger_main.info("主程序接收到中断信号，正在停止...")
    except Exception as e:
        logger_main.exception("主测试循环发生未处理错误")
    finally:
        gw_service.stop()
        logger_main.info("订单执行网关测试运行结束。")
