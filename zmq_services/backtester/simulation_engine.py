from typing import Optional

import zmq
import pickle
import time
import sys
import os
import json
import msgpack # +++ Add msgpack import +++
from datetime import datetime, timedelta
import glob # +++ Add glob import +++

from utils.logger import logger, setup_logging, DEBUG, \
    INFO  # Assuming logger is configured elsewhere (e.g., run_backtest.py)

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')) # Go up two levels
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import necessary vnpy constants and objects
try:
    from vnpy.trader.constant import (
        Direction, OrderType, Exchange, Offset, Status
    )
    from vnpy.trader.object import OrderRequest, TickData, OrderData, TradeData
    VNPY_AVAILABLE = True
except ImportError:
    VNPY_AVAILABLE = False
    logger.error("CRITICAL: Failed to import vnpy objects/constants! Using dummy fallback classes. Backtest results may be inaccurate.")
    # Define fallbacks if needed (simplified)
    class Exchange:
        def __init__(self, value): self.value = value
    class TickData:
        def __init__(self, gateway_name, symbol, exchange, datetime, name, volume, turnover, open_interest, last_price, last_volume, limit_up, limit_down, open_price, high_price, low_price, pre_close, bid_price_1, bid_price_2, bid_price_3, bid_price_4, bid_price_5, ask_price_1, ask_price_2, ask_price_3, ask_price_4, ask_price_5, bid_volume_1, bid_volume_2, bid_volume_3, bid_volume_4, bid_volume_5, ask_volume_1, ask_volume_2, ask_volume_3, ask_volume_4, ask_volume_5, localtime=None):
            # Assign all fields, even if just to self
            self.gateway_name = gateway_name; self.symbol = symbol; self.exchange = exchange; self.datetime = datetime; self.name = name; self.volume = volume; self.turnover = turnover; self.open_interest = open_interest; self.last_price = last_price; self.last_volume = last_volume; self.limit_up = limit_up; self.limit_down = limit_down; self.open_price = open_price; self.high_price = high_price; self.low_price = low_price; self.pre_close = pre_close
            self.bid_price_1 = bid_price_1; self.bid_price_2 = bid_price_2; self.bid_price_3 = bid_price_3; self.bid_price_4 = bid_price_4; self.bid_price_5 = bid_price_5
            self.ask_price_1 = ask_price_1; self.ask_price_2 = ask_price_2; self.ask_price_3 = ask_price_3; self.ask_price_4 = ask_price_4; self.ask_price_5 = ask_price_5
            self.bid_volume_1 = bid_volume_1; self.bid_volume_2 = bid_volume_2; self.bid_volume_3 = bid_volume_3; self.bid_volume_4 = bid_volume_4; self.bid_volume_5 = bid_volume_5
            self.ask_volume_1 = ask_volume_1; self.ask_volume_2 = ask_volume_2; self.ask_volume_3 = ask_volume_3; self.ask_volume_4 = ask_volume_4; self.ask_volume_5 = ask_volume_5
            self.localtime = localtime
            self.vt_symbol = f"{symbol}.{exchange.value if hasattr(exchange, 'value') else exchange}"
    class OrderData: pass # Simplified
    class TradeData: pass # Simplified
    class Direction: LONG = "多"; SHORT = "空"
    class OrderType: LIMIT = "限价"; MARKET = "市价"; STOP = "STOP"
    class Offset: NONE = "无"; OPEN = "开"; CLOSE = "平"; CLOSETODAY="平今"; CLOSEYESTERDAY="平昨"
    class Status: SUBMITTING="提交中"; NOTTRADED="未成交"; PARTTRADED="部分成交"; ALLTRADED="全部成交"; CANCELLED="已撤销"; REJECTED="拒单"

# --- Serialization/Deserialization Helpers (Adapted from gateways) ---
# We need these to process incoming requests and format outgoing reports

# +++ Import the converter function +++
from utils.converter import convert_vnpy_obj_to_dict # UPDATED IMPORT

def create_tick_from_dict(tick_dict: dict) -> Optional[TickData]:
    vt_symbol = tick_dict.get('vt_symbol')
    if not vt_symbol or '.' not in vt_symbol:
        logger.error(f"Invalid or missing vt_symbol in tick dict: {tick_dict}")
        return None
    try:
        # --- Robust timestamp handling ---
        timestamp_ns = tick_dict.get('timestamp_ns') # From CTP Gateway
        if timestamp_ns is None:
            timestamp_ns = tick_dict.get('reception_timestamp_ns') # From DataRecorder
        if timestamp_ns is None:
            # Fallback for older data that might just have 'datetime' string
            dt_str = tick_dict.get('datetime')
            if dt_str:
                try:
                    dt_obj = datetime.fromisoformat(dt_str.replace("Z", "+00:00")) # Handle Z for UTC
                    timestamp_ns = int(dt_obj.timestamp() * 1_000_000_000)
                except ValueError:
                    logger.warning(f"Could not parse datetime string '{dt_str}' for {vt_symbol}. Using current time as fallback.")
                    timestamp_ns = int(time.time() * 1_000_000_000) # Last resort, less accurate
            else:
                logger.error(f"Missing timestamp (timestamp_ns, reception_timestamp_ns, or datetime) in tick dict for {vt_symbol}: {tick_dict}")
                return None
        # --- End Robust timestamp handling ---
        
        exchange_str = vt_symbol.split('.')[-1]
        exchange_obj = Exchange(exchange_str) if VNPY_AVAILABLE else exchange_str # Use string if vnpy not available

        tick = TickData(
            gateway_name=tick_dict.get('gateway_name', 'SIMULATOR'),
            symbol=vt_symbol.split('.')[0],
            exchange=exchange_obj,
            datetime=datetime.fromtimestamp(timestamp_ns / 1_000_000_000), 
            name=tick_dict.get('name', ''),
            volume=float(tick_dict.get('volume', 0.0)),
            turnover=float(tick_dict.get('turnover', 0.0)),
            open_interest=float(tick_dict.get('open_interest', 0.0)),
            last_price=float(tick_dict.get('last_price', 0.0)),
            last_volume=float(tick_dict.get('last_volume', 0.0)),
            limit_up=float(tick_dict.get('limit_up', 0.0)),
            limit_down=float(tick_dict.get('limit_down', 0.0)),
            open_price=float(tick_dict.get('open_price', 0.0)),
            high_price=float(tick_dict.get('high_price', 0.0)),
            low_price=float(tick_dict.get('low_price', 0.0)),
            pre_close=float(tick_dict.get('pre_close', 0.0)),
            bid_price_1=float(tick_dict.get('bid_price_1', 0.0)),
            bid_price_2=float(tick_dict.get('bid_price_2',0.0)),
            bid_price_3=float(tick_dict.get('bid_price_3',0.0)),
            bid_price_4=float(tick_dict.get('bid_price_4',0.0)),
            bid_price_5=float(tick_dict.get('bid_price_5',0.0)),
            ask_price_1=float(tick_dict.get('ask_price_1', 0.0)),
            ask_price_2=float(tick_dict.get('ask_price_2',0.0)),
            ask_price_3=float(tick_dict.get('ask_price_3',0.0)),
            ask_price_4=float(tick_dict.get('ask_price_4',0.0)),
            ask_price_5=float(tick_dict.get('ask_price_5',0.0)),
            bid_volume_1=float(tick_dict.get('bid_volume_1',0.0)),
            bid_volume_2=float(tick_dict.get('bid_volume_2',0.0)),
            bid_volume_3=float(tick_dict.get('bid_volume_3',0.0)),
            bid_volume_4=float(tick_dict.get('bid_volume_4',0.0)),
            bid_volume_5=float(tick_dict.get('bid_volume_5',0.0)),
            ask_volume_1=float(tick_dict.get('ask_volume_1',0.0)),
            ask_volume_2=float(tick_dict.get('ask_volume_2',0.0)),
            ask_volume_3=float(tick_dict.get('ask_volume_3',0.0)),
            ask_volume_4=float(tick_dict.get('ask_volume_4',0.0)),
            ask_volume_5=float(tick_dict.get('ask_volume_5',0.0)),
            localtime=tick_dict.get('localtime')
        )
        # vt_symbol might be re-assigned by TickData constructor if VNPY_AVAILABLE
        if not VNPY_AVAILABLE or not hasattr(tick, 'vt_symbol') or not tick.vt_symbol:
            tick.vt_symbol = vt_symbol 
        return tick
    except (ValueError, TypeError) as e:
        logger.error(f"Error creating TickData from dict for {vt_symbol}: {e}. Dict: {tick_dict}")
        return None

# --- Simulation Engine Service ---
class SimulationEngineService:
    def __init__(self, data_source_path: str, backtest_md_pub_url: str,
                 backtest_report_pub_url: str, backtest_order_pull_url: str,
                 date_str: str,
                 commission_rules: dict,
                 contract_multipliers: dict,
                 slippage: float = 0.0,
                 strategy_symbols: Optional[list[str]] = None # +++ Add strategy_symbols +++
                 ):
        # Setup logging
        setup_logging(service_name=__class__.__name__, level=INFO)
        """Initializes the Simulation Engine service."""
        self.context = zmq.Context()
        self.md_publisher = self.context.socket(zmq.PUB)
        self.md_publisher.bind(backtest_md_pub_url)
        logger.info(f"模拟行情发布器绑定到: {backtest_md_pub_url}")
        self.report_publisher = self.context.socket(zmq.PUB)
        self.report_publisher.bind(backtest_report_pub_url)
        logger.info(f"模拟回报发布器绑定到: {backtest_report_pub_url}")
        self.order_puller = self.context.socket(zmq.PULL)
        self.order_puller.bind(backtest_order_pull_url)
        logger.info(f"模拟订单接收器绑定到: {backtest_order_pull_url}")

        self.data_source_path = data_source_path
        self.date_str = date_str
        self.commission_rules = commission_rules
        self.contract_multipliers = contract_multipliers
        self.slippage = slippage
        self.strategy_symbols = set(strategy_symbols) if strategy_symbols else set() # +++ Store strategy_symbols +++
        if self.strategy_symbols:
            logger.info(f"模拟引擎将只加载和处理以下合约的Tick数据: {self.strategy_symbols}")
        else:
            logger.info("模拟引擎将尝试加载所有合约的Tick数据 (未指定 strategy_symbols)。")

        logger.debug(f"手续费规则: {self.commission_rules}")
        logger.debug(f"合约乘数: {self.contract_multipliers}")
        logger.info(f"固定滑点设置为: {self.slippage}")

        self.all_ticks = []
        self.current_tick_message_dict = None
        self.current_tick_time_ns = 0
        self.active_orders = {}
        self.local_order_id_counter = 0
        self.trade_id_counter = 0
        self.running = False
        self.unique_topics_loaded = set()

    def load_data(self) -> bool:
        file_pattern = os.path.join(self.data_source_path, f"ticks_*_{self.date_str}.jsonl")
        tick_files = glob.glob(file_pattern)
        logger.info(f"模拟引擎: 查找 Ticks 数据文件，模式: {file_pattern}")
        if not tick_files:
            logger.error(f"错误: 未找到匹配模式的数据文件: {file_pattern}")
            return False

        logger.info(f"找到 {len(tick_files)} 个 Ticks 数据文件: {tick_files}")
        self.all_ticks = []
        self.unique_topics_loaded = set()
        topics_to_log = 10 # Log more topics if needed
        loaded_count = 0
        total_lines_processed = 0
        skipped_records = 0
        filtered_out_by_symbol = 0

        for tick_file in tick_files:
            filename_short = os.path.basename(tick_file)
            # +++ Filter files by strategy_symbols if provided +++
            if self.strategy_symbols:
                try:
                    parts = filename_short.split('_')
                    # Expecting ticks_SYMBOL_EXCHANGE_DATE.jsonl
                    if len(parts) >= 3:
                        # Construct vt_symbol from filename like SYMBOL.EXCHANGE
                        file_vt_symbol = f"{parts[1]}.{parts[2]}"
                        if file_vt_symbol not in self.strategy_symbols:
                            logger.info(f"  跳过文件 {filename_short} (合约 {file_vt_symbol} 不在策略所需合约列表 {self.strategy_symbols} 中)。")
                            continue
                        else:
                            logger.info(f"  文件名合约 {file_vt_symbol} 在策略所需列表 {self.strategy_symbols} 中，准备加载文件 {filename_short}。")
                    else:
                        logger.warning(f"  文件名 {filename_short} 格式不符合预期 (ticks_SYMBOL_EXCHANGE_DATE.jsonl)，无法按合约过滤，将尝试加载。")
                except IndexError:
                    logger.warning(f"  无法从文件名 {filename_short} 解析合约代码 (IndexError)，将尝试加载。")
            # --- End Filter --- 
            
            logger.info(f"  正在加载文件: {filename_short}")
            file_line_count = 0
            try:
                with open(tick_file, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        total_lines_processed += 1
                        file_line_count = line_num
                        try:
                            record = json.loads(line.strip())
                            data_dict = record.get("data")
                            reception_ts_ns = record.get("reception_timestamp_ns")
                            topic_str = record.get("zmq_topic")

                            if not isinstance(data_dict, dict) or not reception_ts_ns or not topic_str or not topic_str.startswith("tick."):
                                skipped_records += 1
                                continue
                            
                            # --- Filter individual ticks by symbol if strategy_symbols is restrictive ---
                            # This is a second layer of filtering, useful if a file contains multiple symbols (though current naming suggest one per file)
                            current_tick_vt_symbol = data_dict.get('vt_symbol')
                            if self.strategy_symbols and current_tick_vt_symbol and current_tick_vt_symbol not in self.strategy_symbols:
                                filtered_out_by_symbol +=1
                                continue
                            # --- End Filter --- 

                            if topic_str not in self.unique_topics_loaded and len(self.unique_topics_loaded) < topics_to_log:
                                self.unique_topics_loaded.add(topic_str)
                                logger.debug(f"Found unique ZMQ topic in data file: '{topic_str}'")
                            
                            self.all_ticks.append((reception_ts_ns, topic_str, data_dict))
                            loaded_count += 1

                        except json.JSONDecodeError as e:
                            logger.warning(f"[{filename_short}:{line_num}] 解析 JSON 行时出错: {e}. 行: {line.strip()}")
                            skipped_records += 1
                        except Exception as e_line:
                            logger.warning(f"[{filename_short}:{line_num}] 处理记录时发生未知错误: {e_line}. 记录: {record}")
                            skipped_records += 1
            except IOError as e_io:
                logger.error(f"读取文件 {filename_short} 时出错: {e_io}")
                logger.warning(f"跳过文件 {filename_short}，继续处理其他文件...")
                continue 
            except Exception as e_file:
                logger.exception(f"加载文件 {filename_short} 时发生未知错误: {e_file}")
                logger.warning(f"跳过文件 {filename_short}，继续处理其他文件...")
                continue 
            logger.info(f"  文件 {filename_short} 处理完成 ({file_line_count} 行).")

        if not self.all_ticks:
             logger.error(f"错误：未能从任何匹配的文件中加载任何有效的 Tick 数据 (模式: {file_pattern}, 共处理 {total_lines_processed} 行, 跳过 {skipped_records} 条, 因合约过滤 {filtered_out_by_symbol} 条)。")
             return False

        logger.info("所有文件加载完成，正在按接收时间戳排序...")
        self.all_ticks.sort(key=lambda x: x[0]) 
        logger.info(f"数据加载完成并排序。总共 {loaded_count} 条有效 Tick 数据 (来自 {len(tick_files)} 个文件, 共处理 {total_lines_processed} 行, 跳过 {skipped_records} 条, 因合约过滤 {filtered_out_by_symbol} 条)。")
        logger.info(f"加载完成。从数据文件中找到 {len(self.unique_topics_loaded)} 个唯一 ZMQ Tick 主题 (最多记录 {topics_to_log} 个)。")
        return True

    def publish_tick(self, topic_str: str, inner_tick_dict: dict, processed_count: int):
        """Converts inner tick dict to TickData object and publishes msgpacked dict."""
        if not topic_str:
             logger.warning(f"Attempted to publish tick with empty topic: {inner_tick_dict}")
             return
        
        # --- Create TickData object FROM the inner_tick_dict directly --- 
        tick_object = create_tick_from_dict(inner_tick_dict) # Pass inner_tick_dict directly
        
        if not tick_object:
            # Error logged in create_tick_from_dict
            logger.warning(f"Skipping publish due to TickData creation failure. Inner Dict: {inner_tick_dict}")
            return

        try:
            topic = topic_str.encode('utf-8')
            dict_data = convert_vnpy_obj_to_dict(tick_object) # Convert object back to dict for publishing
            packed_message = msgpack.packb(dict_data, use_bin_type=True)
            
            if processed_count % 5000 == 1: 
                 logger.debug(f"Publishing tick #{processed_count} with topic: '{topic_str}'")
            
            self.md_publisher.send_multipart([topic, packed_message])
        except (msgpack.PackException, TypeError) as e_msgpack:
             logger.error(f"Msgpack序列化Tick字典时出错 (Topic: {topic_str}): {e_msgpack}")
        except Exception as e:
            logger.error(f"发布模拟 Tick (Topic: {topic_str}) 时出错: {e}") 

    def check_for_new_orders(self):
        """Non-blockingly checks for and processes new order requests."""
        while True: 
            try:
                # --- Receive msgpack request bytes --- 
                packed_request = self.order_puller.recv(flags=zmq.NOBLOCK)
                # +++ 添加日志：记录收到的原始字节 +++
                # logger.debug(f"[SimEngine CheckOrders] Received raw bytes: {packed_request!r}") # <-- 注释掉此行
                # +++ 结束添加 +++

                # --- Deserialize using msgpack --- 
                request_data = msgpack.unpackb(packed_request, raw=False)
                # --- End Receive and Deserialize --- 

                # +++ Add DEBUG log after receiving +++
                logger.debug(f"check_for_new_orders: Received potential request data: {request_data}")
                # +++ End Add +++

                # --- Differentiate request type (order submission tuple vs cancel dict) ---
                if isinstance(request_data, (list, tuple)) and len(request_data) == 3 and request_data[0] == "send_order":
                    # --- Handle Order Submission --- 
                    args_part = request_data[1]
                    if args_part and isinstance(args_part, (list, tuple)) and len(args_part) > 0 and isinstance(args_part[0], dict):
                        order_req_dict = args_part[0]
                        logger.debug(f"check_for_new_orders: Valid send_order request dict extracted: {order_req_dict}")
                        
                        # Process the order request dict
                        self.local_order_id_counter += 1
                        local_order_id = f"sim_{self.local_order_id_counter}"
                        vt_orderid = f"SIM.{local_order_id}"
                        
                        # Validate required fields (simple check)
                        required_fields = ['symbol', 'exchange', 'direction', 'type', 'volume']
                        if not all(field in order_req_dict for field in required_fields):
                            missing = [f for f in required_fields if f not in order_req_dict]
                            logger.error(f"订单请求字典缺少字段: {missing}. Dict: {order_req_dict}")
                            # Optionally send REJECTED report here? For now, just continue.
                            continue
                        
                        # Store the order data internally
                        internal_order_data = order_req_dict.copy()
                        internal_order_data['local_order_id'] = local_order_id
                        internal_order_data['vt_orderid'] = vt_orderid
                        internal_order_data['order_time_ns'] = self.current_tick_time_ns
                        internal_order_data['traded'] = 0.0 # Initialize traded volume
                        internal_order_data['status'] = Status.SUBMITTING # Initial status

                        self.active_orders[local_order_id] = internal_order_data
                        # +++ 添加日志：确认订单已加入 active_orders +++
                        logger.debug(f"[SimEngine CheckOrders] Order {local_order_id} ({internal_order_data.get('direction')} {internal_order_data.get('price')}) added to active_orders. Current active_orders: {self.active_orders}")
                        # +++ 结束添加 +++

                        # REMOVED direct log: logger.info(f"订单 {local_order_id} 已接受并激活。VTOrderID: {vt_orderid}")
                        logger.info(f"模拟引擎: 接收到订单请求，分配本地 ID: {local_order_id}, VTOrderID: {vt_orderid}")
                        

                        # Publish SUBMITTING and NOTTRADED status updates
                        logger.debug(f"SimEngine: About to publish initial order reports for order {local_order_id}, vt_orderid {vt_orderid}")
                        
                        self.generate_and_publish_order_report(internal_order_data, Status.SUBMITTING)
                        
                        # Update status for next report before publishing NOTTRADED
                        internal_order_data_nottraded = internal_order_data.copy() # Create copy to avoid race condition if matching happens instantly
                        internal_order_data_nottraded['status'] = Status.NOTTRADED
                        # +++ Update the status IN THE STORED DICTIONARY +++
                        self.active_orders[local_order_id]['status'] = Status.NOTTRADED
                        logger.debug(f"SimEngine: Updated internal status for {local_order_id} to NOTTRADED")
                        # +++ End Update +++
                        
                        self.generate_and_publish_order_report(internal_order_data_nottraded, Status.NOTTRADED)
                        
                    
                    else:
                        logger.error(f"收到的 send_order 请求格式无效 (args part 无效): {request_data}")
                        continue
                    # --- End Handle Order Submission --- 
                    
                elif isinstance(request_data, dict) and request_data.get("action") == "cancel":
                    # --- Handle Cancel Request --- 
                    vt_orderid_to_cancel = request_data.get("vt_orderid")
                    logger.debug(f"check_for_new_orders: Received cancel request for VTOrderID: {vt_orderid_to_cancel}")
                    if not vt_orderid_to_cancel or not vt_orderid_to_cancel.startswith("SIM."):
                        logger.error(f"收到的撤单请求缺少有效的 VTOrderID (应以 SIM. 开头): {request_data}")
                        continue
                        
                    local_id_to_cancel = vt_orderid_to_cancel.split('.')[1] # Extract sim_X
                    
                    # Find the order in active orders
                    if local_id_to_cancel in self.active_orders:
                        order_to_cancel = self.active_orders.pop(local_id_to_cancel) # Remove from active map
                        logger.info(f"模拟引擎: 处理撤单请求，找到并移除活动订单 {local_id_to_cancel} (VTOrderID: {vt_orderid_to_cancel})")
                        # Publish CANCELLED status
                        # Update the status in the dict before publishing
                        order_to_cancel_report = order_to_cancel.copy()
                        order_to_cancel_report['status'] = Status.CANCELLED
                        self.generate_and_publish_order_report(order_to_cancel_report, Status.CANCELLED)
                    else:
                        logger.warning(f"收到的撤单请求针对无效或已完成的订单 VTOrderID: {vt_orderid_to_cancel} (Local ID: {local_id_to_cancel})，忽略。")
                    # --- End Handle Cancel Request --- 
                    
                else:
                    logger.error(f"收到未知的订单请求格式: {request_data}")
                    continue

            except zmq.Again:
                break # No more messages waiting
            except pickle.UnpicklingError as e: # Should not happen with msgpack
                logger.error(f"反序列化订单请求时出错 (Pickle?): {e}")
            except (msgpack.UnpackException, TypeError, ValueError) as e_unpack:
                 logger.error(f"Msgpack反序列化订单请求时出错: {e_unpack}")
            except Exception as e:
                logger.exception(f"处理订单请求时发生未知错误: {e}")

    def match_orders(self):
        """Attempts to match active orders against the current tick (using dict)."""
        # --- FIX: Use self.current_tick_message_dict directly --- 
        if not self.current_tick_message_dict: # Check if the dictionary itself is valid
             logger.warning("match_orders called but self.current_tick_message_dict is None or empty.")
             return
        tick_data = self.current_tick_message_dict # It already holds the tick data payload
        # --- END FIX ---
        
        ask_price_raw = tick_data.get('ask_price_1') # Get raw value
        bid_price_raw = tick_data.get('bid_price_1') # Get raw value
        tick_symbol = tick_data.get('vt_symbol')

        ask_price, bid_price = None, None # Initialize to None
        try:
            if ask_price_raw is not None:
                ask_price = float(ask_price_raw) # Convert to float
            if bid_price_raw is not None:
                bid_price = float(bid_price_raw) # Convert to float
        except (ValueError, TypeError) as e:
            logger.error(f"Error converting raw prices to float for tick {tick_symbol}. AskRaw={ask_price_raw}, BidRaw={bid_price_raw}. Error: {e}")
            return # Exit if conversion fails
        # logger.debug(f"---> Converted prices for {tick_symbol}: Ask={ask_price}, Bid={bid_price}") # <-- 注释掉此行

        # 使用纳秒时间戳创建更精确的 datetime 对象 - 用于内部逻辑和回报
        sim_time_dt = datetime.fromtimestamp(self.current_tick_time_ns / 1_000_000_000)
        # 创建用于日志打印的毫秒级时间字符串
        sim_time_str = sim_time_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        # Check using the converted float prices
        if ask_price is None or bid_price is None or ask_price <= 0 or bid_price <= 0:
            logger.debug(f"--- EXIT match_orders @ {sim_time_str} (Invalid/Zero Price after conversion for {tick_symbol}: Bid={bid_price}, Ask={ask_price}) ---")
            return # 无有效价格，无法撮合

        # +++ 只在有活动订单时才打印循环启动日志 +++
        if self.active_orders:
            # logger.debug(f"[SimEngine MatchOrders Start Loop] Current active orders before loop: {self.active_orders}") # <-- 注释掉
            # logger.debug(f"---> Prices for {tick_symbol} are valid, entering matching loop...") # <-- 注释掉
            pass # 保留 if 块结构，如果以后需要在这里添加条件日志
        # +++ 结束条件判断 +++

        orders_to_remove = []
        # +++ 在循环内部处理订单日志 +++
        for local_id, order_data in list(self.active_orders.items()):
            # logger.debug(f"---> [Match Loop] Processing order: {local_id}, data: {order_data}") # <-- 注释掉
            order_vt_symbol = f"{(order_data.get('symbol'))}.{(order_data.get('exchange'))}"
            if order_vt_symbol != tick_symbol:
                continue

            logger.debug(f"[{sim_time_str}] 撮合检查: 订单 {local_id} ({order_data.get('direction')} {order_data.get('type')} @ {order_data.get('price')}) vs Tick {tick_symbol} (Bid: {bid_price}, Ask: {ask_price})")

            trade_price = 0
            matched = False
            match_condition = None

            # logger.debug(f"---> [Match Loop] Checking direction for order {local_id}: '{order_data['direction']}'") # <-- 注释掉
            if order_data['direction'] == Direction.LONG.value:
                # logger.debug(f"---> [Match Loop] Checking type for LONG order {local_id}: '{order_data['type']}'") # <-- 注释掉
                if order_data['type'] == OrderType.LIMIT.value:
                    if ask_price is not None and ask_price > 0:
                        match_condition = order_data['price'] >= ask_price
                        # logger.debug(f"---> [Match Loop {local_id}] BUY match_condition = {match_condition}") # <-- 注释掉
                        if match_condition:
                            trade_price = ask_price
                            matched = True
                elif order_data['type'] == OrderType.MARKET.value:
                    if ask_price is not None and ask_price > 0:
                        match_condition = True
                        # logger.debug(f"---> [Match Loop {local_id}] BUY MARKET match_condition = {match_condition}") # <-- 注释掉
                        trade_price = ask_price
                        matched = True
            elif order_data['direction'] == Direction.SHORT.value:
                # logger.debug(f"---> [Match Loop] Checking type for SHORT order {local_id}: '{order_data['type']}'") # <-- 注释掉
                if order_data['type'] == OrderType.LIMIT.value:
                    if bid_price is not None and bid_price > 0:
                        match_condition = order_data['price'] <= bid_price
                        # logger.debug(f"---> [Match Loop {local_id}] SELL match_condition = {match_condition}") # <-- 注释掉
                        if match_condition:
                            trade_price = bid_price
                            matched = True
                            # logger.debug(f"[MATCH_DEBUG] SELL Order {local_id} MATCHED at theoretical price {trade_price}") # 保留这个，因为它比较特定
                elif order_data['type'] == OrderType.MARKET.value:
                    if bid_price is not None and bid_price > 0:
                        match_condition = True
                        # logger.debug(f"---> [Match Loop {local_id}] SELL MARKET match_condition = {match_condition}") # <-- 注释掉
                        trade_price = bid_price
                        matched = True

            if matched:
                # logger.debug(f"---> [Match Loop {local_id}] Order matched! Entering matched block.") # <-- 注释掉
                logger.debug(f"SimEngine: Order {local_id} matched! vt_symbol={tick_symbol}, direction={order_data['direction']}, order_price={order_data['price']}, order_type={order_data['type']}, tick_ask={ask_price}, tick_bid={bid_price}, trade_price_before_slippage={trade_price}")
                logger.info(f"[{sim_time_str}] 订单 {local_id} 匹配成功! 理论成交价: {trade_price:.2f}")
                self.trade_id_counter += 1
                self.trade_id_counter += 1
                trade_id = f"simtrade_{self.trade_id_counter}"
                vt_tradeid = f"SIM.{trade_id}"

                # --- 如果匹配成功，应用滑点并生成回报 ---
                if self.slippage != 0: # 检查滑点是否非零
                    # +++ 在应用滑点前记录原始价格 +++
                    original_trade_price = trade_price
                    # +++ 结束记录 +++
                    if order_data['direction'] == Direction.LONG.value:
                        trade_price += self.slippage
                    elif order_data['direction'] == Direction.SHORT.value:
                        trade_price -= self.slippage
                        # 确保价格不会因为滑点变成负数
                        trade_price = max(trade_price, 0.0)
                    # --- 修正日志打印 --- 
                    logger.info(f"    应用滑点: 方向={order_data['direction']}, 滑点值={self.slippage}, 理论价={original_trade_price:.2f} => 实际价={trade_price:.2f}") # Use logger
                    # --- 结束修正 ---
                # +++ 结束应用 +++

                trade_report_dict = {
                    "gateway_name": "SIMULATOR",
                    "symbol": order_data['symbol'],
                    "exchange": order_data['exchange'],
                    "vt_symbol": tick_symbol,
                    "tradeid": trade_id,
                    "vt_tradeid": vt_tradeid,
                    "orderid": order_data['local_order_id'],
                    "vt_orderid": order_data['vt_orderid'],
                    "direction": order_data['direction'],
                    "offset": order_data['offset'],
                    "price": trade_price,
                    "volume": order_data['volume'],
                    "trade_time": sim_time_dt.isoformat(),
                    "datetime": sim_time_dt
                }
                self.generate_and_publish_trade_report(trade_report_dict)
                self.generate_and_publish_order_report(order_data, Status.ALLTRADED, traded_volume=order_data['volume'])
                orders_to_remove.append(local_id)

        for local_id in orders_to_remove:
            try:
                del self.active_orders[local_id]
                # --- 确保日志有时间戳 ---
                logger.info(f"[{sim_time_str}] 订单 {local_id} 已完成并从活动列表移除。") # Use logger
                # --- 结束确保 ---
            except KeyError:
                 logger.warning(f"尝试移除订单 {local_id} 时未找到。") # Use logger

    def generate_and_publish_order_report(self, order_report_dict: dict, status: Status, traded_volume: float = 0):
        """Generates OrderData object from dict and publishes msgpacked dict."""
        
        
        vt_symbol = order_report_dict.get('vt_symbol', f"{(order_report_dict.get('symbol', '' ))}.{(order_report_dict.get('exchange', '' ))}")
        vt_orderid = order_report_dict.get('vt_orderid')
        
        if not vt_orderid:
             logger.error(f"generate_and_publish_order_report 缺少 vt_orderid: {order_report_dict}")
             
             return
        
        # --- Create OrderData object --- 
        try:
             
             # Use order time from original request if available
             order_time_ns = order_report_dict.get('order_time_ns', self.current_tick_time_ns)
             order_time_dt = datetime.fromtimestamp(order_time_ns / 1_000_000_000)
             report_time_dt = datetime.fromtimestamp(self.current_tick_time_ns / 1_000_000_000)

             # Make sure status in the dict matches the status being reported
             order_report_dict['status'] = status 

             order_object = OrderData(
                 gateway_name="SIMULATOR",
                 symbol=order_report_dict['symbol'],
                 exchange=Exchange(order_report_dict['exchange']),
                 orderid=order_report_dict['local_order_id'],
                 direction=Direction(order_report_dict['direction']),
                 offset=Offset(order_report_dict.get('offset', Offset.NONE.value)),
                 type=OrderType(order_report_dict['type']),
                 price=float(order_report_dict.get('price', 0.0)),
                 volume=float(order_report_dict['volume']),
                 traded=float(traded_volume), # Use passed traded_volume
                 status=status, # Use the passed status
                 datetime=report_time_dt # Use the datetime object for report time
             )
             order_object.vt_symbol = vt_symbol # Ensure vt_symbol is set
             order_object.vt_orderid = vt_orderid
             
        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Error creating OrderData object from dict for {vt_orderid}: {e}. Dict: {order_report_dict}")
            
            return
        # --- End Create --- 

        # --- Change Topic to use vt_orderid ---
        topic_str = f"order.{vt_orderid}" 
        
        # --- End Change Topic ---
        try:
            topic = topic_str.encode('utf-8')
            # --- Convert object to dict THEN msgpack --- 
            dict_data = convert_vnpy_obj_to_dict(order_object)
            
            # +++ Add DEBUG log before sending order report +++
            logger.debug(f"SimEngine: Publishing Order Report - Topic: {topic_str}, Data: {{'vt_orderid': {dict_data.get('vt_orderid')}, 'status': {dict_data.get('status')}, 'traded': {dict_data.get('traded')}, 'price': {dict_data.get('price')}}}")
            
            packed_message = msgpack.packb(dict_data, use_bin_type=True)
            self.report_publisher.send_multipart([topic, packed_message])
            
            # --- End Convert and Pack ---
            logger.debug(f"Published order report: Topic={topic_str}, Status={status.value}")
        except (msgpack.PackException, TypeError) as e_msgpack:
             logger.error(f"Msgpack序列化Order字典时出错 (Status: {status.value}, Order: {vt_orderid}): {e_msgpack}")
        except Exception as e:
            sim_time_str = datetime.fromtimestamp(self.current_tick_time_ns / 1_000_000_000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            logger.error(f"[{sim_time_str}] 发布订单回报 (状态: {status.value}, 订单: {vt_orderid}) 时出错: {e}") 

    def generate_and_publish_trade_report(self, trade_report_dict: dict):
        """Calculates commission, generates TradeData object, and publishes msgpacked dict."""
        vt_symbol = trade_report_dict["vt_symbol"]
        symbol = trade_report_dict["symbol"]
        price = trade_report_dict["price"]
        volume = trade_report_dict["volume"]
        offset_val = trade_report_dict["offset"] # Get offset value

        # --- Calculate commission (logic remains same, using dict) --- 
        commission = 0.0
        rules = self.commission_rules.get(symbol)
        multiplier = self.contract_multipliers.get(symbol)

        if rules and multiplier and multiplier > 0:
            rate = 0.0
            fixed = 0.0
            min_comm = rules.get('min_commission', 0.0)

            # 根据开平标志选择费率或固定金额
            # 注意：直接比较从 order_data 传来的字符串值
            if offset_val == Offset.OPEN.value:
                rate = rules.get('open_rate', 0.0)
                fixed = rules.get('open_fixed', 0.0)
            elif offset_val in [Offset.CLOSE.value, Offset.CLOSETODAY.value, Offset.CLOSEYESTERDAY.value]:
                rate = rules.get('close_rate', 0.0)
                fixed = rules.get('close_fixed', 0.0)
            else:
                 logger.warning(f"警告: 未知的开平标志 '{offset_val}'，手续费按开仓计算 (如果规则存在)。")
                 rate = rules.get('open_rate', 0.0) # 或提供一个默认处理
                 fixed = rules.get('open_fixed', 0.0)

            # 优先使用费率计算
            if rate > 0:
                commission = price * volume * multiplier * rate
            # 否则，如果设置了固定费用，则使用固定费用
            elif fixed > 0:
                commission = volume * fixed

            # 应用最低手续费
            if min_comm > 0:
                commission = max(commission, min_comm)

            logger.debug(f"计算手续费: 合约={symbol}, 价格={price}, 量={volume}, 乘数={multiplier}, 开平={offset_val}, 费率={rate}, 固定={fixed}, 最低={min_comm} => 手续费={commission:.2f}") # Use logger.debug

        elif not rules:
             logger.warning(f"警告: 未找到合约 {symbol} 的手续费规则，手续费计为 0。") # Use logger
        elif not multiplier or multiplier <= 0:
             logger.warning(f"警告: 未找到或无效的合约 {symbol} 乘数 ({multiplier})，手续费计为 0。") # Use logger
        # +++ 结束计算 +++

        # 将手续费添加到回报数据中
        trade_report_dict['commission'] = commission

        # --- Create TradeData object (Ensure vt_orderid is included) --- 
        vt_tradeid = trade_report_dict.get('vt_tradeid')
        vt_orderid = trade_report_dict.get('vt_orderid') # Get vt_orderid from the dict
        if not vt_orderid:
            logger.error(f"generate_and_publish_trade_report 缺少 vt_orderid: {trade_report_dict}")
            return # Cannot create TradeData without vt_orderid link

        try:
             trade_time_dt = trade_report_dict.get("datetime")
             if not isinstance(trade_time_dt, datetime):
                 trade_time_str = trade_report_dict.get("trade_time")
                 if trade_time_str:
                      trade_time_dt = datetime.fromisoformat(trade_time_str)
                 else:
                      trade_time_dt = datetime.fromtimestamp(self.current_tick_time_ns / 1_000_000_000)
             
             trade_object = TradeData(
                 gateway_name="SIMULATOR",
                 symbol=symbol,
                 exchange=Exchange(trade_report_dict['exchange']),
                 orderid=trade_report_dict['orderid'], # local_order_id
                 tradeid=trade_report_dict['tradeid'], # local_trade_id
                 direction=Direction(trade_report_dict['direction']),
                 offset=Offset(offset_val),
                 price=float(price),
                 volume=float(volume),
                 datetime=trade_time_dt # Use datetime object
             )
             trade_object.vt_symbol = vt_symbol 
             trade_object.vt_orderid = vt_orderid # Assign the correct vt_orderid
             trade_object.vt_tradeid = vt_tradeid
             trade_object.commission = float(commission)
        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Error creating TradeData object from dict for {vt_tradeid}: {e}. Dict: {trade_report_dict}")
            return
        # --- End Create --- 

        topic_str = f"trade.{vt_symbol}" # Use generic trade topic
        try:
            topic = topic_str.encode('utf-8')
            # --- Convert object to dict THEN msgpack --- 
            dict_data = convert_vnpy_obj_to_dict(trade_object)
            # +++ Add DEBUG log before sending trade report +++
            logger.debug(f"SimEngine: Publishing Trade Report - Topic: {topic_str}, Data: {{'vt_tradeid': {dict_data.get('vt_tradeid')}, 'vt_orderid': {dict_data.get('vt_orderid')}, 'price': {dict_data.get('price')}, 'volume': {dict_data.get('volume')}}}")
            # +++ End Add +++
            packed_message = msgpack.packb(dict_data, use_bin_type=True)
            # --- End Convert and Pack ---
            self.report_publisher.send_multipart([topic, packed_message])
            logger.debug(f"Published trade report: Topic={topic_str}, VTTradeID={vt_tradeid}")
        except (msgpack.PackException, TypeError) as e_msgpack:
             logger.error(f"Msgpack序列化Trade字典时出错 (Trade: {vt_tradeid}): {e_msgpack}")
        except Exception as e:
             logger.error(f"发布成交回报 (Trade: {vt_tradeid}) 时出错: {e}")
    # --- End Modification ---

    def run_simulation(self):
        """Runs the main simulation loop."""
        if not self.all_ticks:
            logger.error("没有数据可供模拟。请先调用 load_data()。") # Use logger
            return

        logger.info(f"开始模拟回测 {self.date_str}...") # Use logger
        self.running = True
        processed_count = 0

        start_sim_time = time.time() # Wall clock time

        # +++ Add delay for subscriber connection +++
        initial_delay_s = 0.5 # Give subscriber 500ms to connect
        logger.info(f"等待 {initial_delay_s} 秒以确保订阅者连接...")
        time.sleep(initial_delay_s)
        # +++ End Add delay +++

        # --- Loop unpacks dict, publish calls pickle --- 
        for ts_ns, topic_str, tick_msg_dict in self.all_ticks:
            if not self.running:
                break

            self.current_tick_time_ns = ts_ns
            self.current_tick_message_dict = tick_msg_dict # Store the dict
            loop_start_time = time.perf_counter_ns()

            # 1. Publish current tick (Pass processed_count)
            publish_start = time.perf_counter_ns()
            self.publish_tick(topic_str, tick_msg_dict, processed_count)
            publish_end = time.perf_counter_ns()

            # 2. Check for new orders (receives pickled tuple)
            check_order_start = time.perf_counter_ns()
            self.check_for_new_orders()
            check_order_end = time.perf_counter_ns()

            # +++ Add a FORCED INFO log right before the DEBUG log +++
            logger.debug("SIM_ENGINE_LOOP: Immediately before DEBUG log for active_orders.")
            # +++ End Add FORCED INFO log +++

            # +++ Add DEBUG log for active_orders before matching +++
            logger.debug(f"SimEngine Loop: Before match_orders. Active orders: {self.active_orders}")
            # +++ End Add +++

            # 3. Attempt to match active orders (uses dict)
            match_start = time.perf_counter_ns()
            self.match_orders() # Match logic uses dicts
            match_end = time.perf_counter_ns()

            loop_end_time = time.perf_counter_ns() # 高精度循环结束时间

            processed_count += 1
            if processed_count % 5000 == 0: # Print progress and timing
                 data_time = datetime.fromtimestamp(ts_ns / 1_000_000_000)
                 loop_duration_ms = (loop_end_time - loop_start_time) / 1_000_000
                 publish_ms = (publish_end - publish_start) / 1_000_000
                 check_ms = (check_order_end - check_order_start) / 1_000_000
                 match_ms = (match_end - match_start) / 1_000_000
                 logger.info(f"已处理 {processed_count}/{len(self.all_ticks)} ticks | 模拟时间: {data_time.strftime('%H:%M:%S.%f')[:-3]} | 循环耗时: {loop_duration_ms:.3f} ms (Pub: {publish_ms:.3f}, Check: {check_ms:.3f}, Match: {match_ms:.3f})")

        end_sim_time = time.time()
        logger.info(f"模拟回测结束。处理了 {processed_count} 条 ticks。") # Use logger
        logger.info(f"模拟耗时: {end_sim_time - start_sim_time:.2f} 秒。") # Use logger
        self.running = False
        self.stop() # Clean up ZMQ

    def stop(self):
        """Stops the service and cleans up resources."""
        logger.info("停止模拟引擎...") # Use logger
        self.running = False
        sockets = [self.md_publisher, self.report_publisher, self.order_puller]
        for sock in sockets:
            if sock:
                try: sock.close()
                except Exception: pass # Ignore errors on close
        if self.context:
            try:
                if not self.context.closed: self.context.term()
            except Exception: pass
        logger.info("模拟引擎资源已释放。") # Use logger

# --- Main execution block (for testing, might not be practical) ---
if __name__ == "__main__":
     logger.error("Simulation Engine - 此脚本通常不直接运行，而是由回测主脚本调用。")
     # Example basic run now needs commission_rules and contract_multipliers
     # today = datetime.now().strftime('%Y%m%d')
     # example_commission_rules = {
     #     "SA505": {"open_rate": 0.0001, "close_rate": 0.00015, "min_commission": 1.0},
     #     "rb2510": {"open_fixed": 2.0, "close_fixed": 2.0}
     # }
     # example_multipliers = {"SA505": 10, "rb2510": 10}
     # engine = SimulationEngineService(
     #     data_source_path=config.BACKTEST_DATA_SOURCE_PATH,
     #     backtest_md_pub_url=config.BACKTEST_DATA_PUB_URL,
     #     backtest_report_pub_url=config.BACKTEST_ORDER_REPORT_PUB_URL,
     #     backtest_order_pull_url=config.BACKTEST_ORDER_REQUEST_PULL_URL,
     #     date_str=today,
     #     commission_rules=example_commission_rules,
     #     contract_multipliers=example_multipliers
     # )
     # if engine.load_data():
     #     engine.run_simulation()

