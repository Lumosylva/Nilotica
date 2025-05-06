from typing import Optional

import zmq
import pickle
import time
import sys
import os
import json
import msgpack # +++ Add msgpack import +++
from datetime import datetime, timedelta
from collections import deque

# +++ Add Logger Import +++
from utils.logger import logger # Assuming logger is configured elsewhere (e.g., run_backtest.py)

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
except ImportError:
    # +++ Add ERROR log here +++
    logger.error("CRITICAL: Failed to import vnpy objects/constants! Using dummy fallback classes. Backtest results may be inaccurate.")
    # +++ End Add +++
    logger.warning("无法导入 vnpy 常量/对象，将使用内部定义的 Fallback。请确保 vnpy 已安装。")
    # Define fallbacks if needed
    class Direction: LONG = "多"; SHORT = "空"
    class OrderType: LIMIT = "限价"; MARKET = "市价"; STOP = "STOP" # Add others if used
    class Exchange: pass # Placeholder
    class Offset: NONE = "无"; OPEN = "开"; CLOSE = "平"; CLOSETODAY="平今"; CLOSEYESTERDAY="平昨"
    class Status: SUBMITTING="提交中"; NOTTRADED="未成交"; PARTTRADED="部分成交"; ALLTRADED="全部成交"; CANCELLED="已撤销"; REJECTED="拒单"
    class OrderRequest: pass # Placeholder
    class TickData: pass
    class OrderData: pass
    class TradeData: pass

# --- Serialization/Deserialization Helpers (Adapted from gateways) ---
# We need these to process incoming requests and format outgoing reports

# +++ Import the converter function +++
from utils.converter import convert_vnpy_obj_to_dict # UPDATED IMPORT

def create_tick_from_dict(tick_dict: dict) -> Optional[TickData]:
    """Creates a TickData object from a dictionary, handling potential errors."""
    vt_symbol = tick_dict.get('vt_symbol')
    if not vt_symbol or '.' not in vt_symbol:
        logger.error(f"Invalid or missing vt_symbol in tick dict: {tick_dict}")
        return None
    try:
        tick = TickData(
            gateway_name=tick_dict.get('gateway_name', 'SIMULATOR'),
            symbol=vt_symbol.split('.')[0],
            exchange=Exchange(vt_symbol.split('.')[-1]),
            datetime=datetime.fromtimestamp(tick_dict.get('timestamp') / 1_000_000_000) if tick_dict.get('timestamp') else datetime.now(),
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
            bid_price_2=float(tick_dict.get('bid_price_2', 0.0)),
            bid_price_3=float(tick_dict.get('bid_price_3', 0.0)),
            bid_price_4=float(tick_dict.get('bid_price_4', 0.0)),
            bid_price_5=float(tick_dict.get('bid_price_5', 0.0)),
            ask_price_1=float(tick_dict.get('ask_price_1', 0.0)),
            ask_price_2=float(tick_dict.get('ask_price_2', 0.0)),
            ask_price_3=float(tick_dict.get('ask_price_3', 0.0)),
            ask_price_4=float(tick_dict.get('ask_price_4', 0.0)),
            ask_price_5=float(tick_dict.get('ask_price_5', 0.0)),
            bid_volume_1=float(tick_dict.get('bid_volume_1', 0.0)),
            bid_volume_2=float(tick_dict.get('bid_volume_2', 0.0)),
            bid_volume_3=float(tick_dict.get('bid_volume_3', 0.0)),
            bid_volume_4=float(tick_dict.get('bid_volume_4', 0.0)),
            bid_volume_5=float(tick_dict.get('bid_volume_5', 0.0)),
            ask_volume_1=float(tick_dict.get('ask_volume_1', 0.0)),
            ask_volume_2=float(tick_dict.get('ask_volume_2', 0.0)),
            ask_volume_3=float(tick_dict.get('ask_volume_3', 0.0)),
            ask_volume_4=float(tick_dict.get('ask_volume_4', 0.0)),
            ask_volume_5=float(tick_dict.get('ask_volume_5', 0.0)),
            localtime=tick_dict.get('localtime') # Keep as is or convert if needed
        )
        tick.vt_symbol = vt_symbol # Ensure vt_symbol is set
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
                 # +++ 添加滑点参数 +++
                 slippage: float = 0.0 # 默认为 0, 将由调用者传入 2.0
                 # +++ 结束添加 +++
                 ):
        """Initializes the Simulation Engine service."""
        self.context = zmq.Context()

        # Publisher for simulated market data
        self.md_publisher = self.context.socket(zmq.PUB)
        self.md_publisher.bind(backtest_md_pub_url)
        logger.info(f"模拟行情发布器绑定到: {backtest_md_pub_url}") # Use logger

        # Publisher for simulated order reports (status, trades)
        self.report_publisher = self.context.socket(zmq.PUB)
        self.report_publisher.bind(backtest_report_pub_url)
        logger.info(f"模拟回报发布器绑定到: {backtest_report_pub_url}") # Use logger

        # Pull socket to receive order requests from strategy
        self.order_puller = self.context.socket(zmq.PULL)
        self.order_puller.bind(backtest_order_pull_url)
        logger.info(f"模拟订单接收器绑定到: {backtest_order_pull_url}") # Use logger

        self.data_source_path = data_source_path
        self.date_str = date_str
        self.tick_data_file = os.path.join(self.data_source_path, f"ticks_{self.date_str}.jsonl")

        # +++ 存储手续费和合约乘数设置 +++
        self.commission_rules = commission_rules
        self.contract_multipliers = contract_multipliers
        # +++ 存储滑点设置 +++
        self.slippage = slippage
        logger.debug(f"手续费规则: {self.commission_rules}")
        logger.debug(f"合约乘数: {self.contract_multipliers}")
        logger.info(f"固定滑点设置为: {self.slippage}") # Keep slippage as info
        # +++ 结束存储 +++

        # --- Modified tick storage --- 
        self.all_ticks = [] # List of (original_ts_ns, topic_string, original_tick_message_dict)
        # --- End Modification ---
        self.current_tick_message_dict = None # Store the dict
        self.current_tick_time_ns = 0
        self.active_orders = {} # local_order_id -> order_request_data (dict)
        self.local_order_id_counter = 0
        self.trade_id_counter = 0

        self.running = False
        self.unique_topics_loaded = set() # +++ Add set to track loaded topics +++

    def load_data(self) -> bool:
        """Loads tick data from the specified file."""
        logger.info(f"模拟引擎: 尝试从 {self.tick_data_file} 加载数据...") # Use logger
        if not os.path.exists(self.tick_data_file):
            logger.error(f"数据文件不存在: {self.tick_data_file}") # Use logger
            return False
        try:
            loaded_count = 0
            # +++ Clear and track unique topics +++
            self.unique_topics_loaded = set()
            topics_to_log = 5 # Log first 5 unique topics
            # +++ End Track +++
            with open(self.tick_data_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        original_message = record.get("original_message")
                        zmq_topic_str = record.get("zmq_topic") # <<< Get the correct topic

                        if original_message and original_message.get("type") == "TICK" and zmq_topic_str:
                            # +++ Log unique topics +++
                            if zmq_topic_str not in self.unique_topics_loaded and len(self.unique_topics_loaded) < topics_to_log:
                                self.unique_topics_loaded.add(zmq_topic_str)
                                logger.debug(f"Found unique ZMQ topic in data file: '{zmq_topic_str}'")
                            # +++ End Log +++
                            original_ts_ns = original_message.get("timestamp")
                            if original_ts_ns:
                                # --- Store topic along with timestamp and message --- 
                                self.all_ticks.append((original_ts_ns, zmq_topic_str, original_message))
                                # --- End Store topic ---
                                loaded_count += 1
                            else:
                                logger.warning(f"Tick record missing internal timestamp: {original_message}")
                        # else: Skip non-tick messages or records missing topic

                    except Exception as e:
                        logger.warning(f"处理记录时出错: {e}. 行: {line.strip()}") # Use logger

            if not self.all_ticks:
                 logger.error("未能从文件中加载任何有效的 Tick 数据。") # Use logger
                 return False

            self.all_ticks.sort(key=lambda x: x[0])
            logger.info(f"数据加载完成并排序。总共 {loaded_count} 条 Tick 数据。") # Use logger
            # +++ Log final unique topics summary +++
            logger.info(f"加载完成。从数据文件中找到 {len(self.unique_topics_loaded)} 个唯一 ZMQ 主题 (最多记录 {topics_to_log} 个)。")
            # +++ End Log +++
            return True
        except Exception as e:
             logger.exception(f"加载数据时发生未知错误: {e}") # Use logger.exception
             return False

    # --- Modified publish_tick to accept processed_count --- 
    def publish_tick(self, topic_str: str, tick_dict: dict, processed_count: int):
        """Converts tick dict to TickData object and publishes pickled object."""
        if not topic_str:
             logger.warning(f"Attempted to publish tick with empty topic: {tick_dict}")
             return
        
        # --- Get the inner 'data' dictionary --- 
        actual_tick_data_dict = tick_dict.get('data')
        if not isinstance(actual_tick_data_dict, dict):
            logger.error(f"Tick message on topic '{topic_str}' is missing 'data' field or it's not a dict: {tick_dict}")
            return
        # --- End Get inner dict --- 

        # --- Create TickData object FROM the inner dict --- 
        tick_object = create_tick_from_dict(actual_tick_data_dict)
        # --- End Create ---
        if not tick_object:
            return # Error logged in create_tick_from_dict

        try:
            topic = topic_str.encode('utf-8')
            # --- Convert object to dict THEN msgpack --- 
            dict_data = convert_vnpy_obj_to_dict(tick_object)
            packed_message = msgpack.packb(dict_data, use_bin_type=True)
            # --- End Convert and Pack ---
            # +++ Log published topic using processed_count +++
            # --- FIX: Correct log frequency condition --- 
            if processed_count % 5000 == 1: # Log every 5000th tick (using the CORRECT loop counter)
                 logger.debug(f"Publishing tick #{processed_count} with topic: '{topic_str}'")
            # --- End FIX --- 
            # +++ End Log +++
            self.md_publisher.send_multipart([topic, packed_message])
        except (msgpack.PackException, TypeError) as e_msgpack:
             logger.error(f"Msgpack序列化Tick字典时出错 (Topic: {topic_str}): {e_msgpack}")
        except Exception as e:
            logger.error(f"发布模拟 Tick (Topic: {topic_str}) 时出错: {e}") 
    # --- End Modification ---

    def check_for_new_orders(self):
        """Non-blockingly checks for and processes new order requests."""
        while True: 
            try:
                # --- Receive msgpack request bytes --- 
                packed_request = self.order_puller.recv(flags=zmq.NOBLOCK)
                request_tuple = msgpack.unpackb(packed_request, raw=False) # Expecting ("send_order", (req_dict,), {})
                # --- End Receive and Unpack ---

                # +++ Add DEBUG log after receiving tuple +++
                logger.debug(f"check_for_new_orders: Received request tuple: {request_tuple}")
                # +++ End Add +++

                # --- Validate and extract req_dict --- 
                if isinstance(request_tuple, tuple) and len(request_tuple) == 3 and request_tuple[0] == "send_order":
                     if request_tuple[1] and isinstance(request_tuple[1][0], dict):
                         request_data = request_tuple[1][0] # Get the actual order dict
                     else:
                         logger.error(f"收到的 send_order 请求格式无效 (args): {request_tuple}")
                         continue
                else:
                     logger.error(f"收到的订单请求格式无效 (非send_order元组): {request_tuple}")
                     continue
                # --- End Validation --- 

                # --- Add DEBUG log after successful validation --- 
                logger.debug(f"check_for_new_orders: Successfully validated order request dict: {request_data}")
                # --- End Add ---

                self.local_order_id_counter += 1
                local_order_id = f"sim_{self.local_order_id_counter}"
                validated_data = request_data # Assume basic validation for now
                # Add more robust validation if needed (check fields, enums)
                required_fields = ['symbol', 'exchange', 'direction', 'type', 'volume']
                if not all(field in validated_data for field in required_fields):
                    missing = [f for f in required_fields if f not in validated_data]
                    logger.error(f"订单请求字典缺少字段: {missing}. Dict: {validated_data}")
                    # Maybe send REJECTED report?
                    continue
                validated_data['local_order_id'] = local_order_id
                validated_data['vt_orderid'] = f"SIM.{local_order_id}"
                validated_data['order_time_ns'] = self.current_tick_time_ns

                self.active_orders[local_order_id] = validated_data
                logger.info(f"订单 {local_order_id} 已接受并激活。VTOrderID: {validated_data['vt_orderid']}")

                self.generate_and_publish_order_report(validated_data, Status.SUBMITTING)
                self.generate_and_publish_order_report(validated_data, Status.NOTTRADED)

            except zmq.Again:
                break # No more messages waiting
            except pickle.UnpicklingError as e:
                logger.error(f"反序列化订单请求时出错: {e}")
            except (msgpack.UnpackException, TypeError, ValueError) as e_unpack:
                 logger.error(f"Msgpack反序列化订单请求时出错: {e_unpack}")
            except Exception as e:
                logger.exception(f"处理订单请求时发生未知错误: {e}")

    def match_orders(self):
        """Attempts to match active orders against the current tick (using dict)."""
        # --- Logic remains largely the same, using self.current_tick_message_dict --- 
        if not self.current_tick_message_dict or not self.current_tick_message_dict.get("data"):
             return
        tick_data = self.current_tick_message_dict["data"]
        ask_price = tick_data.get('ask_price_1')
        bid_price = tick_data.get('bid_price_1')
        tick_symbol = tick_data.get('vt_symbol')
        # 使用纳秒时间戳创建更精确的 datetime 对象 - 用于内部逻辑和回报
        sim_time_dt = datetime.fromtimestamp(self.current_tick_time_ns / 1_000_000_000)
        # 创建用于日志打印的毫秒级时间字符串
        sim_time_str = sim_time_dt.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        if ask_price is None or bid_price is None or ask_price <= 0 or bid_price <= 0:
            # +++ Debug: 因无效价格返回 +++
            # print(f"--- EXIT match_orders @ {sim_time_str} (Invalid Price for {tick_symbol}: Bid={bid_price}, Ask={ask_price}) ---")
            # +++ 结束 +++
            return # 无有效价格，无法撮合

        orders_to_remove = []
        for local_id, order_data in list(self.active_orders.items()):
            order_vt_symbol = f"{order_data.get('symbol')}.{order_data.get('exchange')}"
            if order_vt_symbol != tick_symbol:
                continue

            # --- 修正日志时间戳 ---
            logger.debug(f"[{sim_time_str}] 撮合检查: 订单 {local_id} ({order_data.get('direction')} {order_data.get('type')} @ {order_data.get('price')}) vs Tick {tick_symbol} (Bid: {bid_price}, Ask: {ask_price})")
            # --- 结束修正 ---

            trade_price = 0
            matched = False
            match_condition = None # 初始化撮合条件变量

            if order_data['direction'] == Direction.LONG.value:
                if order_data['type'] == OrderType.LIMIT.value:
                    match_condition = order_data['price'] >= bid_price
                    if match_condition:
                        trade_price = ask_price # 买单以卖一价成交
                        matched = True
                elif order_data['type'] == OrderType.MARKET.value:
                    match_condition = True # 市价单认为条件满足
                    trade_price = ask_price
                    matched = True
            elif order_data['direction'] == Direction.SHORT.value:
                if order_data['type'] == OrderType.LIMIT.value:
                    match_condition = order_data['price'] <= ask_price
                    if match_condition:
                        trade_price = bid_price # 卖单以买一价成交
                        matched = True
                elif order_data['type'] == OrderType.MARKET.value:
                    match_condition = True # 市价单认为条件满足
                    trade_price = bid_price
                    matched = True

            if matched:
                # --- 确保日志有时间戳 ---
                logger.info(f"[{sim_time_str}] 订单 {local_id} 匹配成功! 理论成交价: {trade_price:.2f}") # Use logger
                # --- 结束确保 ---
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
        """Generates OrderData object from dict and publishes pickled object."""
        vt_symbol = order_report_dict.get('vt_symbol', f"{order_report_dict.get('symbol', '')}.{order_report_dict.get('exchange', '')}")
        vt_orderid = order_report_dict.get('vt_orderid')
        
        # --- Create OrderData object --- 
        try:
             # Use order time from original request if available
             order_time_ns = order_report_dict.get('order_time_ns', self.current_tick_time_ns)
             order_time_dt = datetime.fromtimestamp(order_time_ns / 1_000_000_000)
             report_time_dt = datetime.fromtimestamp(self.current_tick_time_ns / 1_000_000_000)

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
                 traded=float(traded_volume),
                 status=status, # Use the passed status
                 datetime=report_time_dt # Use the datetime object for report time
             )
             order_object.vt_symbol = vt_symbol # Ensure vt_symbol is set
             order_object.vt_orderid = vt_orderid
        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Error creating OrderData object from dict for {vt_orderid}: {e}. Dict: {order_report_dict}")
            return
        # --- End Create --- 

        topic_str = f"order.{vt_symbol}" # Use generic order topic now
        try:
            topic = topic_str.encode('utf-8')
            # --- Convert object to dict THEN msgpack --- 
            dict_data = convert_vnpy_obj_to_dict(order_object)
            packed_message = msgpack.packb(dict_data, use_bin_type=True)
            # --- End Convert and Pack ---
            self.report_publisher.send_multipart([topic, packed_message])
        except (msgpack.PackException, TypeError) as e_msgpack:
             logger.error(f"Msgpack序列化Order字典时出错 (Status: {status.value}, Order: {vt_orderid}): {e_msgpack}")
        except Exception as e:
            sim_time_str = datetime.fromtimestamp(self.current_tick_time_ns / 1_000_000_000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            logger.error(f"[{sim_time_str}] 发布订单回报 (状态: {status.value}, 订单: {vt_orderid}) 时出错: {e}") 
    # --- End Modification ---

    def generate_and_publish_trade_report(self, trade_report_dict: dict):
        """Calculates commission, generates TradeData object, and publishes pickled object."""
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

        # --- Create TradeData object --- 
        vt_tradeid = trade_report_dict.get('vt_tradeid')
        try:
             # Use datetime object from dict if available
             trade_time_dt = trade_report_dict.get("datetime")
             if not isinstance(trade_time_dt, datetime):
                 # Fallback to parsing string or using current sim time
                 trade_time_str = trade_report_dict.get("trade_time")
                 if trade_time_str:
                      trade_time_dt = datetime.fromisoformat(trade_time_str)
                 else:
                      trade_time_dt = datetime.fromtimestamp(self.current_tick_time_ns / 1_000_000_000)
             
             trade_object = TradeData(
                 gateway_name="SIMULATOR",
                 symbol=symbol,
                 exchange=Exchange(trade_report_dict['exchange']),
                 orderid=trade_report_dict['orderid'],
                 tradeid=trade_report_dict['tradeid'],
                 direction=Direction(trade_report_dict['direction']),
                 offset=Offset(offset_val),
                 price=float(price),
                 volume=float(volume),
                 datetime=trade_time_dt # Use datetime object
             )
             trade_object.vt_symbol = vt_symbol # Ensure vt_symbol is set
             # --- Set vt_orderid and vt_tradeid AFTER creation ---
             trade_object.vt_orderid = trade_report_dict['vt_orderid'] 
             trade_object.vt_tradeid = vt_tradeid
             # +++ Assign calculated commission to the object +++
             trade_object.commission = float(commission)
             # +++ End Assign +++
             # --- End Set ---
        except (ValueError, TypeError, KeyError) as e:
            logger.error(f"Error creating TradeData object from dict for {vt_tradeid}: {e}. Dict: {trade_report_dict}")
            return
        # --- End Create --- 

        topic_str = f"trade.{vt_symbol}" # Use generic trade topic
        try:
            topic = topic_str.encode('utf-8')
            # --- Convert object to dict THEN msgpack --- 
            dict_data = convert_vnpy_obj_to_dict(trade_object)
            packed_message = msgpack.packb(dict_data, use_bin_type=True)
            # --- End Convert and Pack ---
            self.report_publisher.send_multipart([topic, packed_message])
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

