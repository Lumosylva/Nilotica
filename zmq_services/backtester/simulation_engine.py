import zmq
import msgpack
import time
import sys
import os
import json
import heapq
from datetime import datetime
from collections import deque

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')) # Go up two levels
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import local config
try:
    from zmq_services import config
except ImportError:
     print("无法导入 zmq_services.config。")
     sys.exit(1)

# Import necessary vnpy constants and objects
try:
    from vnpy.trader.constant import (Direction, OrderType, Exchange, Offset, Status)
    from vnpy.trader.object import OrderRequest # For type hinting if needed
    # We won't create full vnpy OrderData/TradeData objects, just dicts mimicking them
except ImportError:
    print("无法导入 vnpy 常量/对象，请确保 vnpy 已安装。")
    # Define fallbacks if needed
    class Direction: LONG = "多"; SHORT = "空"
    class OrderType: LIMIT = "限价"; MARKET = "市价"; STOP = "STOP" # Add others if used
    class Exchange: pass # Placeholder
    class Offset: NONE = "无"; OPEN = "开"; CLOSE = "平"; CLOSETODAY="平今"; CLOSEYESTERDAY="平昨"
    class Status: SUBMITTING="提交中"; NOTTRADED="未成交"; PARTTRADED="部分成交"; ALLTRADED="全部成交"; CANCELLED="已撤销"; REJECTED="拒单"
    class OrderRequest: pass # Placeholder

# --- Serialization/Deserialization Helpers (Adapted from gateways) ---
# We need these to process incoming requests and format outgoing reports

def vnpy_report_to_dict(obj_dict):
    """Converts dicts with enums/datetime to basic types for msgpack."""
    # This function assumes input is already a dict, but might contain non-serializable types
    d = obj_dict.copy() # Work on a copy
    for key, value in d.items():
        if isinstance(value, (Direction, OrderType, Exchange, Offset, Status)):
            d[key] = value.value # Use Enum value
        elif isinstance(value, datetime):
            d[key] = value.isoformat() if value else None
    return d

def dict_to_order_request_data(data_dict) -> dict | None:
     """Converts received message data dict into a validated OrderRequest-like dict."""
     # This doesn't create a vnpy object, just validates and formats the dict
     try:
         # Validate required fields exist
         required_fields = ['symbol', 'exchange', 'direction', 'type', 'volume']
         if not all(field in data_dict for field in required_fields):
             missing = [f for f in required_fields if f not in data_dict]
             print(f"订单请求字典缺少字段: {missing}")
             return None

         # Validate enum values (optional but good practice)
         try:
             Direction(data_dict['direction'])
             OrderType(data_dict['type'])
             Exchange(data_dict['exchange']) # Assumes vnpy's Exchange is available or compatible string used
             Offset(data_dict.get('offset', Offset.NONE)) # Check offset too
         except ValueError as e:
             print(f"订单请求字典包含无效枚举值: {e}")
             return None

         # Return a cleaned-up dict (or the original if validation passed)
         # Add default price/offset if missing and appropriate
         data_dict.setdefault('price', 0.0)
         data_dict.setdefault('offset', Offset.NONE)
         data_dict.setdefault('reference', 'backtest_strategy') # Default reference
         return data_dict

     except Exception as e:
         print(f"转换订单请求字典时出错: {e}")
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
        print(f"模拟行情发布器绑定到: {backtest_md_pub_url}")

        # Publisher for simulated order reports (status, trades)
        self.report_publisher = self.context.socket(zmq.PUB)
        self.report_publisher.bind(backtest_report_pub_url)
        print(f"模拟回报发布器绑定到: {backtest_report_pub_url}")

        # Pull socket to receive order requests from strategy
        self.order_puller = self.context.socket(zmq.PULL)
        self.order_puller.bind(backtest_order_pull_url)
        print(f"模拟订单接收器绑定到: {backtest_order_pull_url}")

        self.data_source_path = data_source_path
        self.date_str = date_str
        self.tick_data_file = os.path.join(self.data_source_path, f"ticks_{self.date_str}.jsonl")

        # +++ 存储手续费和合约乘数设置 +++
        self.commission_rules = commission_rules
        self.contract_multipliers = contract_multipliers
        # +++ 存储滑点设置 +++
        self.slippage = slippage
        print(f"手续费规则已加载: {self.commission_rules}")
        print(f"合约乘数已加载: {self.contract_multipliers}")
        print(f"固定滑点设置为: {self.slippage}")
        # +++ 结束存储 +++

        self.all_ticks = [] # List of (original_ts_ns, original_tick_message)
        self.current_tick_message = None # The tick currently being processed
        self.current_tick_time_ns = 0
        self.active_orders = {} # local_order_id -> order_request_data (dict)
        self.local_order_id_counter = 0
        self.trade_id_counter = 0

        self.running = False

    def load_data(self) -> bool:
        """Loads tick data from the specified file."""
        print(f"模拟引擎: 尝试从 {self.tick_data_file} 加载数据...")
        # Basic loading, similar to DataPlayer, but stores the whole original message dict
        if not os.path.exists(self.tick_data_file):
            print(f"错误: 数据文件不存在: {self.tick_data_file}")
            return False
        try:
            loaded_count = 0
            with open(self.tick_data_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        original_message = record.get("original_message")
                        if original_message and original_message.get("type") == "TICK":
                            original_ts_ns = original_message.get("timestamp")
                            if original_ts_ns:
                                self.all_ticks.append((original_ts_ns, original_message))
                                loaded_count += 1
                    except Exception as e:
                        print(f"警告: 处理记录时出错: {e}. 行: {line.strip()}")

            if not self.all_ticks:
                 print("错误：未能从文件中加载任何有效的 Tick 数据。")
                 return False

            self.all_ticks.sort(key=lambda x: x[0])
            print(f"数据加载完成并排序。总共 {loaded_count} 条 Tick 数据。")
            return True
        except Exception as e:
             print(f"加载数据时发生未知错误: {e}")
             import traceback; traceback.print_exc()
             return False

    def publish_tick(self, tick_message: dict):
        """Publishes the tick message to the backtest market data URL."""
        try:
            topic_str = tick_message.get("topic", "TICK.UNKNOWN")
            topic = topic_str.encode('utf-8')
            packed_message = msgpack.packb(tick_message, use_bin_type=True)
            self.md_publisher.send_multipart([topic, packed_message])
        except Exception as e:
            print(f"发布模拟 Tick 时出错: {e}")

    def check_for_new_orders(self):
        """Non-blockingly checks for and processes new order requests."""
        while True: # Process all waiting requests immediately
            try:
                packed_request = self.order_puller.recv(flags=zmq.NOBLOCK)
                request_msg = msgpack.unpackb(packed_request, raw=False)
                request_data = request_msg.get('data')
                print(f"模拟引擎: 收到订单请求: {request_data}")

                validated_data = dict_to_order_request_data(request_data)
                if validated_data:
                    self.local_order_id_counter += 1
                    local_order_id = f"sim_{self.local_order_id_counter}"
                    validated_data['local_order_id'] = local_order_id # Store our ID
                    validated_data['vt_orderid'] = f"SIM.{local_order_id}" # vnpy style ID
                    validated_data['order_time_ns'] = self.current_tick_time_ns # Timestamp the order

                    self.active_orders[local_order_id] = validated_data
                    print(f"  订单 {local_order_id} 已接受并激活。")

                    # Send SUBMITTED status report immediately
                    self.generate_and_publish_order_report(validated_data, Status.SUBMITTING) # Or SUBMITTED
                    self.generate_and_publish_order_report(validated_data, Status.NOTTRADED)

                else:
                     print("  无效订单请求，已忽略。")
                     # Maybe send a REJECTED report?

            except zmq.Again:
                break # No more messages waiting
            except msgpack.UnpackException as e:
                print(f"解码订单请求时出错: {e}")
            except Exception as e:
                print(f"处理订单请求时发生未知错误: {e}")
                import traceback; traceback.print_exc()

    def match_orders(self):
        """Attempts to match active orders against the current tick."""
        # +++ Debug: 函数入口 +++
        # entry_sim_time_str = datetime.fromtimestamp(self.current_tick_time_ns / 1_000_000_000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        # print(f"--- ENTER match_orders @ {entry_sim_time_str} ---")
        # +++ 结束 +++

        if not self.current_tick_message or not self.current_tick_message.get("data"):
            # +++ Debug: 因无 Tick 数据返回 +++
            # entry_sim_time_str = datetime.fromtimestamp(self.current_tick_time_ns / 1_000_000_000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] # Need this if uncommenting below
            # print(f"--- EXIT match_orders @ {entry_sim_time_str} (No Tick Data) ---")
            # +++ 结束 +++
            return

        tick_data = self.current_tick_message["data"]
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

        # +++ Debug: 即将进入循环 +++
        # print(f"--- LOOPING active_orders @ {sim_time_str} for Tick {tick_symbol} ---")
        # +++ 结束 +++

        orders_to_remove = []
        for local_id, order_data in list(self.active_orders.items()):
            order_vt_symbol = f"{order_data.get('symbol')}.{order_data.get('exchange')}"
            if order_vt_symbol != tick_symbol:
                continue

            # +++ 注释掉额外调试 +++
            # print(f"  DEBUG [{sim_time_str}] Checking order: ID={local_id}, Symbol={order_vt_symbol}, Direction={order_data.get('direction')}, Type={order_data.get('type')}, Price={order_data.get('price')}")
            # +++ 结束注释 +++

            # --- 修正日志时间戳 ---
            print(f"[{sim_time_str}] 撮合检查: 订单 {local_id} ({order_data.get('direction')} {order_data.get('type')} @ {order_data.get('price')}) vs Tick {tick_symbol} (Bid: {bid_price}, Ask: {ask_price})")
            # --- 结束修正 ---

            trade_price = 0
            matched = False
            match_condition = None # 初始化撮合条件变量

            if order_data['direction'] == Direction.LONG.value:
                if order_data['type'] == OrderType.LIMIT.value:
                    match_condition = order_data['price'] >= bid_price
                    # --- 注释掉条件检查日志 ---
                    # print(f"  [{sim_time_str}] 买单限价条件检查: order_price({order_data['price']}) >= bid_price({bid_price}) ? {match_condition}")
                    # --- 结束注释 ---
                    if match_condition:
                        trade_price = ask_price # 买单以卖一价成交
                        matched = True
                elif order_data['type'] == OrderType.MARKET.value:
                    # --- 确保市价日志也有时间戳 ---
                    match_condition = True # 市价单认为条件满足
                    trade_price = ask_price
                    matched = True
                    # --- 注释掉条件检查日志 ---
                    # print(f"  [{sim_time_str}] 买单市价条件: 自动匹配，成交价 = ask_price({ask_price})")
                    # --- 结束注释 ---
            elif order_data['direction'] == Direction.SHORT.value:
                if order_data['type'] == OrderType.LIMIT.value:
                    match_condition = order_data['price'] <= ask_price
                    # --- 注释掉条件检查日志 ---
                    # print(f"  [{sim_time_str}] 卖单限价条件检查: order_price({order_data['price']}) <= ask_price({ask_price}) ? {match_condition}")
                    # --- 结束注释 ---
                    if match_condition:
                        trade_price = bid_price # 卖单以买一价成交
                        matched = True
                elif order_data['type'] == OrderType.MARKET.value:
                    # --- 确保市价日志也有时间戳 ---
                    match_condition = True # 市价单认为条件满足
                    trade_price = bid_price
                    matched = True
                    # --- 注释掉条件检查日志 ---
                    # print(f"  [{sim_time_str}] 卖单市价条件: 自动匹配，成交价 = bid_price({bid_price})")
                    # --- 结束注释 ---


            if matched:
                # --- 确保日志有时间戳 ---
                print(f"    [{sim_time_str}] 订单 {local_id} 匹配成功! 成交价: {trade_price:.2f}")
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
                    print(f"    应用滑点: 方向={order_data['direction']}, 滑点值={self.slippage}, 理论价={original_trade_price:.2f} => 实际价={trade_price:.2f}")
                    # --- 结束修正 ---
                # +++ 结束应用 +++

                trade_report_data = {
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
                    "price": trade_price, # <--- 使用调整后的价格
                    "volume": order_data['volume'],
                    "trade_time": sim_time_dt.isoformat(), # 使用模拟时间 dt 对象转 iso
                    "datetime": sim_time_dt # 使用模拟时间 dt 对象
                }
                # 注意：generate_and_publish_trade_report 会使用这个价格计算手续费
                self.generate_and_publish_trade_report(trade_report_data)
                self.generate_and_publish_order_report(order_data, Status.ALLTRADED, traded_volume=order_data['volume'])
                orders_to_remove.append(local_id)
            # --- (可选) 添加未匹配时的日志 ---
            # elif match_condition is not None and not matched:
            #     print(f"  [{sim_time_str}] 订单 {local_id} 未满足撮合条件。")
            # --- 结束 ---

        for local_id in orders_to_remove:
            try:
                del self.active_orders[local_id]
                # --- 确保日志有时间戳 ---
                print(f"    [{sim_time_str}] 订单 {local_id} 已完成并从活动列表移除。")
                # --- 结束确保 ---
            except KeyError:
                 print(f"警告：[{sim_time_str}] 尝试移除订单 {local_id} 时未找到。")

        # +++ Debug: 函数正常结束 +++
        # print(f"--- EXIT match_orders @ {sim_time_str} (Normal) ---")
        # +++ 结束 +++

    def generate_and_publish_order_report(self, order_request_data: dict, status: Status, traded_volume: float = 0):
        """Generates and publishes an OrderData-like report dictionary."""
        vt_symbol = f"{order_request_data['symbol']}.{order_request_data['exchange']}"
        # 使用订单时间戳
        order_time_dt = datetime.fromtimestamp(order_request_data['order_time_ns'] / 1_000_000_000)
        # *** 修正: 在函数内部根据当前模拟时间计算回报时间 ***
        report_time_dt = datetime.fromtimestamp(self.current_tick_time_ns / 1_000_000_000) # 使用当前模拟时间

        report_data = {
            "gateway_name": "SIMULATOR",
            "symbol": order_request_data['symbol'],
            "exchange": order_request_data['exchange'],
            "vt_symbol": vt_symbol,
            "orderid": order_request_data['local_order_id'],
            "vt_orderid": order_request_data['vt_orderid'],
            "direction": order_request_data['direction'],
            "offset": order_request_data['offset'],
            "type": order_request_data['type'],
            "price": order_request_data['price'],
            "volume": order_request_data['volume'],
            "traded": traded_volume,
            "status": status,
            "order_time": order_time_dt.isoformat(), # 订单原始时间
            "datetime": report_time_dt # 回报生成时的模拟时间
        }
        topic_str = f"ORDER_STATUS.{vt_symbol}"
        message = {
            "topic": topic_str,
            "type": "ORDER_STATUS",
            "source": "SimulationEngine",
            "timestamp": self.current_tick_time_ns, # 使用模拟时间戳
            "data": vnpy_report_to_dict(report_data)
        }
        try:
            topic = topic_str.encode('utf-8')
            packed_message = msgpack.packb(message, use_bin_type=True)
            self.report_publisher.send_multipart([topic, packed_message])
        except Exception as e:
            # *** 添加错误发生时的时间戳和上下文信息 ***
            sim_time_str = datetime.fromtimestamp(self.current_tick_time_ns / 1_000_000_000).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
            print(f"[{sim_time_str}] 发布订单回报 (状态: {status.value}, 订单: {order_request_data.get('local_order_id')}) 时出错: {e}")
            # *** 结束添加 ***

    def generate_and_publish_trade_report(self, trade_data: dict):
        """Generates and publishes a TradeData-like report dictionary, including commission."""
        vt_symbol = trade_data["vt_symbol"]
        symbol = trade_data["symbol"]
        price = trade_data["price"]
        volume = trade_data["volume"]
        offset = trade_data["offset"] # 获取开平仓信息 (应为 '开', '平', '平今', '平昨')

        # +++ 计算手续费 +++
        commission = 0.0
        rules = self.commission_rules.get(symbol)
        multiplier = self.contract_multipliers.get(symbol)

        if rules and multiplier and multiplier > 0:
            rate = 0.0
            fixed = 0.0
            min_comm = rules.get('min_commission', 0.0)

            # 根据开平标志选择费率或固定金额
            # 注意：直接比较从 order_data 传来的字符串值
            if offset == Offset.OPEN.value:
                rate = rules.get('open_rate', 0.0)
                fixed = rules.get('open_fixed', 0.0)
            elif offset in [Offset.CLOSE.value, Offset.CLOSETODAY.value, Offset.CLOSEYESTERDAY.value]:
                rate = rules.get('close_rate', 0.0)
                fixed = rules.get('close_fixed', 0.0)
            else:
                 print(f"警告: 未知的开平标志 '{offset}'，手续费按开仓计算 (如果规则存在)。")
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

            print(f"    计算手续费: 合约={symbol}, 价格={price}, 量={volume}, 乘数={multiplier}, 开平={offset}, 费率={rate}, 固定={fixed}, 最低={min_comm} => 手续费={commission:.2f}")

        elif not rules:
             print(f"警告: 未找到合约 {symbol} 的手续费规则，手续费计为 0。")
        elif not multiplier or multiplier <= 0:
             print(f"警告: 未找到或无效的合约 {symbol} 乘数 ({multiplier})，手续费计为 0。")
        # +++ 结束计算 +++

        # 将手续费添加到回报数据中
        trade_data['commission'] = commission

        # --- 后续发布逻辑不变 ---
        topic_str = f"TRADE.{vt_symbol}"
        # 确保 trade_data 中的 datetime 是 datetime 对象
        if isinstance(trade_data.get("datetime"), str):
             trade_data["datetime"] = datetime.fromisoformat(trade_data["datetime"])

        message = {
            "topic": topic_str,
            "type": "TRADE",
            "source": "SimulationEngine",
            "timestamp": self.current_tick_time_ns, # 使用模拟时间戳
            "data": vnpy_report_to_dict(trade_data) # vnpy_report_to_dict 会处理 datetime
        }
        try:
            topic = topic_str.encode('utf-8')
            packed_message = msgpack.packb(message, use_bin_type=True)
            self.report_publisher.send_multipart([topic, packed_message])
        except Exception as e:
             print(f"发布成交回报时出错: {e}")

    def run_simulation(self):
        """Runs the main simulation loop."""
        if not self.all_ticks:
            print("错误: 没有数据可供模拟。请先调用 load_data()。")
            return

        print(f"开始模拟回测 {self.date_str}...")
        self.running = True
        processed_count = 0

        start_sim_time = time.time() # Wall clock time

        for ts_ns, tick_msg in self.all_ticks:
            if not self.running:
                print("模拟被中断。")
                break

            self.current_tick_time_ns = ts_ns
            self.current_tick_message = tick_msg
            loop_start_time = time.perf_counter_ns() # 高精度循环开始时间

            # 1. Publish current tick
            publish_start = time.perf_counter_ns()
            self.publish_tick(tick_msg)
            publish_end = time.perf_counter_ns()

            # 2. Check for new orders
            check_order_start = time.perf_counter_ns()
            self.check_for_new_orders()
            check_order_end = time.perf_counter_ns()

            # 3. Attempt to match active orders
            match_start = time.perf_counter_ns()
            self.match_orders()
            match_end = time.perf_counter_ns()

            loop_end_time = time.perf_counter_ns() # 高精度循环结束时间

            processed_count += 1
            if processed_count % 5000 == 0: # Print progress and timing
                 data_time = datetime.fromtimestamp(ts_ns / 1_000_000_000)
                 loop_duration_ms = (loop_end_time - loop_start_time) / 1_000_000
                 publish_ms = (publish_end - publish_start) / 1_000_000
                 check_ms = (check_order_end - check_order_start) / 1_000_000
                 match_ms = (match_end - match_start) / 1_000_000
                 print(f"  已处理 {processed_count}/{len(self.all_ticks)} ticks | 模拟时间: {data_time.strftime('%H:%M:%S.%f')[:-3]} | 循环耗时: {loop_duration_ms:.3f} ms (Pub: {publish_ms:.3f}, Check: {check_ms:.3f}, Match: {match_ms:.3f})")

        end_sim_time = time.time()
        print(f"模拟回测结束。处理了 {processed_count} 条 ticks。")
        print(f"模拟耗时: {end_sim_time - start_sim_time:.2f} 秒。")
        self.running = False
        self.stop() # Clean up ZMQ

    def stop(self):
        """Stops the service and cleans up resources."""
        print("停止模拟引擎...")
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
        print("模拟引擎资源已释放。")

# --- Main execution block (for testing, might not be practical) ---
if __name__ == "__main__":
     print("Simulation Engine - 此脚本通常不直接运行，而是由回测主脚本调用。")
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

