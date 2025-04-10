import zmq
import msgpack
import time
import sys
import os
from datetime import datetime

# Add project root to Python path if needed (e.g., for vnpy types if reconstructing objects)
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import local config
from . import config

# Import vnpy constants if needed for constructing order requests
from vnpy.trader.constant import Direction, OrderType, Exchange, Offset, Status

# --- Helper function to reconstruct vnpy objects (Optional for now) ---
# If you need full vnpy objects in the strategy, uncomment and potentially expand this.
# from vnpy.trader.object import TickData
# from vnpy.trader.constant import Exchange
#
# def dict_to_vnpy_object(data_dict, obj_type):
#     """Converts a dictionary back into a vnpy object."""
#     if obj_type == "TICK":
#         try:
#             # Handle datetime conversion from ISO string
#             dt_str = data_dict.get('datetime')
#             dt_obj = datetime.fromisoformat(dt_str) if dt_str else None
#             data_dict['datetime'] = dt_obj
#
#             # Handle localtime conversion
#             lt_str = data_dict.get('localtime')
#             lt_obj = datetime.fromisoformat(lt_str) if lt_str else None
#             data_dict['localtime'] = lt_obj
#
#             # Handle exchange conversion
#             exchange_str = data_dict.get('exchange')
#             exchange_obj = Exchange(exchange_str) if exchange_str else None
#             data_dict['exchange'] = exchange_obj
#
#             # Create TickData object (ensure all necessary fields are present)
#             # This might require more careful handling based on TickData.__init__ args
#             # or by setting attributes after creating a default object.
#             tick = TickData(**data_dict) # This assumes dict keys match TickData attributes
#             return tick
#         except Exception as e:
#             print(f"Error reconstructing TickData object: {e}")
#             print(f"Original data: {data_dict}")
#             return None
#     # Add elif for other types like BarData
#     else:
#         return data_dict # Return dict if type is unknown/unhandled

# --- Strategy Subscriber ---
class StrategySubscriber:
    def __init__(self, gateway_pub_url: str, order_req_url: str, order_report_url: str, subscribe_symbols: list):
        """Initializes the subscriber and order pusher."""
        self.context = zmq.Context()

        # Socket to subscribe to market data and order reports
        self.subscriber = self.context.socket(zmq.SUB)
        self.subscriber.setsockopt(zmq.LINGER, 0)
        self.subscriber.connect(gateway_pub_url) # Connect to Market Data PUB
        self.subscriber.connect(order_report_url) # Connect to Order Report PUB
        print(f"策略订阅器连接行情发布器: {gateway_pub_url}")
        print(f"策略订阅器连接回报发布器: {order_report_url}")

        # Socket to push order requests
        self.order_pusher = self.context.socket(zmq.PUSH)
        self.order_pusher.setsockopt(zmq.LINGER, 0)
        self.order_pusher.connect(order_req_url)
        print(f"策略订单推送器连接到: {order_req_url}")

        self.subscribe_topics = []
        if not subscribe_symbols:
            print("警告: 未指定订阅合约，将接收所有行情 (不推荐)!")
            # Still subscribe to specific report topics
            self.subscriber.subscribe("TICK.") # Subscribe to all ticks
            self.subscribe_topics.append("TICK.")
        else:
            for vt_symbol in subscribe_symbols:
                # Subscribe to TICK topics
                tick_topic_str = f"TICK.{vt_symbol}"
                self.subscriber.subscribe(tick_topic_str.encode('utf-8'))
                self.subscribe_topics.append(tick_topic_str)
                print(f"  订阅行情主题: {tick_topic_str}")

        # Subscribe to Order Status and Trade Reports (using prefix subscription)
        order_status_prefix = "ORDER_STATUS."
        trade_prefix = "TRADE."
        self.subscriber.subscribe(order_status_prefix.encode('utf-8'))
        self.subscriber.subscribe(trade_prefix.encode('utf-8'))
        self.subscribe_topics.append(order_status_prefix)
        self.subscribe_topics.append(trade_prefix)
        print(f"  订阅订单状态主题: {order_status_prefix}*")
        print(f"  订阅成交回报主题: {trade_prefix}*")

        print(f"已订阅 {len(self.subscribe_topics)} 个主题/前缀。")

        self.running = False
        self.last_order_time = {}
        self.sa505_long_pending_or_open = False
        # +++ 添加用于止盈止损的状态变量 +++
        self.sa505_entry_price = 0.0
        self.sa505_target_price = 0.0
        self.sa505_stop_price = 0.0
        # +++ 添加等待平仓标志 +++
        self.sa505_close_pending = False
        # +++ 结束添加 +++
        self.trades = []

    def send_limit_order(self, symbol: str, exchange: Exchange, direction: Direction, price: float, volume: float, offset: Offset = Offset.NONE):
        """Constructs and sends a limit order request via ZMQ PUSH socket."""
        # --- 禁用基于 time.time() 的冷却 (不适用于回测) ---
        # # Prevent sending orders too frequently for the same symbol
        # now = time.time()
        # if now - self.last_order_time.get(symbol, 0) < 10: # Cooldown period of 10 seconds
        #     # print(f"Order cooldown for {symbol}, skipping.")
        #     return
        # --- 结束禁用 ---

        # --- Add Price Validity Check ---
        if price is None or price <= 0:
            print(f"错误: 尝试发送订单时价格无效 ({price})，跳过。 Symbol: {symbol}")
            return

        print(f"准备发送订单: {direction.value} {volume} lots {symbol}@{price}")
        
        request_data = {
            "symbol": symbol,
            "exchange": exchange.value,
            "direction": direction.value,
            "type": OrderType.LIMIT.value,
            "volume": volume,
            "price": price,
            "offset": offset.value, # Specify offset if needed (e.g., Offset.CLOSE for closing positions)
            "reference": "SimpleZmqStrategy_01" # Strategy identifier
        }

        message = {
            "topic": "ORDER_REQUEST", # Generic topic for requests via PUSH/PULL
            "type": "ORDER_REQUEST",
            "source": request_data["reference"],
            "timestamp": time.time_ns(),
            "data": request_data
        }

        try:
            packed_message = msgpack.packb(message, use_bin_type=True)
            self.order_pusher.send(packed_message)
            self.last_order_time[symbol] = time.time() # Update last order time
            print(f"  订单请求已推送: {request_data}")

            # --- Update state after sending specific order --- 
            if symbol == "SA505" and direction == Direction.LONG and offset == Offset.OPEN:
                self.sa505_long_pending_or_open = True
                # +++ 记录入场价并计算目标/止损价 +++
                self.sa505_entry_price = price # 使用下单价格作为基准
                profit_target = 10.0 # 示例：止盈点数
                stop_loss = 5.0    # 示例：止损点数
                self.sa505_target_price = price + profit_target
                self.sa505_stop_price = price - stop_loss
                print(f"  设置 SA505 买入开仓状态: True, Entry: {self.sa505_entry_price:.1f}, Target: {self.sa505_target_price:.1f}, Stop: {self.sa505_stop_price:.1f}")
                # +++ 结束记录 +++

            # --- 重置状态 (如果发送的是平仓单) ---
            # 注意：更健壮的做法是根据成交回报来重置，这里简化处理
            elif symbol == "SA505" and offset == Offset.CLOSE:
                 # self.sa505_long_pending_or_open = False # Don't reset this here, reset on trade confirmation
                 # self.sa505_entry_price = 0.0 # Reset prices
                 # self.sa505_target_price = 0.0
                 # self.sa505_stop_price = 0.0
                 # --- 移除在此处设置等待平仓标志 --- 
                 # self.sa505_close_pending = True # Moved to closing logic block
                 # --- 结束移除 ---
                 # print("  设置 SA505 等待平仓标志为 True (平仓单已发送)...") # Log message removed/changed
                 pass # No specific action needed here now when sending close order

        except Exception as e:
            print(f"推送订单请求时出错: {e}")
            print(f"原始请求: {message}")

    def start(self):
        """Starts listening for messages and executing strategy logic."""
        if self.running:
            print("策略订阅器已在运行中。")
            return

        print("启动策略订阅器...")
        self.running = True

        # +++ 使用 Poller +++
        poller = zmq.Poller()
        poller.register(self.subscriber, zmq.POLLIN)
        poll_timeout_ms = 100 # 轮询超时（毫秒）
        # +++ 结束使用 +++

        while self.running:
            try:
                # +++ 使用 Poller 检查消息 +++
                sockets = dict(poller.poll(timeout=poll_timeout_ms))
                # +++ 结束检查 +++

                # --- 在处理消息前检查 self.running --- 
                if not self.running:
                    print("策略运行标志为 False，退出循环...")
                    break # 优先退出
                # --- 结束检查 ---

                # --- 如果 Poller 报告有消息 --- 
                if self.subscriber in sockets and sockets[self.subscriber] == zmq.POLLIN:
                    # Receive multipart message [topic, packed_data]
                    # 使用 NOBLOCK 因为 poller 确认了消息存在
                    topic, packed_message = self.subscriber.recv_multipart(zmq.NOBLOCK) 

                    # Deserialize the message payload
                    message = msgpack.unpackb(packed_message, raw=False)
                # --- 结束处理 Poller 消息 ---
                # --- 如果 Poller 超时且无消息，则继续循环检查 self.running ---
                else:
                    # No message received within timeout, continue loop to check self.running flag
                    continue 
                # --- 结束超时处理 ---

                # Identify message type based on topic or message content
                topic_str = topic.decode('utf-8')
                msg_type = message.get('type', 'UNKNOWN')
                msg_data = message.get('data', {})
                timestamp_ns = message.get('timestamp', 0)
                timestamp_sec = timestamp_ns / 1_000_000_000
                dt_object = datetime.fromtimestamp(timestamp_sec)
                pretty_time = dt_object.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

                # --- Process different message types --- 
                if msg_type == "TICK":
                    symbol = msg_data.get('vt_symbol', 'N/A')
                    price = msg_data.get('last_price', None)
                    ask_price = msg_data.get('ask_price_1', None)
                    bid_price = msg_data.get('bid_price_1', None) # 买一价
                    exchange_str = msg_data.get('exchange', None)

                    # print(f"[{pretty_time}] 行情 [{topic_str}] - 符号: {symbol}, 价格: {price}, 卖一: {ask_price}")

                    # --- Simple Strategy Logic with State Check --- 
                    # --- Strategy Logic for SA505.CZCE ---
                    if symbol == "SA505.CZCE" and exchange_str is not None:
                        # --- Opening Logic ---
                        if (price is not None and price > 1310.0 and # Use last_price for condition check
                            ask_price is not None and ask_price > 0 and # Ensure ask_price is valid for order
                            not self.sa505_long_pending_or_open):
                             try:
                                 # Make sure to use correct Exchange enum
                                 exchange_enum = Exchange(exchange_str)
                                 # Send a buy limit order using ask price
                                 self.send_limit_order(
                                     symbol="SA505",
                                     exchange=exchange_enum,
                                     direction=Direction.LONG,
                                     price=ask_price, # 使用卖一价尝试开仓
                                     volume=1,
                                     offset=Offset.OPEN # Assuming opening a position
                                 )
                             except ValueError as e:
                                 print(f"  创建开仓订单时交易所错误: {e}")
                             except Exception as e:
                                  print(f"  发送开仓订单时发生错误: {e}")

                        # --- Closing Logic (if holding long position) ---
                        elif self.sa505_long_pending_or_open and not self.sa505_close_pending and bid_price is not None and bid_price > 0:
                             should_close = False
                             reason = ""
                             # Check for target profit
                             if bid_price >= self.sa505_target_price:
                                  should_close = True
                                  reason = "Target Profit"
                             # Check for stop loss
                             elif bid_price <= self.sa505_stop_price:
                                  should_close = True
                                  reason = "Stop Loss"

                             if should_close:
                                 # +++ 在尝试发送前设置标志 +++
                                 self.sa505_close_pending = True
                                 print(f"[{pretty_time}] 触发平仓条件 ({reason}) for SA505 @ Bid={bid_price} (Target: {self.sa505_target_price:.1f}, Stop: {self.sa505_stop_price:.1f}). 设置等待平仓标志, 尝试发送平仓单...")
                                 # +++ 结束设置 +++
                                 try:
                                     exchange_enum = Exchange(exchange_str)
                                     # print(f"[{pretty_time}] 触发平仓条件 ({reason}) for SA505 @ Bid={bid_price} (Target: {self.sa505_target_price:.1f}, Stop: {self.sa505_stop_price:.1f}). 发送平仓单...") # Original log moved
                                     self.send_limit_order(
                                         symbol="SA505",
                                         exchange=exchange_enum,
                                         direction=Direction.SHORT, # 平多仓
                                         price=bid_price,        # 使用买一价尝试平仓
                                         volume=1,
                                         offset=Offset.CLOSE
                                     )
                                     # Flag reset is now handled inside send_limit_order after sending close
                                 except ValueError as e:
                                     print(f"  创建平仓订单时交易所错误: {e}")
                                 except Exception as e:
                                      print(f"  发送平仓订单时发生错误: {e}")

                    # --- Handle other symbols or add more logic ---
                    # elif symbol == "rb2510.SHFE":
                    #    # ... logic for rb2510 ...

                    elif symbol == "SA505.CZCE" and price is not None and price > 1310.0 and not self.sa505_long_pending_or_open:
                         # Condition met but ask_price is invalid
                         print(f"[{pretty_time}] WARN: {symbol} 价格满足条件 ({price} > 1310) 但卖一价无效 ({ask_price})，无法下单。")

                elif msg_type == "ORDER_STATUS":
                    order_id = msg_data.get('vt_orderid', 'N/A')
                    order_status = msg_data.get('status', 'N/A')
                    traded_vol = msg_data.get('traded', 0)
                    order_ref = msg_data.get('reference', '') # Assuming reference field exists in order data
                    symbol_from_order = msg_data.get('symbol', '') # Assuming symbol exists

                    print(f"[{pretty_time}] 订单回报 [{topic_str}] - ID: {order_id}, 状态: {order_status}, 成交量: {traded_vol}")
                    # +++ 如果平仓单被拒或撤销，重置 close_pending +++
                    # Also check offset to ensure it's a closing order status
                    if (symbol_from_order == "SA505" and
                        msg_data.get('offset') == Offset.CLOSE.value and # Check if it's a closing order
                        order_ref == "SimpleZmqStrategy_01" and # 确保是本策略的订单
                        self.sa505_close_pending and
                        order_status in [Status.REJECTED.value, Status.CANCELLED.value]):

                        self.sa505_close_pending = False
                        # Keep sa505_long_pending_or_open as True since the close failed
                        # self.sa505_long_pending_or_open = True # No need to reset this, it should still be true
                        print(f"  SA505 平仓订单 {order_id} 状态为 {order_status}, 重置等待平仓标志。持仓状态不变。")
                    # +++ 结束处理 +++

                elif msg_type == "TRADE":
                    trade_id = msg_data.get('vt_tradeid', 'N/A')
                    order_id = msg_data.get('vt_orderid', 'N/A')
                    trade_price = msg_data.get('price', 0.0) # Default to float
                    trade_vol = msg_data.get('volume', 0.0) # Default to float
                    direction = msg_data.get('direction', 'N/A')
                    offset = msg_data.get('offset', 'N/A') # Get offset info
                    commission = msg_data.get('commission', 0.0) # Get commission
                    symbol = msg_data.get('symbol', 'N/A') # Get symbol
                    exchange = msg_data.get('exchange', 'N/A') # Get exchange
                    trade_time_str = msg_data.get('datetime', None) # Get trade datetime str

                    print(f"[{pretty_time}] 成交回报 [{topic_str}] - TradeID: {trade_id}, OrderID: {order_id}, 方向: {direction}, 开平: {offset}, 价格: {trade_price}, 数量: {trade_vol}, 手续费: {commission:.2f}")

                    # +++ 收集成交记录 +++
                    try:
                        trade_record = {
                            "datetime": datetime.fromisoformat(trade_time_str) if trade_time_str else dt_object, # Use precise trade time if available
                            "symbol": symbol,
                            "exchange": exchange,
                            "vt_symbol": f"{symbol}.{exchange}",
                            "direction": direction,
                            "offset": offset,
                            "price": trade_price,
                            "volume": trade_vol,
                            "commission": commission,
                            "trade_id": trade_id,
                            "order_id": order_id
                        }
                        self.trades.append(trade_record)
                        # print(f"    成交记录已添加: {trade_record}") # Optional debug print

                        # +++ 如果是平仓成交，重置 close_pending 和持仓状态 +++
                        if symbol == "SA505" and offset == Offset.CLOSE.value:
                            self.sa505_close_pending = False
                            self.sa505_long_pending_or_open = False # Reset position state on successful close
                            self.sa505_entry_price = 0.0 # Reset prices
                            self.sa505_target_price = 0.0
                            self.sa505_stop_price = 0.0
                            print(f"  SA505 平仓成交 {trade_id} (OrderID: {order_id}), 重置等待平仓标志和持仓状态。")
                        # +++ 结束处理 +++

                    except Exception as e:
                        print(f"    添加到成交记录时出错: {e}. 数据: {msg_data}")
                    # +++ 结束收集 +++

                    # Update internal position state here... (If needed by strategy logic)

                else:
                    print(f"[{pretty_time}] 收到未知类型消息 [{topic_str}] - Type: {msg_type}")

            except zmq.ZMQError as e:
                # --- 区分预期关闭错误和意外错误 ---
                # 如果 stop() 已经被调用 (self.running is False)，这个错误是预期的
                if not self.running:
                    # 可以选择不打印，或者打印更温和的信息
                    print(f"捕获到预期的 ZMQ 错误 ({e.errno})，策略正在停止...")
                    # 不需要再次设置 self.running = False，循环将在下一次检查 self.running 时退出
                else:
                    # 如果 self.running 仍然是 True，说明是意外错误
                    print(f"捕获到意外的 ZMQ 错误 ({e.errno}): {e}")
                    if e.errno == zmq.ETERM:
                        print("  错误原因是 Context 已终止。")
                    # 在任何意外错误时停止循环
                    print("因意外 ZMQ 错误，停止策略订阅器循环...")
                    self.running = False # Ensure loop stops on unexpected error
                # --- 结束区分 ---
            except msgpack.UnpackException as e:
                print(f"Msgpack 解码错误: {e}. 跳过消息。")
            except KeyboardInterrupt:
                print("检测到中断信号，停止订阅器...")
                self.running = False
            except Exception as e:
                print(f"处理消息时发生未知错误: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(1)

        print("策略订阅器循环结束。")

    def stop(self):
        """Signals the subscriber to stop and closes both sockets. Context termination should be handled externally after thread join.""" # Docstring updated
        print("停止策略订阅器 (发送信号并关闭 sockets)...") # Modified log
        self.running = False # Signal the loop to stop

        # Close SUB Socket - This should interrupt recv_multipart
        if self.subscriber:
            try:
                print("关闭 subscriber socket...")
                self.subscriber.close()
                print("ZeroMQ subscriber socket 已关闭。")
            except Exception as e:
                print(f"关闭 ZeroMQ subscriber socket 时出错: {e}")
            finally:
                self.subscriber = None # Set to None regardless of success/failure
        
        # --- 恢复关闭 PUSH socket 的逻辑 --- 
        if self.order_pusher:
            try:
                print("关闭 order pusher socket...")
                self.order_pusher.close()
                print("ZeroMQ order pusher socket 已关闭。")
            except Exception as e:
                print(f"关闭 ZeroMQ order pusher socket 时出错: {e}")
            finally:
                self.order_pusher = None # Set to None
        # --- 结束恢复 --- 

        print("策略订阅器停止信号已发送，sockets 已关闭。") # Modified log


# --- Main execution block (for testing) ---
if __name__ == "__main__":
    # Use configuration from config.py
    # Replace '*' with 'localhost' or the appropriate IP for connection
    md_gateway_url = config.MARKET_DATA_PUB_URL.replace("*", "localhost")
    order_req_target_url = config.ORDER_REQUEST_PULL_URL.replace("*", "localhost")
    order_report_source_url = config.ORDER_REPORT_PUB_URL.replace("*", "localhost")

    symbols_to_sub = config.SUBSCRIBE_SYMBOLS

    subscriber_service = StrategySubscriber(
        gateway_pub_url=md_gateway_url,
        order_req_url=order_req_target_url,
        order_report_url=order_report_source_url,
        subscribe_symbols=symbols_to_sub
    )

    try:
        subscriber_service.start() # This now blocks until stopped
    except KeyboardInterrupt:
        print("\n主程序接收到中断信号。")
    finally:
        if subscriber_service.running:
            subscriber_service.stop()
        print("策略订阅器测试运行结束。")
