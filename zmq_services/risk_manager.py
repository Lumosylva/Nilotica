import zmq
import msgpack
import time
import sys
import os
from datetime import datetime

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import local config
from . import config

# Import necessary vnpy constants
try:
    from vnpy.trader.constant import Direction
except ImportError:
    print("无法导入 vnpy.trader.constant.Direction，请确保 vnpy 已安装。")
    # Define fallback constants if vnpy is not fully available in this environment
    class Direction:
        LONG = "多"
        SHORT = "空"

# --- Risk Manager Service ---
class RiskManagerService:
    def __init__(self, market_data_url: str, order_report_url: str, position_limits: dict):
        """Initializes the risk manager service."""
        self.context = zmq.Context()
        self.subscriber = self.context.socket(zmq.SUB)

        # Connect to both publishers
        self.subscriber.connect(market_data_url)
        self.subscriber.connect(order_report_url)
        print(f"风险管理器连接行情发布器: {market_data_url}")
        print(f"风险管理器连接回报发布器: {order_report_url}")

        # Subscribe to TICKs (optional, for market risk later) and TRADEs
        tick_prefix = "TICK."
        trade_prefix = "TRADE."
        self.subscriber.subscribe(tick_prefix.encode('utf-8'))
        self.subscriber.subscribe(trade_prefix.encode('utf-8'))
        print(f"订阅主题: {tick_prefix}* , {trade_prefix}*")

        # --- State --- 
        self.positions = {} # vt_symbol -> net position (int)
        self.position_limits = position_limits
        print(f"加载持仓限制: {self.position_limits}")

        self.running = False

    def update_position(self, trade_data: dict):
        """Updates position based on trade data."""
        vt_symbol = trade_data.get('vt_symbol')
        direction = trade_data.get('direction')
        volume = trade_data.get('volume') # Should be positive
        offset = trade_data.get('offset') # Useful for more precise position logic (e.g., handling close today)

        if not all([vt_symbol, direction, volume]):
            print("错误：成交回报缺少关键字段 (vt_symbol, direction, volume)")
            return None, None

        # Calculate position change
        pos_change = 0
        if direction == Direction.LONG.value:
            pos_change = volume
        elif direction == Direction.SHORT.value:
            pos_change = -volume
        else:
            print(f"错误：未知的成交方向 '{direction}'")
            return None, None

        # Update position map
        current_pos = self.positions.get(vt_symbol, 0)
        new_pos = current_pos + pos_change
        self.positions[vt_symbol] = new_pos
        print(f"持仓更新: {vt_symbol} | 旧: {current_pos} | 变动: {pos_change} | 新: {new_pos}")

        return vt_symbol, new_pos

    def check_risk(self, vt_symbol: str, current_position: int):
        """Checks if the position exceeds the defined limit."""
        if vt_symbol is None or current_position is None:
            return
            
        limit = self.position_limits.get(vt_symbol)
        if limit is None:
            # print(f"注意: {vt_symbol} 未设置持仓限制，跳过检查。")
            return # No limit set for this symbol

        # Check absolute position against the limit
        if abs(current_position) > limit:
            print("!!! 风险告警 !!!")
            print(f"    合约: {vt_symbol}")
            print(f"    当前持仓: {current_position}")
            print(f"    持仓限制: {limit}")
            print("    已超过最大持仓限制！")
            print("!!!!!!!!!!!!!!!!")
        # else:
            # print(f"持仓检查: {vt_symbol} | 当前: {current_position} | 限制: {limit} | OK")
            
    def start(self):
        """Starts listening for messages and performing risk checks."""
        if self.running:
            print("风险管理器已在运行中。")
            return

        print("启动风险管理器...")
        self.running = True

        while self.running:
            try:
                topic, packed_message = self.subscriber.recv_multipart()
                message = msgpack.unpackb(packed_message, raw=False)

                msg_type = message.get('type', 'UNKNOWN')
                msg_data = message.get('data', {})
                timestamp_ns = message.get('timestamp', 0)
                timestamp_sec = timestamp_ns / 1_000_000_000
                dt_object = datetime.fromtimestamp(timestamp_sec)
                pretty_time = dt_object.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

                if msg_type == "TRADE":
                    print(f"[{pretty_time}] 收到成交回报: {msg_data.get('vt_symbol')}")
                    symbol, updated_pos = self.update_position(msg_data)
                    self.check_risk(symbol, updated_pos)

                elif msg_type == "TICK":
                    # Optional: Process ticks for market risk checks later
                    # print(f"[{pretty_time}] 收到行情: {msg_data.get('vt_symbol')}")
                    pass 
                
                # Ignore other message types for now

            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    print("ZMQ Context 已终止，停止风险管理器...")
                    self.running = False
                else:
                    print(f"ZMQ 错误: {e}")
                    time.sleep(1)
            except msgpack.UnpackException as e:
                print(f"Msgpack 解码错误: {e}. 跳过消息。")
            except KeyboardInterrupt:
                print("检测到中断信号，停止风险管理器...")
                self.running = False
            except Exception as e:
                print(f"处理消息时发生未知错误: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(1)

        print("风险管理器循环结束。")
        self.stop() # Ensure cleanup

    def stop(self):
        """Stops the service and cleans up resources."""
        print("停止风险管理器...")
        self.running = False
        if self.subscriber:
            self.subscriber.close()
            print("ZeroMQ 订阅器已关闭。")
        if self.context:
            try:
                self.context.term()
                print("ZeroMQ Context 已终止。")
            except zmq.ZMQError as e:
                 print(f"终止 ZeroMQ Context 时出错 (可能已终止): {e}")
        print("风险管理器已停止。")

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
