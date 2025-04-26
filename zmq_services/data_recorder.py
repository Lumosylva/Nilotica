import zmq
import msgpack
import time
import sys
import os
import json
import pickle
from datetime import datetime
# Import logger
import logging
from logger import getLogger
# Import vnpy objects and constants needed for type checking and enum conversion
from vnpy.trader.object import TickData, OrderData, TradeData, AccountData, ContractData, LogData
from vnpy.trader.constant import Direction, OrderType, Exchange, Offset, Status, Product, OptionType # Remove PriceType

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import local config
from config import zmq_config as config

# --- Data Recorder Service ---
class DataRecorderService:
    def __init__(self, market_data_pub_addr: str, order_gateway_pub_addr: str, recording_path: str):
        """Initializes the data recorder service."""
        self.logger = getLogger(__name__)

        self.context = zmq.Context()
        self.subscriber = self.context.socket(zmq.SUB)
        self.subscriber.setsockopt(zmq.LINGER, 0) # Avoid blocking on close

        # Connect to publishers using the provided RPC PUB addresses
        self.subscriber.connect(market_data_pub_addr)
        self.subscriber.connect(order_gateway_pub_addr)
        self.logger.info(f"数据记录器连接行情发布器: {market_data_pub_addr}")
        self.logger.info(f"数据记录器连接回报发布器: {order_gateway_pub_addr}")

        # Subscribe to relevant topics (adjust as needed)
        self.subscriber.subscribe("tick.")       # Ticks from MD Gateway
        self.subscriber.subscribe("order.")      # Orders from Order Gateway
        self.subscriber.subscribe("trade.")      # Trades from Order Gateway
        self.subscriber.subscribe("account.")    # Account from Order Gateway
        self.subscriber.subscribe("contract.")   # Contracts from both?
        self.subscriber.subscribe("log")         # Logs from both
        self.logger.info("订阅主题前缀: tick., order., trade., account., contract., log")

        # Ensure recording directory exists
        self.recording_path = recording_path
        os.makedirs(self.recording_path, exist_ok=True)
        self.logger.info(f"数据将记录到: {self.recording_path}")
        self.file_handles = {}
        self.running = False

    # +++ Add Helper Function to Convert VNPY Objects +++
    def _convert_vnpy_obj_to_dict(self, obj: object) -> dict | str:
        """Converts known VNPY objects to JSON-serializable dicts."""
        if isinstance(obj, (TickData, OrderData, TradeData, AccountData, ContractData, LogData)):
            d = obj.__dict__.copy() # Use a copy to avoid modifying original object's dict
            for key, value in d.items():
                if isinstance(value, (Direction, OrderType, Exchange, Offset, Status, Product, OptionType)):
                    d[key] = value.value if value else None # Convert Enum to its value
                elif isinstance(value, datetime):
                    d[key] = value.isoformat() if value else None # Convert datetime to ISO string
                elif isinstance(value, (list, tuple)):
                    # Recursively convert items in lists/tuples if they are complex objects (basic implementation)
                    try:
                        d[key] = [self._convert_vnpy_obj_to_dict(item) for item in value]
                    except TypeError: # Handle cases where list contains non-convertible items
                        self.logger.warning(f"Could not convert items in list/tuple for key '{key}', using string representation.")
                        d[key] = str(value)
                elif not isinstance(value, (str, int, float, bool, dict, type(None))):
                    # Handle other potential non-serializable types
                    # Check if it has __dict__ as a generic approach
                    if hasattr(value, '__dict__'):
                         d[key] = self._convert_vnpy_obj_to_dict(value) # Recursive call
                    else:
                        self.logger.warning(f"Unhandled type '{type(value)}' for key '{key}', converting to string.")
                        d[key] = str(value)
            return d
        elif isinstance(obj, dict):
             # If it's already a dict, try converting its values recursively
             return {k: self._convert_vnpy_obj_to_dict(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            try:
                 return [self._convert_vnpy_obj_to_dict(item) for item in obj]
            except TypeError:
                 self.logger.warning(f"Could not convert items in list/tuple, using string representation.")
                 return str(obj)
        elif isinstance(obj, (str, int, float, bool, type(None))):
            return obj # Basic types are fine
        elif isinstance(obj, datetime):
             return obj.isoformat() # Handle standalone datetime
        else:
            # Fallback for completely unknown types
            self.logger.warning(f"Unknown object type '{type(obj)}' encountered, converting to string.")
            return str(obj)
    # +++ End Helper Function +++

    def get_log_filename(self, topic: str) -> str | None:
        """Determines the log filename based on topic prefix."""
        self.logger.debug(f"[DR] Determining filename for topic: {topic}")
        today_str = datetime.now().strftime('%Y%m%d')
        # Use topic prefixes to determine file
        if topic.startswith("tick."):
            return os.path.join(self.recording_path, f"ticks_{today_str}.jsonl")
        elif topic.startswith("order."):
            return os.path.join(self.recording_path, f"orders_{today_str}.jsonl")
        elif topic.startswith("trade."):
            return os.path.join(self.recording_path, f"trades_{today_str}.jsonl")
        elif topic.startswith("account."):
            return os.path.join(self.recording_path, f"accounts_{today_str}.jsonl")
        elif topic.startswith("contract."):
             return os.path.join(self.recording_path, f"contracts_{today_str}.jsonl")
        elif topic == "log":
             return os.path.join(self.recording_path, f"gateway_logs_{today_str}.jsonl")
        else:
            self.logger.warning(f"收到未知主题，不记录: {topic}")
            return None

    def record_message(self, topic_bytes: bytes, data_bytes: bytes):
        """Records a received message (topic, data) to the appropriate file."""
        self.logger.debug(f"[DR] Entering record_message. Topic bytes: {topic_bytes}, Data length: {len(data_bytes)}")
        record_time = time.time_ns() # Record reception time
        topic_str = topic_bytes.decode('utf-8', errors='ignore')
        filename = self.get_log_filename(topic_str)
        self.logger.debug(f"[DR] Determined filename: {filename}")

        if not filename:
            return # Skip recording unknown topics

        try:
            # Deserialize data using pickle (since RpcServer uses send_pyobj)
            data_obj = pickle.loads(data_bytes)
            self.logger.debug(f"[DR] Deserialized data object type: {type(data_obj)}")

            # Use the helper function for conversion
            record_data = self._convert_vnpy_obj_to_dict(data_obj)
            # Log partial converted data for verification
            if isinstance(record_data, dict):
                 log_snippet = {k: record_data.get(k) for k in list(record_data)[:5]}
                 self.logger.debug(f"[DR] Converted record_data (snippet): {log_snippet}")
            else:
                 self.logger.debug(f"[DR] Converted record_data (non-dict): {str(record_data)[:100]}...")

            record = {
                "zmq_topic": topic_str,
                "reception_timestamp_ns": record_time,
                "data": record_data # Store the deserialized and dictionary-converted data
            }

            # Append to file using 'a' mode
            self.logger.debug(f"[DR] Attempting to write to {filename}")
            with open(filename, 'a', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False)
                f.write('\n')
            self.logger.debug(f"[DR] Successfully wrote to {filename}")

        except pickle.UnpicklingError as e:
             self.logger.error(f"[DR] Pickle 解码错误: {e}. Topic: {topic_str}")
        except TypeError as e:
            # Log TypeError specifically from json.dump
            self.logger.error(f"[DR] JSON 序列化错误 (可能由于不支持的数据类型): {e}. Topic: {topic_str}")
            self.logger.error(f"[DR] Data object type: {type(data_obj)}")
            self.logger.error(f"[DR] Converted data causing error (first 500 chars): {str(record_data)[:500]}") # Log problematic data
        except IOError as e:
            # Log IOError specifically
            self.logger.error(f"[DR] 写入文件 {filename} 时出错: {e}")
        except Exception as e:
            self.logger.exception(f"[DR] 记录消息时发生意外错误 (Topic: {topic_str})")

    def start(self):
        """Starts listening for messages and recording them."""
        if self.running:
            self.logger.warning("数据记录器已在运行中。")
            return

        self.logger.info("启动数据记录器...")
        self.running = True

        while self.running:
            try:
                # Receive topic and data bytes
                topic_bytes, data_bytes = self.subscriber.recv_multipart()
                self.record_message(topic_bytes, data_bytes)

            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    self.logger.info("ZMQ Context 已终止，停止数据记录器...")
                    self.running = False
                else:
                    self.logger.error(f"ZMQ 错误: {e}")
                    time.sleep(1)
            except KeyboardInterrupt:
                self.logger.info("检测到中断信号，停止数据记录器...")
                self.running = False
            except Exception as e:
                self.logger.exception("处理消息时发生未知错误")
                time.sleep(1)

        self.logger.info("数据记录器循环结束。")
        self.stop()

    def stop(self):
        """Stops the service and cleans up resources."""
        self.logger.info("停止数据记录器...")
        self.running = False
        if self.subscriber:
            try:
                self.subscriber.close()
                self.logger.info("ZeroMQ 订阅器已关闭。")
            except Exception as e:
                 self.logger.error(f"关闭 ZeroMQ 订阅器时出错: {e}")
        if self.context:
            try:
                if not self.context.closed:
                    self.context.term()
                    self.logger.info("ZeroMQ Context 已终止。")
                else:
                    self.logger.info("ZeroMQ Context 已终止 (之前已关闭).")
            except zmq.ZMQError as e:
                 self.logger.error(f"终止 ZeroMQ Context 时出错 (可能已终止): {e}")
            except Exception as e:
                 self.logger.exception("关闭 ZeroMQ Context 时发生未知错误")
        self.logger.info("数据记录器已停止。")


# --- Main execution block (for testing) ---
if __name__ == "__main__":
    # Setup logging for direct execution test
    try:
        from logger import setup_logging, getLogger
        setup_logging(service_name="DataRecorder_DirectRun")
    except ImportError as log_err:
        print(f"CRITICAL: Failed to import or setup logger: {log_err}. Exiting.")
        sys.exit(1)

    logger_main = getLogger(__name__)

    logger_main.info("Starting Data Recorder direct test run...")

    # Connect to localhost publishers using RPC addresses
    md_pub_addr = config.MARKET_DATA_PUB_ADDRESS.replace("*", "localhost")
    order_pub_addr = config.ORDER_GATEWAY_PUB_ADDRESS.replace("*", "localhost")
    rec_path = config.DATA_RECORDING_PATH
    rec_path = os.path.abspath(rec_path)

    # Use the updated addresses
    recorder = DataRecorderService(md_pub_addr, order_pub_addr, rec_path)

    try:
        recorder.start()
    except KeyboardInterrupt:
        logger_main.info("\n主程序接收到中断信号。")
    except Exception as e:
        logger_main.exception("主测试循环发生未处理错误")
    finally:
        if recorder.running:
            recorder.stop()
        logger_main.info("数据记录器测试运行结束。")
