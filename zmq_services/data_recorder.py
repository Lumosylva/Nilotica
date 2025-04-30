from typing import Any

import zmq
import time
import sys
import os
import json
import pickle
from datetime import datetime
import struct
import threading
from collections import defaultdict
from utils.logger import logger
from vnpy.trader.object import TickData, OrderData, TradeData, AccountData, ContractData, LogData
from vnpy.trader.constant import Direction, OrderType, Exchange, Offset, Status, Product, OptionType

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import local config
from config import zmq_config as config

# +++ Batch Writing Configuration +++
# Use getattr to safely get values from config, with defaults
BATCH_WRITE_SIZE = getattr(config, 'RECORDER_BATCH_SIZE', 100)  # Records per flush
# +++ End Configuration +++

# --- Data Recorder Service ---
class DataRecorderService:
    def __init__(self, market_data_pub_addr: str, order_gateway_pub_addr: str, recording_path: str):
        """Initializes the data recorder service."""
        self.context = zmq.Context()
        self.subscriber = self.context.socket(zmq.SUB)
        self.subscriber.setsockopt(zmq.LINGER, 0) # Avoid blocking on close

        # Connect to publishers using the provided RPC PUB addresses
        self.subscriber.connect(market_data_pub_addr)
        self.subscriber.connect(order_gateway_pub_addr)
        logger.info(f"数据记录器连接行情发布器: {market_data_pub_addr}")
        logger.info(f"数据记录器连接回报发布器: {order_gateway_pub_addr}")

        # Subscribe to relevant topics (adjust as needed)
        self.subscriber.subscribe("tick.")       # Ticks from MD Gateway
        self.subscriber.subscribe("order.")      # Orders from Order Gateway
        self.subscriber.subscribe("trade.")      # Trades from Order Gateway
        self.subscriber.subscribe("account.")    # Account from Order Gateway
        self.subscriber.subscribe("contract.")   # Contracts from both?
        self.subscriber.subscribe("log")         # Logs from both
        logger.info("订阅主题前缀: tick., order., trade., account., contract., log")

        # Ensure recording directory exists
        self.recording_path = recording_path
        os.makedirs(self.recording_path, exist_ok=True)
        logger.info(f"数据将记录到: {self.recording_path}")

        # --- Batch Writing State ---
        # Restore initialization of batch writing variables
        self.file_batches: dict[str, list] = defaultdict(list) # Maps filename -> list of records
        self.batch_write_size: int = BATCH_WRITE_SIZE
        self.batch_lock = threading.Lock() # Lock for accessing file_batches
        # --- End Batch Writing State ---

        self.running = False
        # Log the instance's batch size
        logger.info(f"记录器批处理大小设置为: {self.batch_write_size}")

    # +++ Add Helper Function to Convert VNPY Objects +++
    def _convert_vnpy_obj_to_dict(self, obj: object) -> Any:
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
                        logger.warning(f"Could not convert items in list/tuple for key '{key}', using string representation.")
                        d[key] = str(value)
                elif not isinstance(value, (str, int, float, bool, dict, type(None))):
                    # Handle other potential non-serializable types
                    # Check if it has __dict__ as a generic approach
                    if hasattr(value, '__dict__'):
                         d[key] = self._convert_vnpy_obj_to_dict(value) # Recursive call
                    else:
                        logger.warning(f"Unhandled type '{type(value)}' for key '{key}', converting to string.")
                        d[key] = str(value)
            return d
        elif isinstance(obj, dict):
             # If it's already a dict, try converting its values recursively
             return {k: self._convert_vnpy_obj_to_dict(v) for k, v in obj.items()}
        elif isinstance(obj, (list, tuple)):
            try:
                 return [self._convert_vnpy_obj_to_dict(item) for item in obj]
            except TypeError:
                 logger.warning(f"Could not convert items in list/tuple, using string representation.")
                 return str(obj)
        elif isinstance(obj, (str, int, float, bool, type(None))):
            return obj # Basic types are fine
        elif isinstance(obj, datetime):
             return obj.isoformat() # Handle standalone datetime
        else:
            # Fallback for completely unknown types
            logger.warning(f"Unknown object type '{type(obj)}' encountered, converting to string.")
            return str(obj)
    # +++ End Helper Function +++

    def get_log_filename(self, topic: str) -> str | None:
        """Determines the log filename based on topic prefix."""
        logger.debug(f"Determining filename for topic: {topic}")
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
        elif topic == "__undecodable__": # Special topic name for undecodable file
            return os.path.join(self.recording_path, f"undecodable_{today_str}.bin")
        else:
            logger.warning(f"收到未知主题，不记录: {topic}")
            return None

    @staticmethod
    def _write_batch_to_file(filename: str, batch: list):
        """
        Internal method to write a given batch of records for a specific file to disk.
        """
        if not batch:
            return

        # Write the batch to file
        try:
            with open(filename, 'a', encoding='utf-8', newline='\n') as f:
                for record in batch:
                    json.dump(record, f, ensure_ascii=False)
                    f.write('\n')
                f.flush()
            logger.debug(f"成功刷写 {len(batch)} 条记录到 {filename}")
        except (IOError, OSError) as e_io:
            logger.error(f"刷写批处理到文件 {filename} 时出错: {e_io}")
            # TODO: Consider how to handle write errors (e.g., retry, log failed records?)
            # For now, the batch data is lost if writing fails.
        except Exception as e_flush:
            logger.exception(f"刷写批处理到文件 {filename} 时发生意外错误: {e_flush}")

    def record_message(self, topic_bytes: bytes, data_bytes: bytes):
        """Converts and adds a message to the appropriate batch, flushing if size is reached."""
        record_time = time.time_ns()
        topic_str = ""
        filename = None
        data_obj = None
        record_data = None
        batch_to_flush_data = None # Store popped batch data
        filename_to_flush = None # Store filename for the popped batch

        try:
            topic_str = topic_bytes.decode('utf-8', errors='ignore')
            filename = self.get_log_filename(topic_str)

            if not filename:
                return

            data_obj = pickle.loads(data_bytes)
            record_data = self._convert_vnpy_obj_to_dict(data_obj)

            record = {
                "zmq_topic": topic_str,
                "reception_timestamp_ns": record_time,
                "data": record_data
            }

            # --- Add to batch and check size under lock --- 
            with self.batch_lock:
                batch_list = self.file_batches[filename]
                batch_list.append(record)
                if len(batch_list) >= self.batch_write_size:
                    # Pop the batch list from the dict to flush it outside the lock
                    batch_to_flush_data = self.file_batches.pop(filename)
                    filename_to_flush = filename
            # --- End Add to batch --- 

            # --- Flush outside the lock if a batch was popped --- 
            if batch_to_flush_data and filename_to_flush:
                 self._write_batch_to_file(filename_to_flush, batch_to_flush_data)
            # --- End Flush --- 

        except pickle.UnpicklingError as e_pickle:
            logger.error(f"Pickle 解码错误: {e_pickle}. Topic bytes: {topic_bytes!r}")
            # --- Save undecodable message to binary file --- 
            undecodable_filename = self.get_log_filename("__undecodable__")
            if undecodable_filename:
                try:
                    with open(undecodable_filename, 'ab') as f_err:
                        # Write timestamp (e.g., 8 bytes integer)
                        f_err.write(struct.pack('!Q', record_time)) 
                        # Write topic length (e.g., 4 bytes integer) + topic bytes
                        f_err.write(struct.pack('!I', len(topic_bytes)))
                        f_err.write(topic_bytes)
                        # Write data length (e.g., 4 bytes integer) + data bytes
                        f_err.write(struct.pack('!I', len(data_bytes)))
                        f_err.write(data_bytes)
                    logger.info(f"无法解码的消息已保存到 {undecodable_filename}")
                except Exception as e_write_err:
                    logger.exception(f"保存无法解码的消息到 {undecodable_filename} 时出错: {e_write_err}")
            # --- End Save undecodable --- 

        except TypeError as e_json:
            logger.error(f"JSON 序列化错误: {e_json}. Topic: {topic_str}")
            # Log details about the problematic data
            logger.error(f"  Data object type before conversion: {type(data_obj)}")
            logger.error(f"  Converted data type causing error: {type(record_data)}")
            try:
                # Try logging the problematic data representation (truncated)
                problematic_data_repr = repr(record_data)
                logger.error(f"  Problematic Converted Data (repr, first 500 chars): {problematic_data_repr[:500]}...")
                # Optionally, try iterating dict items if it's a dict
                if isinstance(record_data, dict):
                     problem_keys = []
                     for k, v in record_data.items():
                         try:
                             json.dumps({k: v}) # Test individual item serialization
                         except TypeError:
                             problem_keys.append(f"{k} (type: {type(v)})")
                     if problem_keys:
                         logger.error(f"  Potential problematic keys/types: {problem_keys}")
            except Exception as e_log_detail:
                 logger.error(f"  记录详细错误信息时也发生错误: {e_log_detail}")

        except IOError as e_io:
            logger.error(f"获取日志文件名或初步IO操作时出错 for topic {topic_str}: {e_io}")
        except Exception as e_general:
            context_info = f"Filename: {filename}" if filename else f"Topic: {topic_str}"
            logger.exception(f"处理消息准备记录时发生意外错误 ({context_info}): {e_general}")

    def start(self):
        """Starts listening for messages and recording them."""
        if self.running:
            logger.warning("数据记录器已在运行中。")
            return

        logger.info("启动数据记录器 (批处理模式)...")
        self.running = True

        while self.running:
            try:
                topic_bytes, data_bytes = self.subscriber.recv_multipart()
                self.record_message(topic_bytes, data_bytes)

            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    logger.info("ZMQ Context 已终止，停止数据记录器...")
                    self.running = False # Ensure loop terminates
                else:
                    logger.error(f"ZMQ 错误: {e}")
                    # Consider breaking loop on persistent ZMQ errors?
                    time.sleep(1) # Add back sleep for ZMQ errors
            except KeyboardInterrupt:
                logger.info("检测到中断信号，停止数据记录器...")
                self.running = False
            except Exception as err:
                logger.exception(f"处理消息时发生未知错误：{err}")
                time.sleep(1) # Add back sleep for general errors

        logger.info("数据记录器主循环结束。")
        self.stop()

    def stop(self):
        """Stops the service, flushes remaining batches, and cleans up resources."""
        logger.info("停止数据记录器...")
        if not self.running and not self.file_batches:
             logger.debug("Data recorder already stopped and no batches to flush.")
             return
        self.running = False
        # --- Flush all remaining batches ---
        logger.info(f"准备刷写剩余的数据批次 (共 {len(self.file_batches)} 个文件)...")
        with self.batch_lock:
            filenames = list(self.file_batches.keys())
            # Use list comprehension to pop all batches under lock first
            batches_to_flush = [(fname, self.file_batches.pop(fname)) for fname in filenames]
            # Now file_batches dict is empty (under lock)
        
        # Flush all popped batches outside the lock
        for filename, batch_data in batches_to_flush:
            self._write_batch_to_file(filename, batch_data)
        logger.info("所有剩余数据批次已刷写。")
        # --- End Flush ---

        # --- Restore ZMQ socket close and context termination ---
        logger.info("关闭 ZMQ sockets 和 context...")
        if self.subscriber:
            try:
                self.subscriber.close()
                logger.info("ZeroMQ 订阅器已关闭。")
            except Exception as e_sub_close:
                 logger.error(f"关闭 ZeroMQ 订阅器时出错: {e_sub_close}")
        
        if self.context:
            try:
                if not self.context.closed:
                    # Give sockets a moment to close before terminating context
                    time.sleep(0.1)
                    self.context.term()
                    logger.info("ZeroMQ Context 已终止。")
                # else:
                    # logger.info("ZeroMQ Context 已终止 (之前已关闭).") # Can be noisy
            except zmq.ZMQError as e_term:
                 # Log specific ZMQ termination error
                 logger.error(f"终止 ZeroMQ Context 时出错: {e_term}")
            except Exception as e_term_general:
                 logger.exception(f"关闭 ZeroMQ Context 时发生未知错误：{e_term_general}")
        # --- End Restore --- 
        logger.info("数据记录器已停止。")
