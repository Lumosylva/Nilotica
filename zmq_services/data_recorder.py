import json
import os
import pickle
import struct
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime
from typing import Any

import msgpack  # Add import
import zmq

# +++ Import ConfigManager +++
from utils.config_manager import ConfigManager

# +++ Import the converter function +++
# from .zmq_base import convert_vnpy_obj_to_dict # REMOVED (Relative import)
from utils.converter import convert_vnpy_obj_to_dict  # UPDATED IMPORT
from utils.logger import logger
from vnpy.trader.constant import Direction, Exchange, Offset, OptionType, OrderType, Product, Status
from vnpy.trader.object import AccountData, ContractData, LogData, OrderData, TickData, TradeData

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# --- Remove old config import ---
# from config import zmq_config as config

# --- Remove old module-level BATCH_WRITE_SIZE definition ---
# BATCH_WRITE_SIZE = getattr(config, 'RECORDER_BATCH_SIZE', 100)  # Records per flush

# --- Data Recorder Service ---
class DataRecorderService:
    def __init__(self, config_manager: ConfigManager, market_data_pub_addr: str, order_gateway_pub_addr: str, recording_path: str):
        """Initializes the data recorder service."""
        # +++ Use passed ConfigManager instance +++
        self.config_service = config_manager
        # --- Remove self.config_service = ConfigManager() ---

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
        # +++ Get batch_write_size from ConfigManager +++
        self.batch_write_size: int = self.config_service.get_global_config("service_settings.recorder_batch_size", 100)
        self.batch_lock = threading.Lock() # Lock for accessing file_batches
        # --- End Batch Writing State ---

        self.running = False
        # Log the instance's batch size
        logger.info(f"记录器批处理大小设置为: {self.batch_write_size}")

    def get_log_filename(self, topic: str) -> str | None:
        """Determines the log filename based on topic prefix."""
        logger.debug(f"Determining filename for topic: {topic}")
        today_str = datetime.now().strftime('%Y%m%d')
        # Use topic prefixes to determine file
        if topic.startswith("tick."):
            # --- FIX: Extract symbol and create separate file ---
            try:
                parts = topic.split('.')
                if len(parts) >= 3: # Expecting tick.SYMBOL.EXCHANGE
                    vt_symbol = f"{parts[1]}.{parts[2]}" # Reconstruct vt_symbol
                    # Sanitize vt_symbol for filename (replace . with _ for better compatibility)
                    sanitized_symbol = vt_symbol.replace('.', '_')
                    filename = os.path.join(self.recording_path, f"ticks_{sanitized_symbol}_{today_str}.jsonl")
                    # No need for debug log here every time, it can be noisy.
                    return filename
                else:
                    logger.warning(f"无法从 tick 主题解析合约代码: {topic}. 将使用通用 Ticks 文件名。")
                    # Fallback to generic filename if parsing fails
                    return os.path.join(self.recording_path, f"ticks_{today_str}.jsonl")
            except Exception as e:
                 logger.exception(f"解析 tick 主题 '{topic}' 时出错: {e}. 将使用通用 Ticks 文件名。")
                 return os.path.join(self.recording_path, f"ticks_{today_str}.jsonl")
            # --- End FIX ---
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
            # logger.warning(f"收到未知主题，不记录: {topic}") # Can be noisy, disable if needed
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

            # Replace pickle.loads with msgpack.unpackb
            # data_obj = pickle.loads(data_bytes)
            data_obj = msgpack.unpackb(data_bytes, raw=False) # Use raw=False for auto string decoding

            # +++ Call the imported function +++
            # record_data = self._convert_vnpy_obj_to_dict(data_obj) # Old call removed
            record_data = convert_vnpy_obj_to_dict(data_obj) # Call imported function
            # +++ End call +++

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

        except (msgpack.UnpackException, msgpack.exceptions.ExtraData, TypeError, ValueError) as e_msgpack: # Catch broader msgpack errors
            logger.error(f"Msgpack 解码错误: {e_msgpack}. Topic bytes: {topic_bytes!r}")
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
