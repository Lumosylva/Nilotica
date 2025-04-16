import zmq
import msgpack
import time
import sys
import os
import json
from datetime import datetime
# Import logger
import logging
from logger import getLogger

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import local config
from config import zmq_config as config

# --- Data Recorder Service ---
class DataRecorderService:
    def __init__(self, market_data_url: str, order_report_url: str, recording_path: str):
        """Initializes the data recorder service."""
        self.logger = getLogger(__name__)

        self.context = zmq.Context()
        self.subscriber = self.context.socket(zmq.SUB)

        # Connect to publishers
        self.subscriber.connect(market_data_url)
        self.subscriber.connect(order_report_url)
        self.logger.info(f"数据记录器连接行情发布器: {market_data_url}")
        self.logger.info(f"数据记录器连接回报发布器: {order_report_url}")

        # Subscribe to ALL topics
        self.subscriber.subscribe("")
        self.logger.info("订阅所有主题 (*)")

        # Ensure recording directory exists
        self.recording_path = recording_path
        os.makedirs(self.recording_path, exist_ok=True)
        self.logger.info(f"数据将记录到: {self.recording_path}")

        # File handles (lazy opening or manage carefully)
        self.file_handles = {} # Store open file handles if needed, but appending might be safer

        self.running = False

    def get_log_filename(self, msg_type: str) -> str | None:
        """Determines the log filename based on message type."""
        today_str = datetime.now().strftime('%Y%m%d')
        if msg_type == "TICK":
            return os.path.join(self.recording_path, f"ticks_{today_str}.jsonl")
        elif msg_type == "ORDER_STATUS":
            return os.path.join(self.recording_path, f"orders_{today_str}.jsonl")
        elif msg_type == "TRADE":
            return os.path.join(self.recording_path, f"trades_{today_str}.jsonl")
        elif msg_type == "ORDER_REQUEST": # Also log requests if they are published
             return os.path.join(self.recording_path, f"requests_{today_str}.jsonl")
        else:
            # Optionally log unknown types to a separate file
            # return os.path.join(self.recording_path, f"unknown_{today_str}.jsonl")
            return None # Or ignore unknown types

    def record_message(self, topic: bytes, message: dict):
        """Records a received message to the appropriate file."""
        record_time = time.time_ns() # Record reception time
        msg_type = message.get('type', 'UNKNOWN')
        filename = self.get_log_filename(msg_type)

        if not filename:
            self.logger.debug(f"跳过记录未知类型的消息: {msg_type}")
            return

        record = {
            "zmq_topic": topic.decode('utf-8', errors='ignore'), # Decode topic safely
            "reception_timestamp_ns": record_time,
            "original_message": message # Store the full original message
        }

        try:
            # Append to file using 'a' mode, ensures atomicity for single writes on most OS
            with open(filename, 'a', encoding='utf-8') as f:
                json.dump(record, f, ensure_ascii=False) # Write as JSON line
                f.write('\n') # Add newline separator
        except IOError as e:
            self.logger.error(f"写入文件 {filename} 时出错: {e}")
        except Exception as e:
            self.logger.exception("记录消息时发生意外错误")
            self.logger.error(f"出错时的消息内容 (部分): {{'zmq_topic': record.get('zmq_topic'), 'type': message.get('type')}}") # Log only essential parts

    def start(self):
        """Starts listening for messages and recording them."""
        if self.running:
            self.logger.warning("数据记录器已在运行中。")
            return

        self.logger.info("启动数据记录器...")
        self.running = True

        while self.running:
            try:
                topic, packed_message = self.subscriber.recv_multipart()
                try:
                    message = msgpack.unpackb(packed_message, raw=False)
                    self.record_message(topic, message)
                except msgpack.UnpackException as e:
                    self.logger.error(f"Msgpack 解码错误: {e}. Topic: {topic.decode('utf-8', errors='ignore')}")
                    # Optionally log the raw packed_message for debugging

            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    self.logger.info("ZMQ Context 已终止，停止数据记录器...")
                    self.running = False
                else:
                    print(f"ZMQ 错误: {e}")
                    self.logger.error(f"ZMQ 错误: {e}")
                    time.sleep(1)
            except KeyboardInterrupt:
                self.logger.info("检测到中断信号，停止数据记录器...")
                self.running = False
            except Exception as e:
                print(f"处理消息时发生未知错误: {e}")
                self.logger.exception("处理消息时发生未知错误")
                time.sleep(1)

        self.logger.info("数据记录器循环结束。")
        self.stop()

    def stop(self):
        """Stops the service and cleans up resources."""
        self.logger.info("停止数据记录器...")
        self.running = False
        # Close subscriber first to prevent receiving more messages
        if self.subscriber:
            try:
                self.subscriber.close()
                self.logger.info("ZeroMQ 订阅器已关闭。")
            except Exception as e:
                 print(f"关闭 ZeroMQ 订阅器时出错: {e}")
                 self.logger.error(f"关闭 ZeroMQ 订阅器时出错: {e}")

        # Close context
        if self.context:
            try:
                # Check if context is already terminated before terminating again
                if not self.context.closed:
                    self.context.term()
                    self.logger.info("ZeroMQ Context 已终止。")
                else:
                    print("ZeroMQ Context 已终止 (之前已关闭).")
                    self.logger.info("ZeroMQ Context 已终止 (之前已关闭).")
            except zmq.ZMQError as e:
                 print(f"终止 ZeroMQ Context 时出错 (可能已终止): {e}")
                 self.logger.error(f"终止 ZeroMQ Context 时出错 (可能已终止): {e}")
            except Exception as e:
                 print(f"关闭 ZeroMQ Context 时发生未知错误: {e}")
                 self.logger.exception("关闭 ZeroMQ Context 时发生未知错误")

        # Close any open file handles (if managing them explicitly)
        # for f in self.file_handles.values():
        #     try:
        #         f.close()
        #     except Exception as e:
        #         print(f"关闭文件句柄时出错: {e}")
        # self.file_handles.clear()
        print("数据记录器已停止。")
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

    # Connect to localhost publishers
    md_url = config.MARKET_DATA_PUB_URL.replace("*", "localhost")
    report_url = config.ORDER_REPORT_PUB_URL.replace("*", "localhost")
    rec_path = config.DATA_RECORDING_PATH

    # Ensure the path uses correct OS separators (though os.path.join handles it)
    rec_path = os.path.abspath(rec_path)

    recorder = DataRecorderService(md_url, report_url, rec_path)

    try:
        recorder.start()
    except KeyboardInterrupt:
        logger_main.info("\n主程序接收到中断信号。")
    except Exception as e:
        logger_main.exception("主测试循环发生未处理错误")
    finally:
        # The start loop handles cleanup, but call stop for safety
        if recorder.running:
            recorder.stop()
        logger_main.info("数据记录器测试运行结束。")
