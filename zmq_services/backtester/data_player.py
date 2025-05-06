import zmq
import msgpack
import time
import sys
import os
import json
import glob
from datetime import datetime, time as dt_time
import heapq # For efficient sorting/merging if loading multiple project_files
import pickle
# +++ Add Logger Import +++
from utils.logger import logger # Assuming logger is configured elsewhere
# +++ Correct Config Import +++
# from zmq_services import config
from config import zmq_config as config
# +++ End Correction +++

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')) # Go up two levels
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import local config (need to adjust relative path)
# try:
#     from zmq_services import config
# except ImportError:
#      print("无法导入 zmq_services.config。请确保路径设置正确。")
     # Define fallback config values if necessary for testing standalone
# --- Use the same top-level config import as run_data_player --- 
# from config import zmq_config as config
# --- End Use top-level import ---

# --- Data Player Service ---
class DataPlayerService:
    def __init__(self, data_source_path: str, backtest_pub_url: str, date_str: str):
        """
        Initializes the Data Player service.
        :param data_source_path: Path to the directory containing recorded data project_files.
        :param backtest_pub_url: ZMQ URL to publish backtest market data.
        :param date_str: The date string (YYYYMMDD) for which to play back data.
        """
        self.context = zmq.Context()
        self.publisher = self.context.socket(zmq.PUB)
        self.publisher.bind(backtest_pub_url)
        logger.info(f"回测数据发布器绑定到: {backtest_pub_url}")

        self.data_source_path = data_source_path
        self.backtest_pub_url = backtest_pub_url
        self.date_str = date_str
        self.tick_data_file = os.path.join(self.data_source_path, f"ticks_{self.date_str}.jsonl")

        self.all_ticks = [] # List to hold all tick data [(original_ts_ns, topic_bytes, packed_message_bytes)]
        self.running = False

    def load_data(self) -> bool:
        """Loads tick data from the specified single file."""
        # --- Reverted logic to load single combined file --- 
        logger.info(f"尝试从 {self.tick_data_file} 加载数据...")
        if not os.path.exists(self.tick_data_file):
            logger.error(f"错误: 数据文件不存在: {self.tick_data_file}")
            return False

        self.all_ticks = [] # Ensure list is clear before loading
        loaded_count = 0
        total_lines = 0

        logger.info(f"  正在加载文件: {self.tick_data_file}")
        try:
            with open(self.tick_data_file, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    total_lines += 1
                    try:
                        record = json.loads(line.strip())
                        # Use the inner 'data' field which is the dict representation of the VNPY object
                        data_dict = record.get("data")
                        if not isinstance(data_dict, dict):
                            logger.warning(f"[{os.path.basename(self.tick_data_file)}:{line_num}] 跳过记录，'data' 字段不是字典: {record}")
                            continue

                        original_ts_ns = record.get("reception_timestamp_ns") # Use reception time
                        topic_str = record.get("zmq_topic")

                        if not original_ts_ns or not topic_str:
                            logger.warning(f"[{os.path.basename(self.tick_data_file)}:{line_num}] 跳过记录，缺少时间戳或主题: {record}")
                            continue

                        # --- Serialize the data_dict using msgpack --- 
                        try:
                            # No need to convert again, 'data' is already the dict from convert_vnpy_obj_to_dict
                            msgpacked_data_bytes = msgpack.packb(data_dict, use_bin_type=True)
                        except (msgpack.PackException, TypeError) as pe:
                            logger.warning(f"[{os.path.basename(self.tick_data_file)}:{line_num}] Msgpack 序列化错误: {pe}. Data: {data_dict}")
                            continue # Skip this record
                        # --- End Serialize ---
                             
                        topic_bytes = topic_str.encode('utf-8')
                        self.all_ticks.append((original_ts_ns, topic_bytes, msgpacked_data_bytes))
                        loaded_count += 1

                    except json.JSONDecodeError as e:
                        logger.warning(f"[{os.path.basename(self.tick_data_file)}:{line_num}] 解析 JSON 行时出错: {e}. 行: {line.strip()}")
                    except Exception as e_line:
                        logger.warning(f"[{os.path.basename(self.tick_data_file)}:{line_num}] 处理记录时发生未知错误: {e_line}. 记录: {record}")
        except IOError as e_io:
            logger.error(f"读取文件 {self.tick_data_file} 时出错: {e_io}")
            return False # Stop if file reading fails
        except Exception as e_file:
            logger.exception(f"加载文件 {self.tick_data_file} 时发生未知错误: {e_file}")
            return False # Stop on other file loading errors
        # --- End Reverted logic ---

        if not self.all_ticks:
            logger.error(f"错误：未能从文件 {self.tick_data_file} 中加载任何有效的 Tick 数据 (共处理 {total_lines} 行)。")
            return False

        # Sort data by original timestamp
        self.all_ticks.sort(key=lambda x: x[0])
        logger.info(f"数据加载完成并排序。总共 {loaded_count} 条有效 Tick 数据 (来自文件: {os.path.basename(self.tick_data_file)}, 共 {total_lines} 行)。")
        return True

    def start_playback(self, playback_speed: float = 0):
        """
        Starts playing back the loaded data.
        :param playback_speed: Multiplier for playback speed.
                               0 = Max speed (no delay).
                               1 = Real-time speed (approx).
                               > 1 = Faster than real-time.
                               < 1 = Slower than real-time.
        """
        if not self.all_ticks:
            logger.error("错误: 没有数据可供回放。请先调用 load_data()。")
            return

        logger.info(f"开始数据回放 (速度: {playback_speed if playback_speed > 0 else '最大'})... 按 Ctrl+C 停止。")
        self.running = True
        start_time_ns = time.time_ns()
        start_data_ts_ns = self.all_ticks[0][0]
        last_data_ts_ns = start_data_ts_ns
        played_count = 0

        try:
            for original_ts_ns, topic_bytes, pickled_data_bytes in self.all_ticks:
                if not self.running:
                    logger.info("回放被中断。")
                    break

                if playback_speed > 0:
                    # Calculate time delta based on original timestamps
                    data_delta_ns = original_ts_ns - last_data_ts_ns
                    playback_delta_ns = data_delta_ns / playback_speed

                    # Calculate target playback time relative to start
                    target_playback_offset_ns = (original_ts_ns - start_data_ts_ns) / playback_speed
                    target_playback_time_ns = start_time_ns + target_playback_offset_ns

                    # Wait until the target playback time
                    current_time_ns = time.time_ns()
                    wait_ns = target_playback_time_ns - current_time_ns
                    if wait_ns > 0:
                        time.sleep(wait_ns / 1_000_000_000) # Convert ns to seconds

                # Publish the data
                self.publisher.send_multipart([topic_bytes, pickled_data_bytes])
                played_count += 1
                last_data_ts_ns = original_ts_ns # Update last timestamp for delta calculation

                # Print progress occasionally
                if played_count % 1000 == 0:
                    data_time = datetime.fromtimestamp(original_ts_ns / 1_000_000_000)
                    logger.info(f"  已回放 {played_count}/{len(self.all_ticks)} 条 | 当前数据时间: {data_time.strftime('%H:%M:%S.%f')[:-3]}")

        except KeyboardInterrupt:
            logger.info("\n检测到中断信号，停止回放...")
        except Exception as e:
            logger.exception(f"回放过程中发生错误: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.running = False
            logger.info(f"数据回放结束。总共回放 {played_count} 条消息。")
            self.stop()

    def stop(self):
        """Stops the service and cleans up resources."""
        logger.info("停止数据回放服务...")
        self.running = False # Ensure playback loop stops
        if self.publisher:
            try:
                self.publisher.close()
                logger.info("ZeroMQ 发布器已关闭。")
            except Exception as e:
                logger.error(f"关闭 ZeroMQ 发布器时出错: {e}")
        if self.context:
            try:
                if not self.context.closed:
                    self.context.term()
                    logger.info("ZeroMQ Context 已终止。")
                else:
                    logger.debug("ZeroMQ Context 已终止 (之前已关闭).")
            except zmq.ZMQError as e:
                 logger.error(f"终止 ZeroMQ Context 时出错 (可能已终止): {e}")
            except Exception as e:
                 logger.exception(f"关闭 ZeroMQ Context 时发生未知错误: {e}")
        logger.info("数据回放服务已停止。")

# --- Main execution block (Example Usage) ---
if __name__ == "__main__":
    # Example: Play back data for today's date (or specific date)
    playback_date = datetime.now().strftime('%Y%m%d')
    # Or set a specific date: playback_date = "20231027"

    data_path = config.BACKTEST_DATA_SOURCE_PATH
    pub_url = config.BACKTEST_DATA_PUB_URL

    player = DataPlayerService(data_path, pub_url, playback_date)

    if player.load_data():
        # Start playback at maximum speed (speed=0)
        # Or set speed=1 for approximate real-time, speed=10 for 10x faster, etc.
        player.start_playback(playback_speed=0)
    else:
        print("未能加载数据，退出。")
