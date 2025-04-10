import zmq
import msgpack
import time
import sys
import os
import json
import glob
from datetime import datetime
import heapq # For efficient sorting/merging if loading multiple files

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')) # Go up two levels
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Import local config (need to adjust relative path)
try:
    from zmq_services import config
except ImportError:
     print("无法导入 zmq_services.config。请确保路径设置正确。")
     # Define fallback config values if necessary for testing standalone
     class config:
         BACKTEST_DATA_SOURCE_PATH = "../recorded_data/" # Adjust path
         BACKTEST_DATA_PUB_URL = "tcp://*:5560"
         # Add other needed configs if testing standalone

# --- Data Player Service ---
class DataPlayerService:
    def __init__(self, data_source_path: str, backtest_pub_url: str, date_str: str):
        """
        Initializes the Data Player service.
        :param data_source_path: Path to the directory containing recorded data files.
        :param backtest_pub_url: ZMQ URL to publish backtest market data.
        :param date_str: The date string (YYYYMMDD) for which to play back data.
        """
        self.context = zmq.Context()
        self.publisher = self.context.socket(zmq.PUB)
        self.publisher.bind(backtest_pub_url)
        print(f"回测数据发布器绑定到: {backtest_pub_url}")

        self.data_source_path = data_source_path
        self.backtest_pub_url = backtest_pub_url
        self.date_str = date_str
        self.tick_data_file = os.path.join(self.data_source_path, f"ticks_{self.date_str}.jsonl")

        self.all_ticks = [] # List to hold all tick data [(original_ts_ns, topic_bytes, packed_message_bytes)]
        self.running = False

    def load_data(self) -> bool:
        """Loads tick data from the specified file."""
        print(f"尝试从 {self.tick_data_file} 加载数据...")
        if not os.path.exists(self.tick_data_file):
            print(f"错误: 数据文件不存在: {self.tick_data_file}")
            return False

        try:
            with open(self.tick_data_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        original_message = record.get("original_message")
                        if original_message and original_message.get("type") == "TICK":
                            # Extract necessary info for playback
                            original_ts_ns = original_message.get("timestamp")
                            topic_str = original_message.get("topic") # Use original topic
                            # Re-pack the original message for sending
                            packed_original_msg = msgpack.packb(original_message, use_bin_type=True)
                            topic_bytes = topic_str.encode('utf-8')

                            if original_ts_ns and topic_str:
                                self.all_ticks.append((original_ts_ns, topic_bytes, packed_original_msg))
                            else:
                                print(f"警告: 跳过缺少时间戳或主题的记录: {record}")

                    except json.JSONDecodeError as e:
                        print(f"警告: 解析 JSON 行时出错: {e}. 行: {line.strip()}")
                    except msgpack.PackException as e:
                         print(f"警告: 重新打包消息时出错: {e}. 消息: {original_message}")
                    except Exception as e:
                         print(f"警告: 处理记录时发生未知错误: {e}. 记录: {record}")


            if not self.all_ticks:
                print("错误：未能从文件中加载任何有效的 Tick 数据。")
                return False

            # Sort data by original timestamp
            self.all_ticks.sort(key=lambda x: x[0])
            print(f"数据加载完成并排序。总共 {len(self.all_ticks)} 条 Tick 数据。")
            return True

        except IOError as e:
            print(f"读取文件 {self.tick_data_file} 时出错: {e}")
            return False
        except Exception as e:
             print(f"加载数据时发生未知错误: {e}")
             import traceback
             traceback.print_exc()
             return False

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
            print("错误: 没有数据可供回放。请先调用 load_data()。")
            return

        print(f"开始数据回放 (速度: {playback_speed if playback_speed > 0 else '最大'})... 按 Ctrl+C 停止。")
        self.running = True
        start_time_ns = time.time_ns()
        start_data_ts_ns = self.all_ticks[0][0]
        last_data_ts_ns = start_data_ts_ns
        played_count = 0

        try:
            for original_ts_ns, topic_bytes, packed_message_bytes in self.all_ticks:
                if not self.running:
                    print("回放被中断。")
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
                self.publisher.send_multipart([topic_bytes, packed_message_bytes])
                played_count += 1
                last_data_ts_ns = original_ts_ns # Update last timestamp for delta calculation

                # Print progress occasionally
                if played_count % 1000 == 0:
                    data_time = datetime.fromtimestamp(original_ts_ns / 1_000_000_000)
                    print(f"  已回放 {played_count}/{len(self.all_ticks)} 条 | 当前数据时间: {data_time.strftime('%H:%M:%S.%f')[:-3]}")

        except KeyboardInterrupt:
            print("\n检测到中断信号，停止回放...")
        except Exception as e:
            print(f"回放过程中发生错误: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.running = False
            print(f"数据回放结束。总共回放 {played_count} 条消息。")
            self.stop()

    def stop(self):
        """Stops the service and cleans up resources."""
        print("停止数据回放服务...")
        self.running = False # Ensure playback loop stops
        if self.publisher:
            try:
                self.publisher.close()
                print("ZeroMQ 发布器已关闭。")
            except Exception as e:
                print(f"关闭 ZeroMQ 发布器时出错: {e}")
        if self.context:
            try:
                if not self.context.closed:
                    self.context.term()
                    print("ZeroMQ Context 已终止。")
                else:
                    print("ZeroMQ Context 已终止 (之前已关闭).")
            except zmq.ZMQError as e:
                 print(f"终止 ZeroMQ Context 时出错 (可能已终止): {e}")
            except Exception as e:
                 print(f"关闭 ZeroMQ Context 时发生未知错误: {e}")
        print("数据回放服务已停止。")

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
