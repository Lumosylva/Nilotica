# zmq_services/zmq_base.py
import zmq
import pickle
import msgpack
import threading
import time
from typing import Any # Add Any import
from datetime import datetime # Add datetime import
from decimal import Decimal # Add Decimal import
from utils.logger import logger

# +++ Add VNPY imports needed for the converter +++
# from vnpy.trader.object import TickData, OrderData, TradeData, AccountData, ContractData, LogData # REMOVED
# from vnpy.trader.constant import Direction, OrderType, Exchange, Offset, Status, Product, OptionType # REMOVED
# +++ End VNPY imports +++


# +++ Add the converter function (adapted from DataRecorder) +++
# def convert_vnpy_obj_to_dict(obj: object) -> Any: # --- ENTIRE FUNCTION REMOVED --- 
#     ...
# +++ End converter function +++


class ZmqPublisherBase:
    """
    一个简单的基类，用于管理 ZMQ Context 和 PUB Socket，并提供发布方法。
    """
    def __init__(self):
        """构造函数"""
        self._context: zmq.Context = zmq.Context()
        self._socket_pub: zmq.Socket | None = None # PUB socket
        self._pub_address: str | None = None
        self._active: bool = False
        self._lock: threading.Lock = threading.Lock() # 用于保护 socket 访问

    def is_active(self) -> bool:
        """检查服务是否活动"""
        return self._active

    def start(self, pub_address: str) -> bool:
        """
        启动发布者，创建并绑定 PUB socket。

        Args:
            pub_address: PUB socket 需要绑定的地址 (e.g., "tcp://*:5555").

        Returns:
            True 如果成功启动，False 如果已在运行或启动失败。
        """
        if self._active:
            logger.warning(f"{self.__class__.__name__} 已在运行中。")
            return False

        logger.info(f"正在启动 {self.__class__.__name__}...")
        try:
            self._socket_pub = self._context.socket(zmq.PUB)
            # 设置 LINGER 选项，避免 close() 时阻塞
            self._socket_pub.setsockopt(zmq.LINGER, 0)
             # 可选：设置高水位标记 (SNDHWM)
            # sndhwm = 1000 # Example value
            # self._socket_pub.setsockopt(zmq.SNDHWM, sndhwm)
            # logger.info(f"设置 ZMQ PUB socket SNDHWM: {sndhwm}")

            self._socket_pub.bind(pub_address)
            self._pub_address = pub_address
            self._active = True
            logger.info(f"{self.__class__.__name__} 已启动，PUB socket 绑定到 {pub_address}")
            return True
        except zmq.ZMQError as e:
            logger.exception(f"{self.__class__.__name__} 启动失败 (绑定 PUB socket 到 {pub_address} 时出错): {e}")
            self._active = False
            # 尝试关闭可能已部分创建的 socket
            if self._socket_pub:
                try:
                    self._socket_pub.close()
                except Exception:
                    pass
                self._socket_pub = None
            return False
        except Exception as e:
             logger.exception(f"{self.__class__.__name__} 启动时发生未知错误: {e}")
             self._active = False
             if self._socket_pub:
                 try:
                     self._socket_pub.close()
                 except Exception:
                     pass
                 self._socket_pub = None
             return False


    def stop(self):
        """停止发布者，关闭 socket 和 context。"""
        if not self._active:
            # logger.info(f"{self.__class__.__name__} 未运行。")
            return

        logger.info(f"正在停止 {self.__class__.__name__}...")
        self._active = False # 首先标记为非活动状态

        # 关闭 PUB socket
        if self._socket_pub:
            socket_to_close = self._socket_pub
            self._socket_pub = None # 立即清除引用
            try:
                # 先取消绑定可能有助于更快关闭
                # if self._pub_address:
                #    # Use the stored address for unbind
                #    socket_to_close.unbind(self._pub_address) # <-- Comment out unbind
                socket_to_close.close()
                logger.info("ZMQ PUB socket 已关闭。")
            except Exception as e:
                logger.error(f"关闭 ZMQ PUB socket 时出错: {e}")

        # 终止 Context
        # 等待短暂时间确保 socket 关闭完成再终止 context
        time.sleep(0.1)
        try:
            # 检查 context 是否仍然有效且未关闭
            if self._context and not self._context.closed:
                self._context.term()
                logger.info("ZMQ Context 已终止。")
        except Exception as e:
            logger.error(f"终止 ZMQ Context 时出错: {e}")
        
        logger.info(f"{self.__class__.__name__} 已停止。")

    def publish(self, topic: str, data: object) -> bool:
        """
        Converts data to a dict (if needed), serializes with msgpack, and publishes.
        """
        if not self._active or not self._socket_pub:
            logger.warning(f"发布失败：{self.__class__.__name__} 未激活或 PUB socket 不可用。 Topic: {topic}")
            return False

        try:
            topic_bytes = topic.encode('utf-8')
            # +++ Convert data before packing +++
            # --- IMPORT the converter function --- 
            from utils.converter import convert_vnpy_obj_to_dict
            data_to_pack = convert_vnpy_obj_to_dict(data)
            # +++ End conversion +++
            data_bytes = msgpack.packb(data_to_pack, use_bin_type=True) # Pack the converted data

            # 使用锁保护 socket 发送操作
            with self._lock:
                # 再次检查 socket 状态，因为 stop 可能在等待锁时被调用
                if not self._active or not self._socket_pub:
                    logger.warning(f"发布取消：在获取锁后发现服务已停止。Topic: {topic}")
                    return False
                # +++ Add specific logging for tick sending +++
                send_attempted = False
                send_succeeded = False
                if topic.startswith("tick."):
                    logger.debug(f"[PUB Tick Send] Attempting send_multipart for Topic: {topic}")
                    send_attempted = True
                # +++ End Add +++
                self._socket_pub.send_multipart([topic_bytes, data_bytes])
                # +++ Log success for tick +++
                if send_attempted:
                    logger.debug(f"[PUB Tick Send] send_multipart call completed for Topic: {topic}")
                    send_succeeded = True # Assume success if no exception
                # +++ End Log +++
            # --- Adjust general publish log level to DEBUG --- 
            # logger.debug(f"已发布数据 (主题: {topic}, 大小: {len(data_bytes)} B)") # Keep as DEBUG
            # --- End Adjust ---
            return True
        except (msgpack.PackException, TypeError) as e_msgpack:
            logger.error(f"Msgpack 序列化错误 (发布数据, 主题: {topic}): {e_msgpack}") # Renamed
            try:
                # Log the type of the *original* data and the *converted* data
                logger.error(f"  原始对象类型: {type(data)}")
                logger.error(f"  转换后对象类型: {type(data_to_pack)}")
            except Exception as log_err:
                 logger.error(f"  记录对象细节时出错: {log_err}")
            return False
        except zmq.ZMQError as e_zmq:
            # 处理特定 ZMQ 错误，例如缓冲区满 (EAGAIN)
            if e_zmq.errno == zmq.EAGAIN:
                logger.warning(f"发布事件时 ZMQ 缓冲区可能已满 (EAGAIN). Topic: {topic}")
            else:
                logger.error(f"发布事件时 ZMQ 错误: {e_zmq}. Topic: {topic}")
            return False
        except Exception as e:
            logger.exception(f"发布事件时发生未知错误 (主题: {topic}): {e}")
            return False 