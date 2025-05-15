#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : zmq_base.py
@Date       : 2025/5/9 12:07
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 一个简单的基类, 用于管理 ZMQ Context 和 PUB Socket, 并提供发布方法。
A simple base class for managing ZMQ Context and PUB Socket, and providing
publishing methods.
"""
import threading
import time

import msgpack
import zmq

from utils.i18n import get_translator
from utils.logger import logger


class ZmqPublisherBase:

    def __init__(self):
        self._context: zmq.Context = zmq.Context()
        self._socket_pub: zmq.Socket | None = None  # PUB socket
        self._pub_address: str | None = None
        self._active: bool = False
        self._lock: threading.Lock = threading.Lock()  # 用于保护 socket 访问
        self._ = get_translator()

    def is_active(self) -> bool:
        """检查服务是否活动"""
        return self._active

    def start(self, pub_address: str) -> bool:
        """
        启动发布者，创建并绑定 PUB socket。

        Args:
            pub_address: PUB socket 需要绑定的地址 (e.g., "tcp://*:5555").

        Returns:
            True 如果成功启动, False 如果已在运行或启动失败。
        """
        if self._active:
            logger.warning(self._("{} 已在运行中。").format(self.__class__.__name__))
            return False

        logger.info(self._("正在启动 {}...").format(self.__class__.__name__))
        try:
            self._socket_pub = self._context.socket(zmq.PUB)
            # 设置 LINGER 选项，避免 close() 时阻塞
            self._socket_pub.setsockopt(zmq.LINGER, 0)
            # 可选：设置高水位标记 (SNDHWM)
            # sndhwm = 1000 # Example value
            # self._socket_pub.setsockopt(zmq.SNDHWM, sndhwm)
            # logger.info(self._("设置 ZMQ PUB socket SNDHWM: {}").format(sndhwm))

            self._socket_pub.bind(pub_address)
            self._pub_address = pub_address
            self._active = True
            logger.info(self._("{} 已启动，PUB socket 绑定到 {}").format(self.__class__.__name__, pub_address))
            return True
        except zmq.ZMQError as e:
            logger.exception(self._("{} 启动失败 (绑定 PUB socket 到 {} 时出错): {}").format(
                self.__class__.__name__, pub_address, e))
            self._active = False
            # 尝试关闭可能已部分创建的 socket
            if self._socket_pub:
                try:
                    self._socket_pub.close()
                except Exception as ex:
                    logger.error(self._("关闭 ZMQ PUB socket 时: {}").format(ex))
                    pass
                self._socket_pub = None
            return False
        except Exception as e:
             logger.exception(self._("{} 启动时发生未知错误: {}").format(self.__class__.__name__, e))
             self._active = False
             if self._socket_pub:
                 try:
                     self._socket_pub.close()
                 except Exception as ex:
                     logger.error(self._("关闭 ZMQ PUB socket 时: {}").format(ex))
                     pass
                 self._socket_pub = None
             return False


    def stop(self):
        """停止发布者，关闭 socket 和 context。"""
        if not self._active:
            logger.debug(self._("{} 未运行。").format(self.__class__.__name__))
            return

        logger.info(self._("正在停止 {}...").format(self.__class__.__name__))
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
                logger.info(self._("ZMQ PUB socket 已关闭。"))
            except Exception as e:
                logger.error(self._("关闭 ZMQ PUB socket 时出错: {}").format(e))

        # 终止 Context
        # 等待短暂时间确保 socket 关闭完成再终止 context
        time.sleep(0.1)
        try:
            # 检查 context 是否仍然有效且未关闭
            if self._context and not self._context.closed:
                self._context.term()
                logger.info(self._("ZMQ Context 已终止。"))
        except Exception as e:
            logger.error(self._("终止 ZMQ Context 时出错: {}").format(e))
        
        logger.info(self._("{} 已停止。").format(self.__class__.__name__))

    def publish(self, topic: str, data: object) -> bool:
        """
        Converts data to a dict (if needed), serializes with msgpack, and publishes.
        """
        if not self._active or not self._socket_pub:
            logger.warning(self._("发布失败：{} 未激活或 PUB socket 不可用。 Topic: {}").format(
                self.__class__.__name__, topic))
            return False
        data_to_pack = None
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
                    logger.warning(self._("发布取消：在获取锁后发现服务已停止。Topic: {}").format(topic))
                    return False
                # +++ Add specific logging for tick sending +++
                send_attempted = False
                send_succeeded = False
                if topic.startswith("tick."):
                    logger.debug(self._("[PUB Tick Send] Attempting send_multipart for Topic: {}").format(topic))
                    send_attempted = True
                # +++ End Add +++
                self._socket_pub.send_multipart([topic_bytes, data_bytes])
                # +++ Log success for tick +++
                if send_attempted:
                    logger.debug(self._("[PUB Tick Send] send_multipart call completed for Topic: {}").format(topic))
                    send_succeeded = True # Assume success if no exception
                # +++ End Log +++
            # --- Adjust general publish log level to DEBUG --- 
            # logger.debug(self._("已发布数据 (主题: {}, 大小: {} B)").format(topic, len(data_bytes))) # Keep as DEBUG
            # --- End Adjust ---
            return True
        except (msgpack.PackException, TypeError) as e_msgpack:
            logger.error(self._("Msgpack 序列化错误 (发布数据, 主题: {}): {}").format(topic, e_msgpack)) # Renamed
            try:
                # Log the type of the *original* data and the *converted* data
                logger.error(self._("原始对象类型: {}").format(type(data)))
                logger.error(self._("转换后对象类型: {}").format(type(data_to_pack)))
            except Exception as log_err:
                 logger.error(self._("记录对象细节时出错: {}").format(log_err))
            return False
        except zmq.ZMQError as e_zmq:
            # 处理特定 ZMQ 错误，例如缓冲区满 (EAGAIN)
            if e_zmq.errno == zmq.EAGAIN:
                logger.warning(self._("发布事件时 ZMQ 缓冲区可能已满 (EAGAIN). Topic: {}").format(topic))
            else:
                logger.error(self._("发布事件时 ZMQ 错误: {}. Topic: {}").format(e_zmq, topic))
            return False
        except Exception as e:
            logger.exception(self._("发布事件时发生未知错误 (主题: {}): {}").format(topic, e))
            return False 