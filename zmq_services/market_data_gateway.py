#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : market_data_gateway.py
@Date       : 2025/4/12 10:02
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 行情网关
"""
import logging
import os
import sys
import threading

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.config_manager import ConfigManager
from utils.i18n import _
from utils.logger import logger, setup_logging, INFO
from vnpy.event import Event, EventEngine
from vnpy.trader.constant import Exchange
from vnpy.trader.event import EVENT_CONTRACT, EVENT_LOG, EVENT_TICK
from vnpy.trader.object import ContractData, LogData, SubscribeRequest, TickData
from vnpy.trader.utility import load_json
from vnpy_ctp import CtpGateway

from .zmq_base import ZmqPublisherBase

# Define the expected CTP connection success message
CTP_MD_LOGIN_SUCCESS_MSG = "行情服务器登录成功"
CTP_CONNECTION_TIMEOUT_S = 30.0

class MarketDataGatewayService(ZmqPublisherBase):
    def __init__(self, config_manager: ConfigManager, environment_name: str):
        super().__init__()
        self.config_service = config_manager
        self.environment_name = environment_name
        setup_logging(service_name=f"{__class__.__name__}[{self.environment_name}]", level=INFO)

        logger.info(_("使用提供的 ConfigManager 为环境 [{}] 初始化 MarketDataGatewayService。").format(environment_name))

        self.event_engine = EventEngine()
        self.gateway: CtpGateway = CtpGateway(self.event_engine, f"CTP_{environment_name}")

        self._ctp_connected: bool = False
        self._ctp_connection_event = threading.Event()

        self.ctp_setting: dict | None = None
        try:
            all_ctp_settings = load_json("connect_ctp.json")
            if environment_name in all_ctp_settings:
                self.ctp_setting = all_ctp_settings[environment_name]
                logger.info(_("已为环境加载 CTP 设置：[{}]").format(environment_name))
            else:
                logger.error(_("在 connect_ctp.json 中未找到环境 '{}'！无法连接 CTP。" ).format(environment_name))
        except FileNotFoundError:
            logger.error(_("未找到 connect_ctp.json！无法连接 CTP。"))
        except Exception as err:
            logger.exception(_("加载或解析 connect_ctp.json 时出错：{}" ).format(str(err)))

        self._subscribe_list = []
        logger.info(_("行情网关服务 for [{}] 初始化完成.").format(environment_name))

    def process_event(self, event: Event):
        event_type = event.type
        if event_type == EVENT_TICK:
            tick: TickData = event.data
            logger.debug(_("发布Tick: {} - Price: {}" ).format(tick.vt_symbol, tick.last_price))
            topic = f"tick.{tick.vt_symbol}"
            success = self.publish(topic, tick)
            if not success:
                logger.error(_("发布 Tick 失败 (主题: {})" ).format(topic))

        elif event_type == EVENT_LOG:
            log: LogData = event.data
            if not self._ctp_connected and CTP_MD_LOGIN_SUCCESS_MSG in log.msg:
                logger.info(_("检测到 CTP 行情登录成功信号！"))
                self._ctp_connected = True
                self._ctp_connection_event.set()

            level_map = {
                logging.DEBUG: logging.DEBUG,
                logging.INFO: logging.INFO,
                logging.WARNING: logging.WARNING,
                logging.ERROR: logging.ERROR,
                logging.CRITICAL: logging.CRITICAL,
            }
            log_level_value = getattr(log, 'level', logging.INFO)
            if hasattr(log_level_value, 'value'):
                log_level_value = log_level_value.value
            if not isinstance(log_level_value, int):
                log_level_value = logging.INFO

            logger_level = level_map.get(log_level_value, logging.INFO)
            gateway_name = getattr(log, 'gateway_name', 'UnknownGateway')
            translated_vnpy_msg = log.msg
            logger.log(logger_level, _("[VNPY Logs - MDGW Processed] {} - {}".format(gateway_name, translated_vnpy_msg)))
            
            topic = "log"
            success = self.publish(topic, log)
            if not success:
                logger.error(_("发布 Log 失败 (主题: {})" ).format(topic))

        elif event_type == EVENT_CONTRACT:
            contract: ContractData = event.data
            topic = f"contract.{contract.vt_symbol}"
            success = self.publish(topic, contract)
            if not success:
                logger.error(_("发布 Contract 失败 (主题: {})" ).format(topic))

    def start(self, pub_address=None):
        pub_address = self.config_service.get_global_config("zmq_addresses.market_data_pub", "tcp://*:5555")
        if not pub_address:
            logger.error(_("行情发布地址 'zmq_addresses.market_data_pub' 未在配置中找到，无法启动."))
            return

        if not super().start(pub_address=pub_address):
            logger.error(_("无法启动 ZmqPublisherBase (绑定到 {})，行情网关启动中止." ).format(pub_address))
            return

        logger.info(_("行情网关服务 for [{}] 正在启动..." ).format(self.environment_name))

        self.event_engine.register(EVENT_TICK, self.process_event)
        self.event_engine.register(EVENT_LOG, self.process_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_event)
        self.event_engine.start()
        logger.info(_("事件引擎已启动."))

        logger.info(_("连接 CTP 网关 for [{}]..." ).format(self.environment_name))
        if not self.ctp_setting:
            logger.error(_("CTP 设置未加载或环境无效，无法连接 CTP 网关。"))
            self.stop()
            return
        try:
            logger.info(_("CTP 连接配置 (Env: {}): UserID={}, BrokerID={}, MD={}").format(
                       self.environment_name,
                       self.ctp_setting.get('userid'),
                       self.ctp_setting.get('broker_id'),
                       self.ctp_setting.get('md_address')))
            
            self._ctp_connection_event.clear()
            self._ctp_connected = False
            
            self.gateway.connect(self.ctp_setting)
            logger.info(_("CTP 网关连接请求已发送。等待连接成功信号 (超时: {}s)..." ).format(CTP_CONNECTION_TIMEOUT_S))

            connected = self._ctp_connection_event.wait(timeout=CTP_CONNECTION_TIMEOUT_S)

            if not connected:
                logger.error(_("CTP 行情连接超时 ({}s)！未收到登录成功信号." ).format(CTP_CONNECTION_TIMEOUT_S))
                self.stop()
                return
            
            logger.info(_("CTP 行情网关连接成功."))

            logger.info(_("订阅行情..."))
            self._subscribe_list = []
            subscribe_count = 0
            failed_symbols = []
            subscribe_symbols = self.config_service.get_global_config("default_subscribe_symbols", [])
            if not subscribe_symbols:
                logger.warning(_("'default_subscribe_symbols' 未在配置中找到或为空，不订阅任何合约."))

            for vt_symbol in subscribe_symbols:
                try:
                    symbol, exchange_str = vt_symbol.split('.')
                    exchange = Exchange(exchange_str)
                    req = SubscribeRequest(symbol=symbol, exchange=exchange)
                    self.gateway.subscribe(req)
                    self._subscribe_list.append(vt_symbol)
                    if subscribe_count < 5 or subscribe_count % 50 == 0:
                        logger.info(_("发送订阅请求: {}" ).format(vt_symbol))
                    subscribe_count += 1
                except ValueError:
                    logger.error(_("错误的合约格式，跳过订阅: {} (应为 SYMBOL.EXCHANGE)" ).format(vt_symbol))
                    failed_symbols.append(vt_symbol)
                except Exception as err:
                    logger.exception(_("订阅 {} 时出错: {}" ).format(vt_symbol, err))
                    failed_symbols.append(vt_symbol)
            logger.info(_("共发送 {} 个合约的订阅请求." ).format(subscribe_count))
            if failed_symbols:
                logger.warning(_("以下合约订阅失败或跳过: {}" ).format(', '.join(failed_symbols)))

        except Exception as err:
            logger.exception(_("连接或订阅 CTP 网关时发生严重错误 (Env: {}): {}" ).format(self.environment_name, err))
            self.stop()

        logger.info(_("行情网关服务 for [{}] 启动流程完成." ).format(self.environment_name))

    def stop(self):
        if not self.is_active():
            return

        logger.info(_("停止行情网关服务 for [{}]..." ).format(self.environment_name))

        self._ctp_connection_event.set()

        try:
            if hasattr(self.event_engine, '_active') and self.event_engine._active:
                self.event_engine.stop()
                logger.info(_("事件引擎已停止."))
        except Exception as err:
            logger.exception(_("停止事件引擎时出错: {}" ).format(err))

        if self.gateway:
            try:
                self.gateway.close()
                logger.info(_("CTP 网关已关闭."))
            except Exception as err:
                logger.exception(_("关闭 CTP 网关时出错: {}" ).format(err))

        super().stop()
