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
import time

from config import global_vars
from config.constants.params import Params
from utils.service_common import get_gateway
# from vnpy.event.dedup_engine import DedupEventEngine # Not used directly
# from vnpy_ctp import CtpGateway # Not used directly
# from vnpy_tts import TtsGateway # Not used directly

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.config_manager import ConfigManager
from utils.i18n import _
from utils.logger import logger, setup_logging, INFO, DEBUG, get_level_name
from vnpy.event import Event, EventEngine
from vnpy.trader.constant import Exchange
from vnpy.trader.event import EVENT_CONTRACT, EVENT_LOG, EVENT_TICK
from vnpy.trader.object import ContractData, LogData, SubscribeRequest, TickData
from vnpy.trader.utility import load_json

from .zmq_base import ZmqPublisherBase

CTP_CONNECTION_TIMEOUT_S = 30.0 # This constant seems CTP specific, might need review if TTS has different timeout needs


class MarketDataGatewayService(ZmqPublisherBase):
    def __init__(self, config_manager: ConfigManager, environment_name: str):
        super().__init__()
        self.config_service = config_manager
        self.environment_name = environment_name
        self.event_engine = EventEngine()
        self._ctp_connected: bool = False # Renamed from _gateway_connected for clarity if it was generic
        self._ctp_connection_event = threading.Event() # Renamed for clarity

        # --- Load and Select CTP/TTS Settings ---
        self.broker_config_filename = Params.brokers_config_filename # Generic name
        self.current_broker_setting: dict | None = None
        try:
            all_broker_configs = load_json(self.broker_config_filename)
            if self.environment_name in all_broker_configs.get("brokers", {}):
                self.current_broker_setting = all_broker_configs["brokers"][self.environment_name]
                logger.info(_("已加载环境 [{}] 的经纪商设置").format(self.environment_name))
            elif all_broker_configs.get('default_broker') and all_broker_configs['default_broker'] in all_broker_configs.get("brokers", {}):
                self.environment_name = all_broker_configs['default_broker']
                self.current_broker_setting = all_broker_configs["brokers"][self.environment_name]
                logger.info(_("已加载默认环境 [{}] 的经纪商设置").format(self.environment_name))
            else:
                logger.error(_("未找到环境 '{}' 的有效经纪商设置！").format(self.environment_name))
                self.current_broker_setting = None
        except FileNotFoundError:
            logger.error(_("未找到经纪商配置文件 {}！").format(self.broker_config_filename))
            self.current_broker_setting = None
        except Exception as err:
            logger.exception(_("加载或解析经纪商配置文件 {} 时出错：{}").format(self.broker_config_filename, str(err)))
            self.current_broker_setting = None

        # Setup logging with the potentially updated environment_name
        setup_logging(service_name=f"{__class__.__name__}[{self.environment_name}]", level=INFO) # Default to INFO

        self.gateway = get_gateway(self.event_engine, self.environment_name)
        if not self.gateway:
            logger.critical(_("无法为环境 [{}] 获取网关实例，服务将无法启动。").format(self.environment_name))
            # Handle this critical failure, perhaps by not allowing start() to proceed
            # For now, execution will continue but connect_md will likely fail

        self._subscribe_list = [] # Currently not used to resubscribe, gateway handles that
        logger.info(_("行情网关服务 for [{}] 初始化完成.").format(self.environment_name))

    def process_event(self, event: Event):
        event_type = event.type
        if event_type == EVENT_TICK:
            tick: TickData = event.data
            logger.debug(_("发布Tick: {} - Price: {}").format(tick.vt_symbol, tick.last_price))
            topic = f"tick.{tick.vt_symbol}"
            success = self.publish(topic, tick)
            if not success:
                logger.error(_("发布 Tick 失败 (主题: {})").format(topic))
        elif event_type == EVENT_LOG:
            log: LogData = event.data
            # Use global_vars.md_login_success, which should be set by the gateway's onRspUserLogin
            if not self._ctp_connected and global_vars.md_login_success: # Check specific var
                logger.info(_("检测到行情登录成功信号 (via global_vars.md_login_success)！"))
                self._ctp_connected = True
                self._ctp_connection_event.set()
            # Removed specific TTS log message check as global_vars.md_login_success should be the primary signal

            log_level_value = getattr(log, 'level', logging.INFO)
            # Ensure log_level_value is an int, not an enum or other type before get_level_name
            if hasattr(log_level_value, 'value'): # Handle cases where level might be an Enum
                log_level_value = log_level_value.value
            if not isinstance(log_level_value, int):
                 try:
                     log_level_value = int(log_level_value) # Try to convert if it's string like "20"
                 except ValueError:
                     log_level_value = logging.INFO # Default if conversion fails

            logger_level_name = get_level_name(log_level_value) # get_level_name expects int
            event_msg = log.msg
            # Log with the level derived from the event itself
            logger.log(logger_level_name, _("[Event Logs - MDGW Processed] {}").format(event_msg))

            topic = "log"
            success = self.publish(topic, log)
            if not success:
                logger.error(_("发布 Log 失败 (主题: {})").format(topic))
        elif event_type == EVENT_CONTRACT:
            contract: ContractData = event.data
            topic = f"contract.{contract.vt_symbol}"
            success = self.publish(topic, contract)
            if not success:
                logger.error(_("发布 Contract 失败 (主题: {})").format(topic))

    def start(self, pub_address=None):
        if not pub_address:
            logger.error(_("行情发布地址 'pub_address' 未提供，无法启动 ZMQ 发布器。"))
            return

        if not super().start(pub_address=pub_address):
            logger.error(_("无法启动 ZmqPublisherBase (绑定到 {})，行情网关启动中止.").format(pub_address))
            return
        logger.info(_("ZMQ 发布器已在地址 {} 启动。").format(pub_address))

        self.event_engine.register(EVENT_LOG, self.process_event)
        self.event_engine.register(EVENT_TICK, self.process_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_event)
        self.event_engine.start()
        logger.info(_("事件引擎已启动，并注册了事件处理器。"))

        if not self.gateway:
            logger.error(_("网关实例未初始化，无法连接行情服务。"))
            self.stop() # Cleanup ZMQ and event engine
            return

        logger.info(_("连接行情网关 for [{}]...").format(self.environment_name))
        if not self.current_broker_setting:
            logger.error(_("经纪商设置未加载或环境无效，无法连接网关。"))
            self.stop()
            return

        try:
            logger.info(_("行情连接配置 (Env: {}): UserID={}, BrokerID={}, MD_Address={}").format(
                self.environment_name,
                self.current_broker_setting.get('userid'),
                self.current_broker_setting.get('broker_id'),
                self.current_broker_setting.get('md_address')))

            global_vars.md_login_success = False
            self._ctp_connection_event.clear()
            self._ctp_connected = False

            self.gateway.connect_md(self.current_broker_setting)
            logger.info(_("行情网关 connect_md 请求已发送. 等待 {} 秒连接...").format(CTP_CONNECTION_TIMEOUT_S))

            # Wait for connection signal or timeout
            if self._ctp_connection_event.wait(timeout=CTP_CONNECTION_TIMEOUT_S):
                if self._ctp_connected:
                    logger.info(_("行情网关连接并登录成功 (环境: {})!").format(self.environment_name))
                    # After successful connection, subscribe to any predefined symbols if needed
                    # self._subscribe_instruments() # Example call if you have such a method
                else:
                    # This case should ideally not happen if event is set only on True connection
                    logger.error(_("行情网关连接事件触发但未成功连接 (环境: {}).").format(self.environment_name))
                    self.stop()
            else:
                logger.error(_("行情网关连接超时 ({} 秒) (环境: {}). 请检查网络和配置.").format(CTP_CONNECTION_TIMEOUT_S, self.environment_name))
                # Log current global_vars.md_login_success state for diagnostics
                logger.info(f"Timeout diagnostic: global_vars.md_login_success = {global_vars.md_login_success}")
                if hasattr(self.gateway, 'md_api') and self.gateway.md_api:
                    logger.info(f"Timeout diagnostic: md_api.connect_status = {self.gateway.md_api.connect_status}, md_api.login_status = {self.gateway.md_api.login_status}")
                self.stop()

        except Exception as err:
            logger.exception(_("连接行情网关过程中发生错误 (Env: {}): {}").format(self.environment_name, err))
            self.stop()

    # Example subscription method (if you want to manage subscriptions here)
    # def _subscribe_instruments(self):
    #     symbols_to_subscribe = self.config_service.get_global_config("market_data_subscriptions.symbols", [])
    #     if not symbols_to_subscribe:
    #         logger.info(_("没有在配置中找到要订阅的合约列表 (market_data_subscriptions.symbols)."))
    #         return
    #     for symbol_entry in symbols_to_subscribe:
    #         try:
    #             # Assuming symbol_entry can be a string or a dict like {"symbol": "IF2406", "exchange": "CFFEX"}
    #             if isinstance(symbol_entry, str):
    #                 vt_symbol = symbol_entry # Or parse if it's like IF2406.CFFEX
    #                 # Attempt to find exchange if not part of vt_symbol
    #                 parts = vt_symbol.split('.')
    #                 symbol = parts[0]
    #                 exchange_str = parts[1] if len(parts) > 1 else None
    #                 if not exchange_str:
    #                     # TODO: Need a robust way to get exchange for a symbol if not provided
    #                     # This might involve querying contract details first, or having a map
    #                     logger.warning(f"订阅 {symbol} 时缺少交易所信息，跳过。")
    #                     continue
    #                 exchange = Exchange(exchange_str)
    #             elif isinstance(symbol_entry, dict):
    #                 symbol = symbol_entry.get("symbol")
    #                 exchange_str = symbol_entry.get("exchange")
    #                 if not symbol or not exchange_str:
    #                     logger.warning(f"无效的订阅条目: {symbol_entry}, 跳过")
    #                     continue
    #                 exchange = Exchange(exchange_str)
    #             else:
    #                 logger.warning(f"无法识别的订阅条目格式: {symbol_entry}, 跳过")
    #                 continue
    #
    #             req = SubscribeRequest(symbol=symbol, exchange=exchange)
    #             self.gateway.subscribe(req)
    #             logger.info(_("已发送 {}@{} 的订阅请求.").format(symbol, exchange.value))
    #         except Exception as e:
    #             logger.exception(_("处理订阅请求 {} 时出错: {}").format(symbol_entry, e))

    def stop(self):
        logger.info(_("MarketDataGatewayService.stop() CALLED."))
        if not self.is_active() and not (hasattr(self.event_engine, '_active') and self.event_engine._active):
             logger.info(_("MarketDataGatewayService.stop(): Service (ZMQ/EventEngine) not active or already stopped."))
             # Fall through to close gateway if it might be connected
        
        self._ctp_connection_event.set() # Unblock any waiters immediately

        try:
            if hasattr(self.event_engine, '_active') and self.event_engine._active:
                self.event_engine.stop()
                logger.info(_("事件引擎已停止."))
        except Exception as err:
            logger.exception(_("停止事件引擎时出错: {}").format(err))

        if self.gateway:
            try:
                logger.info(_("尝试关闭行情网关..."))
                self.gateway.close()
                logger.info(_("行情网关已关闭."))
            except Exception as err:
                logger.exception(_("关闭行情网关时出错: {}").format(err))

        super().stop() # Stop ZMQ publisher
        logger.info(_("MarketDataGatewayService for [{}] stop sequence complete.").format(self.environment_name))
