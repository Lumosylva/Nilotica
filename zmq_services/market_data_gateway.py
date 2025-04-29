import time
from datetime import datetime
import logging
# from logger import getLogger # Removed
import sys
import os
import pickle

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.logger import logger # Added

from vnpy.trader.utility import load_json
# Add project root to Python path to find vnpy modules
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# from utils.logger import setup_logging, logger

# VNPY imports
try:
    from vnpy.event import EventEngine, Event
    from vnpy.trader.gateway import BaseGateway
    from vnpy.trader.object import TickData, SubscribeRequest, LogData, ContractData # Added ContractData
    from vnpy.trader.event import EVENT_TICK, EVENT_LOG, EVENT_CONTRACT
    from vnpy_ctp import CtpGateway
    from vnpy.rpc import RpcServer # Import RpcServer
    from vnpy.trader.constant import Exchange # Import Exchange here
except ImportError as e:
    print(f"CRITICAL: Error importing vnpy modules: {e}")
    print("Please ensure vnpy and vnpy_ctp are installed and accessible.")
    print(f"CRITICAL: Project root added to path: {project_root}")
    print(f"CRITICAL: Current sys.path: {sys.path}")
    sys.exit(1)

# Import new config location
from config import zmq_config as config # Ensure this has REP and PUB addresses

# --- Market Data Gateway Service ---
# Inherit from RpcServer
class MarketDataGatewayService(RpcServer):
    """
    Market data gateway service that uses CtpGateway for data collection
    and RpcServer for publishing data.
    """
    def __init__(self, environment_name: str):
        """Initializes the gateway service for a specific environment."""
        super().__init__() # Initialize the RpcServer base class
        # Restore logger assignment
        # self.logger = getLogger(__name__) # Removed

        logger.info(f"Initializing MarketDataGatewayService for environment: [{environment_name}]...") # Changed self.logger to logger
        self.environment_name = environment_name

        # Create EventEngine for the gateway
        self.event_engine = EventEngine()

        # Create CTP gateway instance
        self.gateway: BaseGateway = CtpGateway(self.event_engine, f"CTP_MD_{environment_name}")

        # --- Load and Select CTP Settings --- 
        self.ctp_setting: dict | None = None
        try:
            all_ctp_settings = load_json("connect_ctp.json")
            if environment_name in all_ctp_settings:
                self.ctp_setting = all_ctp_settings[environment_name]
                logger.info(f"Loaded CTP settings for environment: [{environment_name}]") # Changed self.logger to logger
            else:
                logger.error(f"Environment '{environment_name}' not found in connect_ctp.json! Cannot connect CTP.") # Changed self.logger to logger
        except FileNotFoundError:
            logger.error("connect_ctp.json not found! Cannot connect CTP.") # Changed self.logger to logger
        except Exception as err:
            logger.exception(f"Error loading or parsing connect_ctp.json: {err}") # Changed self.logger to logger
        # --- End Load and Select --- 

        self._subscribe_list = []

        logger.info(f"行情网关服务(RPC模式) for [{environment_name}] 初始化完成。") # Changed self.logger to logger

    def process_event(self, event: Event):
        """Processes events from the EventEngine."""
        event_type = event.type
        if event_type == EVENT_TICK:
            tick: TickData = event.data
            # self.publish(f"tick.{tick.vt_symbol}", tick)  # <-- Commented out problematic call
            logger.debug(f"发布Tick: {tick.vt_symbol} - Price: {tick.last_price}") # Changed self.logger to logger
            # --- Manually send multipart message ---
            try:
                topic_bytes = f"tick.{tick.vt_symbol}".encode('utf-8')
                data_bytes = pickle.dumps(tick)
                # Directly access self._socket_pub based on vnpy.rpc source
                self._socket_pub.send_multipart([topic_bytes, data_bytes])
                # logger.debug(f"手动发送 multipart Tick 成功 (主题: {topic_bytes.decode()})") # <-- 注释掉
            except AttributeError:
                 logger.error("AttributeError: 无法直接访问 self._socket_pub！RpcServer 内部可能已更改。") # Changed self.logger to logger
            except Exception as e_manual_send:
                logger.exception(f"手动发送 multipart Tick 时出错: {e_manual_send}") # Changed self.logger to logger
            # --- End manual send ---

        elif event_type == EVENT_LOG:
            log: LogData = event.data
            # self.publish(f"log", log) # <-- Commented out problematic call
            # Restore logging of vnpy internal logs using custom logger
            level_map = {
                logging.DEBUG: logging.DEBUG,
                logging.INFO: logging.INFO,
                logging.WARNING: logging.WARNING,
                logging.ERROR: logging.ERROR,
                logging.CRITICAL: logging.CRITICAL,
            }
            log_level_value = getattr(log, 'level', logging.INFO)
            if hasattr(log_level_value, 'value'): # Handle Enum
                log_level_value = log_level_value.value
            if not isinstance(log_level_value, int):
                 log_level_value = logging.INFO # Fallback

            logger_level = level_map.get(log_level_value, logging.INFO)
            gateway_name = getattr(log, 'gateway_name', 'UnknownGateway')
            logger.log(logger_level, f"[VNPY LOG - MDGW Processed] {gateway_name} - {log.msg}") # Changed self.logger to logger
            # --- Manually send multipart message for log ---
            try:
                topic_bytes = b"log"
                data_bytes = pickle.dumps(log)
                # Directly access self._socket_pub
                self._socket_pub.send_multipart([topic_bytes, data_bytes])
                # logger.debug(f"手动发送 multipart Log 成功") # <-- 注释掉
            except AttributeError:
                 logger.error("AttributeError: 无法直接访问 self._socket_pub！RpcServer 内部可能已更改。") # Changed self.logger to logger
            except Exception as e_manual_log_send:
                logger.exception(f"手动发送 multipart Log 时出错: {e_manual_log_send}") # Changed self.logger to logger
            # --- End manual send ---

        elif event_type == EVENT_CONTRACT:
            contract: ContractData = event.data
            # self.publish(f"contract.{contract.vt_symbol}", contract) # <-- Commented out problematic call
            # logger.debug(f"发布合约信息: {contract.vt_symbol}")
            # --- Manually send multipart message for contract ---
            try:
                topic_bytes = f"contract.{contract.vt_symbol}".encode('utf-8')
                data_bytes = pickle.dumps(contract)
                 # Directly access self._socket_pub
                self._socket_pub.send_multipart([topic_bytes, data_bytes])
                # logger.debug(f"手动发送 multipart Contract 成功 (主题: {topic_bytes.decode()})") # <-- 注释掉
            except AttributeError:
                 logger.error("AttributeError: 无法直接访问 self._socket_pub！RpcServer 内部可能已更改。") # Changed self.logger to logger
            except Exception as e_manual_contract_send:
                 logger.exception(f"手动发送 multipart Contract 时出错: {e_manual_contract_send}") # Changed self.logger to logger
             # --- End manual send ---

    def start(self, rep_address=None, pub_address=None):
        """Starts the RpcServer, EventEngine, and connects the gateway."""
        if self.is_active():
            logger.warning(f"行情网关服务(RPC模式) for [{self.environment_name}] 已在运行中。") # Changed self.logger to logger
            return

        logger.info(f"启动行情网关服务(RPC模式) for [{self.environment_name}]...") # Changed self.logger to logger

        # 1. Start the RpcServer (binds sockets, starts threads)
        try:
            # Use addresses from config
            super().start(
                rep_address=config.MARKET_DATA_REP_ADDRESS,
                pub_address=config.MARKET_DATA_PUB_ADDRESS
            )
            logger.info(f"RPC 服务器已启动。 REP: {config.MARKET_DATA_REP_ADDRESS}, PUB: {config.MARKET_DATA_PUB_ADDRESS}") # Changed self.logger to logger
        except Exception as err:
            logger.exception(f"启动 RPC 服务器失败: {err}") # Changed self.logger to logger
            return # Don't proceed if RPC server fails

        # 2. Start the EventEngine
        self.event_engine.register(EVENT_TICK, self.process_event)
        self.event_engine.register(EVENT_LOG, self.process_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_event)
        self.event_engine.start()
        logger.info("事件引擎已启动。") # Changed self.logger to logger

        # 3. Connect CTP Gateway
        logger.info(f"连接 CTP 网关 for [{self.environment_name}]...") # Changed self.logger to logger
        if not self.ctp_setting:
            logger.error("CTP settings not loaded or environment invalid, cannot connect CTP gateway.") # Changed self.logger to logger
            return
        try:
            logger.info(f"CTP 连接配置 (Env: {self.environment_name}): UserID={self.ctp_setting.get('userid')}, " # Changed self.logger to logger
                             f"BrokerID={self.ctp_setting.get('broker_id')}, MD={self.ctp_setting.get('md_address')}")
            self.gateway.connect(self.ctp_setting)
            logger.info("CTP 网关连接请求已发送。等待连接成功...") # Changed self.logger to logger

            # Wait for connection (replace sleep with event-driven logic if possible)
            # TODO: Implement waiting for a specific CTP connection success event/log
            time.sleep(10)
            # Restore custom logger
            logger.info("CTP 网关假定连接成功（基于延时）。") # Changed self.logger to logger

            # 4. Subscribe to market data
            logger.info("订阅行情...") # Changed self.logger to logger
            self._subscribe_list = []
            for vt_symbol in config.SUBSCRIBE_SYMBOLS:
                try:
                    symbol, exchange_str = vt_symbol.split('.')
                    exchange = Exchange(exchange_str)
                    req = SubscribeRequest(symbol=symbol, exchange=exchange)
                    self.gateway.subscribe(req)
                    self._subscribe_list.append(vt_symbol)
                    logger.info(f"发送订阅请求: {vt_symbol}") # Changed self.logger to logger
                    time.sleep(0.5) # Avoid flood
                except ValueError:
                     logger.error(f"错误的合约格式，跳过订阅: {vt_symbol} (应为 SYMBOL.EXCHANGE)") # Changed self.logger to logger
                except Exception as err:
                     logger.exception(f"订阅 {vt_symbol} 时出错: {err}") # Changed self.logger to logger
            logger.info(f"已发送 {len(self._subscribe_list)} 个合约的订阅请求。") # Changed self.logger to logger

        except Exception as err:
            logger.exception(f"连接或订阅 CTP 网关时发生严重错误 (Env: {self.environment_name}): {err}") # Changed self.logger to logger
            self.stop() # Attempt to clean up if connection fails

        logger.info(f"行情网关服务(RPC模式) for [{self.environment_name}] 启动流程完成。") # Changed self.logger to logger


    def stop(self):
        """Stops the service and cleans up resources."""
        if not self.is_active():
            logger.warning(f"行情网关服务(RPC模式) for [{self.environment_name}] 未运行。") # Changed self.logger to logger
            return

        logger.info(f"停止行情网关服务(RPC模式) for [{self.environment_name}]...") # Changed self.logger to logger

        # 1. Stop the EventEngine first to prevent new events processing
        try:
            if hasattr(self.event_engine, '_active') and self.event_engine._active: # Use _active attribute check
                self.event_engine.stop()
                logger.info("事件引擎已停止。") # Changed self.logger to logger
        except Exception as err:
            logger.exception(f"停止事件引擎时出错: {err}") # Changed self.logger to logger

        # 2. Close the CTP Gateway connection
        if self.gateway:
            try:
                self.gateway.close()
                logger.info("CTP 网关已关闭。") # Changed self.logger to logger
            except Exception as err:
                logger.exception(f"关闭 CTP 网关时出错: {err}") # Changed self.logger to logger

        # 3. Stop the RpcServer (closes sockets, terminates context, joins threads)
        super().stop()
        logger.info("RPC 服务器已停止。") # Changed self.logger to logger
        logger.info(f"行情网关服务(RPC模式) for [{self.environment_name}] 已停止。") # Changed self.logger to logger
