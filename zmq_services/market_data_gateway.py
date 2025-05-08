import logging
import sys
import os
import threading

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.logger import logger
# +++ Import ConfigManager +++
from utils.config_manager import ConfigManager

from vnpy.trader.utility import load_json

# VNPY imports
try:
    from vnpy.event import EventEngine, Event
    from vnpy.trader.gateway import BaseGateway
    from vnpy.trader.object import TickData, SubscribeRequest, LogData, ContractData # Added ContractData
    from vnpy.trader.event import EVENT_TICK, EVENT_LOG, EVENT_CONTRACT
    from vnpy_ctp import CtpGateway
    # from vnpy.rpc import RpcServer # <-- Remove RpcServer import
    from vnpy.trader.constant import Exchange # Import Exchange here
except ImportError as e:
    print(f"CRITICAL: Error importing vnpy modules: {e}")
    print("Please ensure vnpy and vnpy_ctp are installed and accessible.")
    print(f"CRITICAL: Project root added to path: {project_root}")
    print(f"CRITICAL: Current sys.path: {sys.path}")
    sys.exit(1)

# --- Remove old config import ---
# from config import zmq_config as config # Ensure this has PUB address

# +++ Import the new base class +++
from .zmq_base import ZmqPublisherBase

# Define the expected CTP connection success message
# Note: This might need adjustment based on the actual vnpy_ctp output
CTP_MD_LOGIN_SUCCESS_MSG = "行情服务器登录成功"
CTP_CONNECTION_TIMEOUT_S = 30.0 # Timeout for waiting connection

# --- Market Data Gateway Service ---
# Inherit from ZmqPublisherBase
class MarketDataGatewayService(ZmqPublisherBase):
    """
    Market data gateway service that uses CtpGateway for data collection
    and ZmqPublisherBase for publishing data via ZMQ PUB socket.
    Uses event-driven check for CTP connection success.
    """
    def __init__(self, config_manager: ConfigManager, environment_name: str):
        """Initializes the gateway service for a specific environment."""
        super().__init__() # Initialize the ZmqPublisherBase base class

        # +++ Use passed ConfigManager instance +++
        self.config_service = config_manager
        # --- Remove self.config_service = ConfigManager() ---

        logger.info(f"Initializing MarketDataGatewayService for environment: [{environment_name}] using provided ConfigManager.") # Updated log
        self.environment_name = environment_name

        # Create EventEngine for the gateway
        self.event_engine = EventEngine()

        # Create CTP gateway instance
        self.gateway: CtpGateway = CtpGateway(self.event_engine, f"CTP_{environment_name}")

        # --- CTP Connection State --- 
        self._ctp_connected: bool = False
        self._ctp_connection_event = threading.Event() # Event to signal connection
        # --- End CTP Connection State --- 

        # --- Load and Select CTP Settings --- 
        self.ctp_setting: dict | None = None
        try:
            all_ctp_settings = load_json("connect_ctp.json")
            if environment_name in all_ctp_settings:
                self.ctp_setting = all_ctp_settings[environment_name]
                logger.info(f"Loaded CTP settings for environment: [{environment_name}]")
            else:
                logger.error(f"Environment '{environment_name}' not found in connect_ctp.json! Cannot connect CTP.")
        except FileNotFoundError:
            logger.error("connect_ctp.json not found! Cannot connect CTP.")
        except Exception as err:
            logger.exception(f"Error loading or parsing connect_ctp.json: {err}")
        # --- End Load and Select --- 

        self._subscribe_list = []

        logger.info(f"行情网关服务 for [{environment_name}] 初始化完成。")

    def process_event(self, event: Event):
        """Processes events from the EventEngine and publishes via ZmqPublisherBase."""
        event_type = event.type
        if event_type == EVENT_TICK:
            tick: TickData = event.data
            # +++ Add a very prominent log here +++
            # logger.critical(f"@@@ EVENT_TICK RECEIVED in process_event for {tick.vt_symbol} @ {tick.datetime} - LastPrice: {tick.last_price} @@@") # <-- REMOVED
            # +++ End Add +++

            logger.debug(f"发布Tick: {tick.vt_symbol} - Price: {tick.last_price}")
            # --- Use the publish method from ZmqPublisherBase --- 
            topic = f"tick.{tick.vt_symbol}"
            success = self.publish(topic, tick)
            if not success:
                 logger.error(f"发布 Tick 失败 (主题: {topic})")
            # --- End use publish method --- 

        elif event_type == EVENT_LOG:
            log: LogData = event.data
            # Check for CTP connection success message
            if not self._ctp_connected and CTP_MD_LOGIN_SUCCESS_MSG in log.msg:
                logger.info("检测到 CTP 行情登录成功信号！")
                self._ctp_connected = True
                self._ctp_connection_event.set() # Signal the start method

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
            logger.log(logger_level, f"[VNPY LOG - MDGW Processed] {gateway_name} - {log.msg}")
            # --- Use the publish method from ZmqPublisherBase --- 
            topic = "log"
            success = self.publish(topic, log)
            if not success:
                 logger.error(f"发布 Log 失败 (主题: {topic})")
            # --- End use publish method --- 

        elif event_type == EVENT_CONTRACT:
            contract: ContractData = event.data
            # --- Use the publish method from ZmqPublisherBase --- 
            topic = f"contract.{contract.vt_symbol}"
            success = self.publish(topic, contract)
            if not success:
                 logger.error(f"发布 Contract 失败 (主题: {topic})")
            # --- End use publish method --- 

    def start(self, pub_address=None):
        """Starts the publisher, EventEngine, and connects the gateway."""
        # Use pub_address from config_service
        pub_address = self.config_service.get_global_config("zmq_addresses.market_data_pub", "tcp://*:5555")
        if not pub_address:
            logger.error(f"行情发布地址 'zmq_addresses.market_data_pub' 未在配置中找到，无法启动。")
            return

        # 1. Start the ZmqPublisherBase (binds PUB socket)
        if not super().start(pub_address=pub_address):
             logger.error(f"无法启动 ZmqPublisherBase (绑定到 {pub_address})，行情网关启动中止。")
             return # Don't proceed if publisher fails to start

        # If publisher started successfully, proceed with the rest
        logger.info(f"行情网关服务 for [{self.environment_name}] 正在启动...")

        # 2. Start the EventEngine
        self.event_engine.register(EVENT_TICK, self.process_event)
        self.event_engine.register(EVENT_LOG, self.process_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_event)
        self.event_engine.start()
        logger.info("事件引擎已启动。")

        # 3. Connect CTP Gateway
        logger.info(f"连接 CTP 网关 for [{self.environment_name}]...")
        if not self.ctp_setting:
            logger.error("CTP settings not loaded or environment invalid, cannot connect CTP gateway.")
            self.stop() # Attempt cleanup if CTP setting missing
            return
        try:
            logger.info(f"CTP 连接配置 (Env: {self.environment_name}): UserID={self.ctp_setting.get('userid')}, "
                             f"BrokerID={self.ctp_setting.get('broker_id')}, MD={self.ctp_setting.get('md_address')}")
            # Clear the connection event before connecting
            self._ctp_connection_event.clear()
            self._ctp_connected = False
            
            self.gateway.connect(self.ctp_setting)
            logger.info(f"CTP 网关连接请求已发送。等待连接成功信号 (超时: {CTP_CONNECTION_TIMEOUT_S}s)...")

            # Wait for the connection success event from process_event
            connected = self._ctp_connection_event.wait(timeout=CTP_CONNECTION_TIMEOUT_S)

            if not connected:
                logger.error(f"CTP 行情连接超时 ({CTP_CONNECTION_TIMEOUT_S}s)！未收到登录成功信号。")
                self.stop()
                return
            
            # If connected is True, proceed with subscriptions
            logger.info("CTP 行情网关连接成功。")

            # 4. Subscribe to market data
            logger.info("订阅行情...")
            self._subscribe_list = []
            subscribe_count = 0
            failed_symbols = []
            # +++ Get subscribe_symbols from ConfigManager +++
            subscribe_symbols = self.config_service.get_global_config("default_subscribe_symbols", [])
            if not subscribe_symbols:
                logger.warning("'default_subscribe_symbols' 未在配置中找到或为空，不订阅任何合约。")

            for vt_symbol in subscribe_symbols:
                try:
                    symbol, exchange_str = vt_symbol.split('.')
                    exchange = Exchange(exchange_str)
                    req = SubscribeRequest(symbol=symbol, exchange=exchange)
                    self.gateway.subscribe(req)
                    self._subscribe_list.append(vt_symbol)
                    # Log first few and periodically, avoid excessive logging
                    if subscribe_count < 5 or subscribe_count % 50 == 0:
                         logger.info(f"发送订阅请求: {vt_symbol}")
                    subscribe_count += 1
                except ValueError:
                     logger.error(f"错误的合约格式，跳过订阅: {vt_symbol} (应为 SYMBOL.EXCHANGE)")
                     failed_symbols.append(vt_symbol)
                except Exception as err:
                     logger.exception(f"订阅 {vt_symbol} 时出错: {err}")
                     failed_symbols.append(vt_symbol)
            logger.info(f"共发送 {subscribe_count} 个合约的订阅请求。")
            if failed_symbols:
                logger.warning(f"以下合约订阅失败或跳过: {', '.join(failed_symbols)}")

        except Exception as err:
            logger.exception(f"连接或订阅 CTP 网关时发生严重错误 (Env: {self.environment_name}): {err}")
            self.stop() # Attempt to clean up if connection fails

        logger.info(f"行情网关服务 for [{self.environment_name}] 启动流程完成。")


    def stop(self):
        """Stops the service and cleans up resources."""
        # Check if base class is active first
        if not self.is_active():
            # logger.warning(f"行情网关服务 for [{self.environment_name}] 未运行。")
            return

        logger.info(f"停止行情网关服务 for [{self.environment_name}]...")

        # Set the connection event just in case the start thread is still waiting
        # Prevents potential deadlock during shutdown if start fails mid-wait
        self._ctp_connection_event.set()

        # 1. Stop the EventEngine first to prevent processing more events
        try:
            if hasattr(self.event_engine, '_active') and self.event_engine._active:
                self.event_engine.stop()
                logger.info("事件引擎已停止。")
        except Exception as err:
            logger.exception(f"停止事件引擎时出错: {err}")

        # 2. Close the CTP Gateway connection
        if self.gateway:
            try:
                self.gateway.close()
                logger.info("CTP 网关已关闭。")
            except Exception as err:
                logger.exception(f"关闭 CTP 网关时出错: {err}")

        # 3. Stop the ZmqPublisherBase (closes PUB socket, terminates context)
        super().stop()
        # Base class stop method already logs its completion
