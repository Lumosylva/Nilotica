import time
from datetime import datetime
import logging
from logger import getLogger
import sys
import os
import pickle

from vnpy.trader.utility import load_json
# Add project root to Python path to find vnpy modules
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# VNPY imports
try:
    from vnpy.event import EventEngine, Event
    from vnpy.trader.gateway import BaseGateway
    from vnpy.trader.object import TickData, SubscribeRequest, LogData, ContractData # Added ContractData
    from vnpy.trader.event import EVENT_TICK, EVENT_LOG, EVENT_CONTRACT
    # from vnpy.trader.setting import SETTINGS # Not used directly
    from vnpy_ctp import CtpGateway
    from vnpy.rpc import RpcServer # Import RpcServer
    from vnpy.trader.constant import Exchange # Import Exchange here
except ImportError as e:
    # Use logger if available, otherwise print
    try:
        getLogger(__name__).critical(f"Error importing vnpy modules: {e}", exc_info=True)
    except Exception:
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
    def __init__(self):
        """Initializes the gateway service."""
        super().__init__() # Initialize the RpcServer base class

        self.logger = getLogger(__name__)

        # Create EventEngine for the gateway
        self.event_engine = EventEngine()

        # Create CTP gateway instance
        # Gateway name is important if multiple gateways run via RPC
        self.gateway: BaseGateway = CtpGateway(self.event_engine, "CTP_MarketData")

        # Prepare CTP gateway settings from config
        self.ctp_setting = {
            "userid": "",
            "password": "",
            "broker_id": "",
            "td_address": "", # Needed for login
            "md_address": "",
            "appid": "",
            "auth_code": "",
            "env": ""
        }
        self.ctp_setting: dict = load_json("connect_ctp.json")

        self._subscribe_list = [] # Store successful subscriptions

        self.logger.info("行情网关服务(RPC模式)初始化完成。")

    def process_event(self, event: Event):
        """Processes events from the EventEngine."""
        event_type = event.type
        if event_type == EVENT_TICK:
            tick: TickData = event.data
            # self.publish(f"tick.{tick.vt_symbol}", tick)  # <-- Commented out problematic call
            self.logger.debug(f"发布Tick: {tick.vt_symbol} - Price: {tick.last_price}")
            # --- Manually send multipart message ---
            try:
                topic_bytes = f"tick.{tick.vt_symbol}".encode('utf-8')
                data_bytes = pickle.dumps(tick)
                # Directly access self._socket_pub based on vnpy.rpc source
                self._socket_pub.send_multipart([topic_bytes, data_bytes])
                # self.logger.debug(f"手动发送 multipart Tick 成功 (主题: {topic_bytes.decode()})") # <-- 注释掉
            except AttributeError:
                 self.logger.error("AttributeError: 无法直接访问 self._socket_pub！RpcServer 内部可能已更改。")
            except Exception as e_manual_send:
                self.logger.exception(f"手动发送 multipart Tick 时出错: {e_manual_send}")
            # --- End manual send ---

        elif event_type == EVENT_LOG:
            log: LogData = event.data
            # self.publish(f"log", log) # <-- Commented out problematic call
            # Log locally as well using the service's logger
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
            self.logger.log(logger_level, f"[VNPY LOG - MDGW] {gateway_name} - {log.msg}")
            # --- Manually send multipart message for log ---
            try:
                topic_bytes = b"log"
                data_bytes = pickle.dumps(log)
                # Directly access self._socket_pub
                self._socket_pub.send_multipart([topic_bytes, data_bytes])
                # self.logger.debug(f"手动发送 multipart Log 成功") # <-- 注释掉
            except AttributeError:
                 self.logger.error("AttributeError: 无法直接访问 self._socket_pub！RpcServer 内部可能已更改。")
            except Exception as e_manual_log_send:
                self.logger.exception(f"手动发送 multipart Log 时出错: {e_manual_log_send}")
            # --- End manual send ---

        elif event_type == EVENT_CONTRACT:
            contract: ContractData = event.data
            # self.publish(f"contract.{contract.vt_symbol}", contract) # <-- Commented out problematic call
            # self.logger.debug(f"发布合约信息: {contract.vt_symbol}")
            # --- Manually send multipart message for contract ---
            try:
                topic_bytes = f"contract.{contract.vt_symbol}".encode('utf-8')
                data_bytes = pickle.dumps(contract)
                 # Directly access self._socket_pub
                self._socket_pub.send_multipart([topic_bytes, data_bytes])
                # self.logger.debug(f"手动发送 multipart Contract 成功 (主题: {topic_bytes.decode()})") # <-- 注释掉
            except AttributeError:
                 self.logger.error("AttributeError: 无法直接访问 self._socket_pub！RpcServer 内部可能已更改。")
            except Exception as e_manual_contract_send:
                 self.logger.exception(f"手动发送 multipart Contract 时出错: {e_manual_contract_send}")
             # --- End manual send ---

    # publish_data method is no longer needed, RpcServer.publish is used directly

    def start(self, rep_address=None, pub_address=None):
        """Starts the RpcServer, EventEngine, and connects the gateway."""
        if self.is_active():
            self.logger.warning("行情网关服务(RPC模式)已在运行中。")
            return

        self.logger.info("启动行情网关服务(RPC模式)...")

        # 1. Start the RpcServer (binds sockets, starts threads)
        try:
            # Use addresses from config
            super().start(
                rep_address=config.MARKET_DATA_REP_ADDRESS,
                pub_address=config.MARKET_DATA_PUB_ADDRESS
            )
            self.logger.info(f"RPC 服务器已启动。 REP: {config.MARKET_DATA_REP_ADDRESS}, PUB: {config.MARKET_DATA_PUB_ADDRESS}")
        except Exception as e:
            self.logger.exception(f"启动 RPC 服务器失败: {e}")
            return # Don't proceed if RPC server fails

        # 2. Start the EventEngine
        self.event_engine.register(EVENT_TICK, self.process_event)
        self.event_engine.register(EVENT_LOG, self.process_event)
        self.event_engine.register(EVENT_CONTRACT, self.process_event)
        self.event_engine.start()
        self.logger.info("事件引擎已启动。")

        # 3. Connect CTP Gateway
        self.logger.info("连接 CTP 网关...")
        try:
            # Load connection settings from JSON (or directly use self.ctp_setting)
            # Keep json loading for consistency if other parts rely on it
            self.logger.info(f"CTP 连接配置 (JSON): Env={self.ctp_setting['env']}")
            self.logger.info(f"使用配置连接 CTP: UserID={self.ctp_setting['userid']}, "
                             f"BrokerID={self.ctp_setting['broker_id']}, "
                             f"TD={self.ctp_setting['td_address']}")
            # Connect using the settings prepared in __init__
            self.gateway.connect(self.ctp_setting)
            self.logger.info("CTP 网关连接请求已发送。等待连接成功...")

            # Wait for connection (replace sleep with event-driven logic if possible)
            # TODO: Implement waiting for a specific CTP connection success event/log
            time.sleep(10)
            self.logger.info("CTP 网关假定连接成功（基于延时）。")

            # 4. Subscribe to market data
            self.logger.info("订阅行情...")
            self._subscribe_list = []
            for vt_symbol in config.SUBSCRIBE_SYMBOLS:
                try:
                    symbol, exchange_str = vt_symbol.split('.')
                    exchange = Exchange(exchange_str)
                    req = SubscribeRequest(symbol=symbol, exchange=exchange)
                    self.gateway.subscribe(req)
                    self._subscribe_list.append(vt_symbol)
                    self.logger.info(f"发送订阅请求: {vt_symbol}")
                    time.sleep(0.5) # Avoid flood
                except ValueError:
                     self.logger.error(f"错误的合约格式，跳过订阅: {vt_symbol} (应为 SYMBOL.EXCHANGE)")
                except Exception as err:
                     self.logger.exception(f"订阅 {vt_symbol} 时出错: {err}")
            self.logger.info(f"已发送 {len(self._subscribe_list)} 个合约的订阅请求。")

        except Exception as err:
            self.logger.exception(f"连接或订阅 CTP 网关时发生严重错误: {err}")
            self.stop() # Attempt to clean up if connection fails

        self.logger.info("行情网关服务(RPC模式)启动流程完成。")


    def stop(self):
        """Stops the service and cleans up resources."""
        if not self.is_active():
            self.logger.warning("行情网关服务(RPC模式)未运行。")
            return

        self.logger.info("停止行情网关服务(RPC模式)...")

        # 1. Stop the EventEngine first to prevent new events processing
        try:
            if self.event_engine._active: # Use _active attribute
                self.event_engine.stop()
                self.logger.info("事件引擎已停止。")
        except Exception as err:
            self.logger.exception(f"停止事件引擎时出错: {err}")

        # 2. Close the CTP Gateway connection
        if self.gateway:
            try:
                self.gateway.close()
                self.logger.info("CTP 网关已关闭。")
            except Exception as e:
                self.logger.exception(f"关闭 CTP 网关时出错: {e}")

        # 3. Stop the RpcServer (closes sockets, terminates context, joins threads)
        super().stop()
        self.logger.info("RPC 服务器已停止。")

        self.logger.info("行情网关服务(RPC模式)已停止。")

# --- Main execution block (for testing) ---
if __name__ == "__main__":
    # Ensure logger is configured before use
    try:
        from logger import setup_logging
        setup_logging(service_name="MarketDataGateway_DirectRun")
    except ImportError as log_err:
        print(f"CRITICAL: Failed to import or setup logger: {log_err}. Exiting.")
        sys.exit(1)

    logger_main = getLogger(__name__) # Get logger after setup

    logger_main.info("Starting direct test run (RPC Mode)...")
    gateway_service = MarketDataGatewayService()

    # Check config for required addresses
    if not hasattr(config, 'MARKET_DATA_REP_ADDRESS') or \
       not hasattr(config, 'MARKET_DATA_PUB_ADDRESS'):
        logger_main.critical("错误：配置文件 config.zmq_config 缺少 MARKET_DATA_REP_ADDRESS 或 MARKET_DATA_PUB_ADDRESS。")
        sys.exit(1)

    gateway_service.start() # Start the combined service

    try:
        # Keep the main thread alive while the RpcServer runs
        while gateway_service.is_active(): # Use is_active() from RpcServer
            time.sleep(1)
    except KeyboardInterrupt:
        logger_main.info("接收到中断信号，正在停止服务...")
    except Exception as e:
        logger_main.exception(f"主测试循环发生未处理错误：{e}")
    finally:
        logger_main.info("开始停止服务...")
        gateway_service.stop()
        logger_main.info("服务已安全停止 (RPC Mode)。") 