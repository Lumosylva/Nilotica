import argparse
import configparser
import os
import sys
import threading
import time
from datetime import datetime
from typing import Dict, List, Tuple

import msgpack
import zmq

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')) # Go up two levels
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# +++ Import ConfigManager +++
from utils.config_manager import ConfigManager
from utils.logger import logger, setup_logging  # Import both

# +++ Import TradeData and creation helper +++
from vnpy.trader.object import TradeData
from zmq_services.backtester.performance import calculate_performance, print_performance_report
from zmq_services.backtester.simulation_engine import SimulationEngineService

# Assuming create_trade_from_dict is defined within simulation_engine or accessible
# If not, we need to define it here or import it properly.
# Let's assume it's defined in simulation_engine for now
# Or even better, import the helper from vnpy.trader.utility if it exists there
# Or define a simple one here if needed.
# Let's try importing from simulation_engine first:
# We'll run the existing StrategySubscriber, just connecting it to backtest URLs
from zmq_services.strategy_engine import StrategyEngine, create_trade_from_dict


# --- Function to run strategy in a separate thread ---
def run_strategy_in_thread(strategy_instance):
    """Target function to run the strategy's start method."""
    # Use logger instead of print
    logger.info("策略线程: 启动...")
    try:
        # Assuming strategy's start() method contains the main loop
        strategy_instance.start()
    except Exception as e:
        # Use logger.exception for errors with traceback
        logger.exception(f"策略线程运行时发生错误: {e}")
    logger.info("策略线程: 已结束。")


# +++ 添加函数：在线程中收集成交回报 +++
def collect_trades_in_thread(report_url: str, trades_list: List[TradeData], stop_event: threading.Event):
    """Connects to the report publisher and collects TradeData."""
    context = zmq.Context()
    subscriber = context.socket(zmq.SUB)
    subscriber.setsockopt(zmq.SUBSCRIBE, b"trade.") # Subscribe only to trade topics
    try:
        # Replace wildcard with localhost if needed
        connect_url = report_url.replace("tcp://*", "tcp://localhost", 1)
        subscriber.connect(connect_url)
        logger.info(f"成交回报收集器连接到: {connect_url}")

        poller = zmq.Poller()
        poller.register(subscriber, zmq.POLLIN)

        while not stop_event.is_set():
            try:
                socks = dict(poller.poll(timeout=100)) # Poll with timeout
                if subscriber in socks and socks[subscriber] == zmq.POLLIN:
                    topic_bytes, data_bytes = subscriber.recv_multipart(zmq.NOBLOCK)
                    topic_str = topic_bytes.decode('utf-8')
                    logger.debug(f"[TradeCollector] Received message on topic: {topic_str}")
                    data_dict = msgpack.unpackb(data_bytes, raw=False)
                    # Attempt to create TradeData object
                    trade_object = create_trade_from_dict(data_dict)
                    if trade_object:
                        logger.debug(f"[TradeCollector] Appending trade: {trade_object.vt_tradeid} OrderID: {trade_object.vt_orderid}")
                        trades_list.append(trade_object)
                    else:
                        logger.warning(f"[TradeCollector] Failed to create TradeData from dict: {data_dict}")

            except zmq.Again:
                continue # Timeout, check stop_event again
            except (msgpack.UnpackException, TypeError, ValueError) as e_unpack:
                logger.error(f"[TradeCollector] Error unpacking trade data: {e_unpack}")
            except Exception as e:
                if not stop_event.is_set(): # Avoid logging errors during shutdown
                    logger.exception(f"[TradeCollector] Error receiving/processing trade report: {e}")
                time.sleep(0.1) # Avoid busy-looping on errors

    except Exception as e_conn:
        logger.exception(f"[TradeCollector] Error connecting or in main loop: {e_conn}")
    finally:
        logger.info("[TradeCollector] Stopping...")
        if subscriber and not subscriber.closed:
            subscriber.close()
        if context and not context.closed:
            context.term()
        logger.info("[TradeCollector] Thread finished.")
# +++ 结束添加 +++


# +++ 添加函数：加载产品信息 (与 order_gateway 中类似) +++
def load_product_info(filepath: str) -> Tuple[Dict, Dict]:
    """Loads commission rules and multipliers from an INI file."""
    parser = configparser.ConfigParser()
    if not os.path.exists(filepath):
        # Use logger.error
        logger.error(f"产品信息文件未找到 {filepath}")
        return {}, {}
    try:
        parser.read(filepath, encoding='utf-8')
    except Exception as e:
        # Use logger.error
        logger.error(f"读取产品信息文件 {filepath} 时出错: {e}")
        return {}, {}

    commission_rules = {}
    contract_multipliers = {}
    for symbol in parser.sections():
        if not parser.has_option(symbol, 'multiplier'): continue
        try:
            multiplier = parser.getfloat(symbol, 'multiplier')
            contract_multipliers[symbol] = multiplier
            rule = {
                "open_rate": parser.getfloat(symbol, 'open_rate', fallback=0.0),
                "close_rate": parser.getfloat(symbol, 'close_rate', fallback=0.0),
                "open_fixed": parser.getfloat(symbol, 'open_fixed', fallback=0.0),
                "close_fixed": parser.getfloat(symbol, 'close_fixed', fallback=0.0),
                "min_commission": parser.getfloat(symbol, 'min_commission', fallback=0.0)
            }
            commission_rules[symbol] = rule
        except Exception as e:
            # Use logger.warning
            logger.warning(f"处理文件 {filepath} 中 [{symbol}] 时出错: {e}")
    # Use logger.info
    logger.info(f"从 {filepath} 加载了 {len(contract_multipliers)} 个合约的乘数和 {len(commission_rules)} 个合约的手续费规则。")
    return commission_rules, contract_multipliers
# +++ 结束添加 +++


# --- Main Backtest Execution ---
def main():
    parser = argparse.ArgumentParser(description="运行回测模拟")
    parser.add_argument(
        "--date",
        type=str,
        default=datetime.now().strftime('%Y%m%d'),
        help="Backtest date in YYYYMMDD format (default: today)"
    )
    parser.add_argument(
        "--strategy",
        type=str,
        default="DefaultFromConfig", # Changed default, will load from config
        help="策略配置的名称 (从 strategies_setting.json 或类似文件加载). 如果为 DefaultFromConfig, 则使用第一个找到的策略。"
    )
    # +++ Add --config-env argument +++
    parser.add_argument(
        "--config-env",
        default="backtest", # Default to backtest for backtesting
        type=str,
        help="The configuration environment to load (e.g., 'dev', 'prod', 'backtest'). Affects default paths, ZMQ URLs, and potentially strategy configs."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the minimum logging level.")
    args = parser.parse_args()
    
    # --- Setup Logging with config_env ---
    setup_logging(service_name=f"BacktestRunner[{args.date}]", level=args.log_level.upper(), config_env=args.config_env)

    # --- Initialize ConfigManager AFTER parsing args --- 
    config_service = ConfigManager(environment=args.config_env)

    # Log configuration details
    logger.info(f"--- 回测配置 (Config Env: '{args.config_env}') ---")
    logger.info(f"回测日期: {args.date}")
    logger.info(f"策略 Arg: {args.strategy}") 

    # Get paths and URLs from the environment-aware ConfigManager
    backtest_data_source_path = config_service.get_backtest_data_source_path()
    backtest_data_pub_url = config_service.get_global_config("zmq_addresses.backtest_data_pub")
    backtest_order_report_pub_url = config_service.get_global_config("zmq_addresses.backtest_order_report_pub")
    backtest_order_request_pull_url = config_service.get_global_config("zmq_addresses.backtest_order_request_pull")
    product_info_filepath = config_service.get_product_info_path() # Get product info path
    
    # --- Load Strategy Config --- 
    all_strategies_config = config_service.get_strategies_config()
    if not all_strategies_config:
        logger.error(f"未能加载任何策略配置。请检查配置中指定的策略文件路径。")
        return
        
    strategies_to_run = {}
    if args.strategy == "DefaultFromConfig":
        if all_strategies_config:
             first_strategy_name = next(iter(all_strategies_config))
             strategies_to_run = {first_strategy_name: all_strategies_config[first_strategy_name]}
             logger.info(f"未指定特定策略，将运行第一个找到的策略配置: '{first_strategy_name}'")
        else:
             logger.error("策略参数为 DefaultFromConfig，但未加载到任何策略配置。")
             return
    elif args.strategy in all_strategies_config:
        strategies_to_run = {args.strategy: all_strategies_config[args.strategy]}
        logger.info(f"将运行指定的策略配置: '{args.strategy}'")
    else:
         logger.error(f"指定的策略名称 '{args.strategy}' 在加载的策略配置中未找到。可用配置: {list(all_strategies_config.keys())}")
         return
    # --- End Load Strategy Config ---

    # Validate necessary configs
    required_keys = {
        "Data Source Path": backtest_data_source_path,
        "ZMQ Backtest Data Pub URL": backtest_data_pub_url,
        "ZMQ Backtest Order Report Pub URL": backtest_order_report_pub_url,
        "ZMQ Backtest Order Request Pull URL": backtest_order_request_pull_url,
        "Product Info File Path": product_info_filepath
    }
    missing_keys = [k for k, v in required_keys.items() if not v]
    if missing_keys:
        logger.error(f"错误：未能从配置 ('{args.config_env}' environment) 中获取以下必需的配置: {missing_keys}")
        return

    logger.info(f"数据源路径: {backtest_data_source_path}") # Already absolute
    logger.info(f"产品信息文件: {product_info_filepath}") # Already absolute
    logger.info(f"--- 回测 ZMQ URLs ---")
    logger.info(f"  市场行情数据 PUB: {backtest_data_pub_url}")
    logger.info(f"  订单报告 PUB: {backtest_order_report_pub_url}")
    logger.info(f"  订单请求 PULL: {backtest_order_request_pull_url}")
    logger.info(f"----------------------------")

    # Load Product Info using the path from config
    logger.info(f"尝试从 {product_info_filepath} 加载产品信息...")
    commission_rules, contract_multipliers = load_product_info(product_info_filepath)
    if not commission_rules or not contract_multipliers:
        logger.error("错误：未能加载手续费规则或合约乘数，无法继续回测。")
        return

    # +++ Extract strategy symbols for the SimulationEngine +++
    symbols_for_simulation = []
    if strategies_to_run:
        for config_name, strategy_config_block in strategies_to_run.items():
            vt_symbol = strategy_config_block.get("vt_symbol")
            if vt_symbol:
                if vt_symbol not in symbols_for_simulation:
                    symbols_for_simulation.append(vt_symbol)
                logger.info(f"策略 '{config_name}' 需要合约: {vt_symbol}")
            else:
                logger.warning(f"策略 '{config_name}' 的配置中缺少 'vt_symbol'。")
    
    if not symbols_for_simulation:
        logger.warning("未能从选定策略中提取任何 vt_symbol 用于数据加载。SimulationEngine 将尝试加载所有可用数据。")
        # 或者，如果严格要求，这里可以报错返回
        # logger.error("错误：必须至少有一个策略指定 vt_symbol 才能运行回测。")
        # return

    logger.info(f"将为 SimulationEngine 加载以下合约的数据: {symbols_for_simulation if symbols_for_simulation else '所有可用合约'}")
    # +++ 结束提取 +++

    # +++ 创建共享列表和停止事件 +++
    all_trades_collected: List[TradeData] = [] # Use a new name to avoid confusion
    stop_collector_event = threading.Event()
    # +++ 结束创建 +++

    # 1. Initialize Simulation Engine
    logger.info("初始化模拟引擎...")
    engine = SimulationEngineService(
        data_source_path=backtest_data_source_path, 
        backtest_md_pub_url=backtest_data_pub_url, 
        backtest_report_pub_url=backtest_order_report_pub_url, 
        backtest_order_pull_url=backtest_order_request_pull_url, 
        date_str=args.date,
        commission_rules=commission_rules,         
        contract_multipliers=contract_multipliers, 
        # +++ Get slippage from config if available, otherwise default +++
        slippage=config_service.get_global_config("backtester_settings.slippage", 2.0),
        strategy_symbols=symbols_for_simulation if symbols_for_simulation else None # +++ 传递提取的合约 +++
    )
    logger.info(f"模拟引擎滑点设置为: {engine.slippage}") # Log the slippage used
    
    logger.info("加载回测数据...")
    if not engine.load_data():
        logger.error("错误：未能加载回测数据，无法继续。")
        engine.stop() 
        return

    # 2. Initialize Strategy Instance 
    logger.info(f"初始化策略引擎，使用配置: {list(strategies_to_run.keys())}")
    strategy_engine_instance = None # Initialize for finally block
    try:
        # StrategyEngine now uses the config passed to it
        strategy_engine_instance = StrategyEngine(
            config_manager=config_service, # +++ Pass config_service
            gateway_pub_url=backtest_data_pub_url.replace("*", "localhost"), 
            order_gw_rep_url=backtest_order_request_pull_url.replace("*", "localhost"), # For backtest, this will be PULL by SimulationEngine
            order_report_url=backtest_order_report_pub_url.replace("*", "localhost"), 
            strategies_config=strategies_to_run # Pass the selected strategy config(s)
        )
        # --- Mark StrategyEngine as being in backtest mode --- 
        strategy_engine_instance.is_backtest_mode = True 
        # --- Set up the PUSH socket for sending orders --- 
        push_socket_url = backtest_order_request_pull_url.replace("*", "localhost")
        logger.info(f"策略引擎 (回测模式): 创建 PUSH socket 连接到 {push_socket_url}")
        strategy_engine_instance.order_pusher = strategy_engine_instance.context.socket(zmq.PUSH)
        strategy_engine_instance.order_pusher.setsockopt(zmq.LINGER, 0)
        strategy_engine_instance.order_pusher.connect(push_socket_url)
        # --- End PUSH socket setup --- 

    except Exception as e:
         logger.exception(f"初始化 StrategyEngine 时出错: {e}")
         engine.stop() # Stop simulation engine if strategy fails to init
         return
         
    if not strategy_engine_instance or not strategy_engine_instance.strategies:
        logger.error("未能成功初始化或加载任何策略实例到 StrategyEngine。")
        engine.stop()
        return

    # +++ 启动成交回报收集线程 +++
    logger.info("启动成交回报收集线程...")
    trade_collector_thread = threading.Thread(
        target=collect_trades_in_thread,
        args=(backtest_order_report_pub_url, all_trades_collected, stop_collector_event),
        daemon=True
    )
    trade_collector_thread.start()
    # +++ 结束启动 +++

    # 3. Start Strategy in a Background Thread
    logger.info("在后台线程中启动策略...")
    strategy_thread = threading.Thread(target=run_strategy_in_thread, args=(strategy_engine_instance,))
    strategy_thread.daemon = True
    strategy_thread.start()

    logger.info("等待策略初始化...")
    time.sleep(2) 

    # 4. Run Simulation Engine in the Main Thread
    logger.info("启动模拟引擎运行循环...")
    engine_stopped_cleanly = False
    try:
        engine.run_simulation() 
        engine_stopped_cleanly = True 
    except KeyboardInterrupt:
         logger.info("\n主程序检测到 Ctrl+C，正在停止模拟引擎...")
         engine.stop()
    except Exception as e:
         logger.exception(f"模拟引擎运行时发生错误: {e}")
         engine.stop()

    # +++ 停止成交回报收集器 +++
    logger.info("通知成交回报收集线程停止...")
    stop_collector_event.set()
    # +++ 结束停止通知 +++

    # 5. Signal Strategy Thread to Stop
    if strategy_engine_instance.running:
        logger.info("通知策略线程停止...")
        strategy_engine_instance.stop()

    # 6. Wait for Strategy Thread to Finish
    logger.info("等待策略线程结束...")
    strategy_thread.join(timeout=10)
    if strategy_thread.is_alive():
        logger.warning("警告：策略线程在超时后仍在运行。")

    # +++ 等待成交回报收集线程结束 +++
    logger.info("等待成交回报收集线程结束...")
    trade_collector_thread.join(timeout=5) # Give it some time to finish cleanly
    if trade_collector_thread.is_alive():
        logger.warning("警告: 成交回报收集线程在超时后仍在运行。")
    # +++ 结束等待 +++

    # 7. Calculate and Display Backtest Results
    logger.info("\n--- 回测完成 ---") 
    if all_trades_collected:
        logger.info(f"共收集到 {len(all_trades_collected)} 条成交记录用于分析。")
        performance_results = calculate_performance(all_trades_collected, contract_multipliers)
        print_performance_report(performance_results)
    else:
        logger.warning("没有收集到成交记录，无法计算性能指标。")

    logger.info("\n回测流程结束。")


if __name__ == "__main__":
    main()
