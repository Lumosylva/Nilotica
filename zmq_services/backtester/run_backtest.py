import argparse
import configparser
import os
import sys
import threading
import time
from datetime import datetime
from typing import Dict, Tuple

import zmq

# Add project root to Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')) # Go up two levels
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from zmq_services.backtester.simulation_engine import SimulationEngineService
# We'll run the existing StrategySubscriber, just connecting it to backtest URLs
from config import zmq_config as config
from zmq_services.strategy_engine import StrategyEngine
# --- 修改导入，移除 plot_performance ---
from zmq_services.backtester.performance import calculate_performance, print_performance_report # 移除 plot_performance
# --- 结束修改 ---
from utils.logger import logger, setup_logging # Import both


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
        default="SimpleStrategy", # Placeholder for potentially selecting different strategies
        help="要运行的策略类的名称（当前使用 StrategySubscriber）"
    )
    # Add arguments for strategy parameters if needed
    # +++ Add log level argument +++
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the minimum logging level.")
    args = parser.parse_args()
    
    # --- Setup Logging EARLY --- 
    # Use a specific name for backtest runner
    setup_logging(service_name=f"BacktestRunner[{args.date}]", level=args.log_level.upper())

    # Use logger for configuration info
    logger.info(f"--- 回测配置 ---")
    logger.info(f"回测日期: {args.date}")
    logger.info(f"策略类 Arg: {args.strategy}") # Note: actual strategy loaded from placeholder config below
    logger.info(f"数据源路径: {os.path.abspath(config.BACKTEST_DATA_SOURCE_PATH)}")
    logger.info(f"--- 回测 ZMQ URLs ---")
    logger.info(f"  市场行情数据 PUB: {config.BACKTEST_DATA_PUB_URL}")
    logger.info(f"  订单报告 PUB: {config.BACKTEST_ORDER_REPORT_PUB_URL}")
    logger.info(f"  订单请求 PULL: {config.BACKTEST_ORDER_REQUEST_PULL_URL}")
    logger.info(f"----------------------------")

    # +++ 加载产品信息 +++
    # 修正路径：从 backtester 目录出发，向上两级到项目根目录，再进入 config
    config_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', 'config')) # <--- 这里需要向上两级
    info_filepath = os.path.join(config_dir, 'project_files', 'product_info.ini')
    logger.info(f"尝试从 {info_filepath} 加载产品信息...")
    commission_rules, contract_multipliers = load_product_info(info_filepath)
    if not commission_rules or not contract_multipliers:
        logger.error("错误：未能加载手续费规则或合约乘数，无法继续回测。")
        return
    # +++ 结束加载 +++

    # 1. Initialize Simulation Engine (传入加载的配置)
    logger.info("初始化模拟引擎...")
    engine = SimulationEngineService(
        data_source_path=config.BACKTEST_DATA_SOURCE_PATH,
        backtest_md_pub_url=config.BACKTEST_DATA_PUB_URL,
        backtest_report_pub_url=config.BACKTEST_ORDER_REPORT_PUB_URL,
        backtest_order_pull_url=config.BACKTEST_ORDER_REQUEST_PULL_URL,
        date_str=args.date,
        commission_rules=commission_rules,         # <--- 传入加载的规则
        contract_multipliers=contract_multipliers, # <--- 传入加载的乘数
        slippage=2.0 
    )
    logger.info("加载回测数据...")
    if not engine.load_data():
        logger.error("错误：未能加载回测数据，无法继续。")
        engine.stop() # Clean up engine resources
        return

    # 2. Initialize Strategy Instance (connecting to BACKTEST URLs)
    logger.info(f"初始化策略: {args.strategy}...")
    # For now, hardcode StrategyEngine, later could dynamically load strategy class
    # +++ Use StrategyEngine instead of StrategySubscriber +++
    # strategy_config = ... # Need to define strategy config similar to run_strategy_engine.py
    # Placeholder config for now - MUST be adapted if SA509 or other strategies are used!
    # This part needs significant rework if we want to run the specific SA509 strategy in backtest
    # as it requires the setting dictionary.
    # For a *generic* backtest, the old subscriber might have worked, but 
    # StrategyEngine now requires strategies_config. 
    
    # --- TEMPORARY Placeholder --- 
    # This assumes a generic strategy or needs the actual config used by StrategyEngine
    # Define a placeholder STRATEGIES_CONFIG if not using SA509 specifically, or load the real one
    # e.g., from run_strategy_engine.py
    placeholder_strategies_config = {
        # Add a simple dummy strategy config if needed, or load the real one
        # e.g., from run_strategy_engine.py
        "SA505_Backtest_1": {
             # --- Update class path --- 
             # "strategy_class": "zmq_services.strategies.sa509_strategy.SA509LiveStrategy",
             "strategy_class": "zmq_services.strategies.threshold_strategy.ThresholdLiveStrategy",
             # --- End Update --- 
             "vt_symbol": "SA505.CZCE", # Example, adjust based on backtest data
             "setting": {              
                 "entry_threshold": "1310.0",
                 "profit_target_ticks": 10,        
                 "stop_loss_ticks": 5,           
                 "price_tick": "1.0",              
                 "order_volume": "1.0",            
                 "order_price_offset_ticks": 0     
             }
         }
    }

    if args.strategy == "SimpleStrategy": # Keeping the arg name for now, but logic uses the config
        strategy = StrategyEngine(
            gateway_pub_url=config.BACKTEST_DATA_PUB_URL.replace("*", "localhost"),
            order_req_url=config.BACKTEST_ORDER_REQUEST_PULL_URL.replace("*", "localhost"), # Use PULL for backtest engine
            order_report_url=config.BACKTEST_ORDER_REPORT_PUB_URL.replace("*", "localhost"),
            strategies_config=placeholder_strategies_config # Pass the strategy config dictionary
        )
    # --- End TEMPORARY Placeholder ---
    # +++ End Class Change +++
    else:
        logger.error(f"错误: 未知的策略名称 '{args.strategy}'")
        engine.stop()
        return

    # 3. Start Strategy in a Background Thread
    logger.info("在后台线程中启动策略...")
    strategy_thread = threading.Thread(target=run_strategy_in_thread, args=(strategy,))
    strategy_thread.daemon = True # Allow main program to exit even if strategy thread hangs (optional)
    strategy_thread.start()

    # Give the strategy a moment to initialize and connect sockets
    logger.info("等待策略初始化...")
    time.sleep(2) # Adjust as needed

    # 4. Run Simulation Engine in the Main Thread (this will block)
    logger.info("启动模拟引擎运行循环...")
    engine_stopped_cleanly = False
    try:
        engine.run_simulation() # This runs until data ends or error/interrupt
        engine_stopped_cleanly = True # Mark if simulation completes normally
    except KeyboardInterrupt:
         logger.info("\n主程序检测到 Ctrl+C，正在停止模拟引擎...")
         engine.stop() # Ensure engine stops cleanly
    except Exception as e:
         logger.exception(f"模拟引擎运行时发生错误: {e}")
         engine.stop()

    # 5. Signal Strategy Thread to Stop (if engine stopped cleanly or errored)
    # If engine stopped, its ZMQ context is terminated, strategy should detect this via ZMQError ETERM
    # However, explicitly calling stop is safer.
    if strategy.running: # Check if strategy is still running
        logger.info("通知策略线程停止...")
        strategy.stop() # Call stop method (only closes sockets now)

    # 6. Wait for Strategy Thread to Finish
    logger.info("等待策略线程结束...")
    strategy_thread.join(timeout=10) # Increased timeout
    if strategy_thread.is_alive():
        logger.warning("警告：策略线程在超时后仍在运行。")

    # +++ 在线程结束后终止 ZMQ Context +++
    logger.info("策略线程已结束，准备终止策略 ZMQ Context...")
    if strategy.context and not strategy.context.closed:
        try:
            logger.info("正在调用 strategy.context.term()...")
            strategy.context.term()
            logger.info("策略 ZMQ Context 已终止。")
        except zmq.ZMQError as e:
            logger.error(f"终止策略 ZMQ Context 时出错: {e}")
    else:
        logger.warning("策略 ZMQ Context 未找到或已关闭，跳过终止。")
    # +++ 结束终止 +++

    # 7. Calculate and Display Backtest Results (传入加载的乘数)
    logger.info("\n--- 回测完成 ---") 
    if strategy:
        all_trades = strategy.trades 
        # --- 调用详细性能计算和打印函数 (传入加载的乘数) ---
        performance_results = calculate_performance(all_trades, contract_multipliers) # <--- 传入加载的乘数
        print_performance_report(performance_results)
    else:
        logger.error("错误：无法获取策略实例以计算性能。")

    logger.info("\n回测流程结束。")


if __name__ == "__main__":
    main()
