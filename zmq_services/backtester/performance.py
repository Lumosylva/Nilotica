from datetime import datetime
from typing import Any, Dict, List

import numpy as np
import pandas as pd

# +++ Add Logger Import +++
from utils.logger import logger  # Assuming logger is configured elsewhere

# Assuming vnpy constants are needed for Direction/Offset comparison
try:
    from vnpy.trader.constant import Direction, Offset
except ImportError:
    # Use logger.warning for import fallback
    logger.warning("vnpy constants not found, using dummy enums for performance calculation.")
    # Provide dummy values if vnpy is not installed in this exact context
    # This is mainly for standalone testing of this module
    class DummyEnum:
        def __init__(self, value):
            self.value = value
    class Direction:
        LONG = DummyEnum("多")
        SHORT = DummyEnum("空")
    class Offset:
        OPEN = DummyEnum("开")
        CLOSE = DummyEnum("平")
        CLOSETODAY = DummyEnum("平今")
        CLOSEYESTERDAY = DummyEnum("平昨")

from vnpy.trader.object import TradeData  # Ensure TradeData is imported

INITIAL_CAPITAL = 1_000_000  # 初始资金
RISK_FREE_RATE = 0.02  # 无风险利率 (年化)
ANNUAL_DAYS = 240      # 年化交易日数量 (根据市场调整)

def calculate_performance(trades: List[TradeData], contract_multipliers: Dict[str, int]) -> Dict[str, Any]:
    """
    Calculates key performance indicators based on a list of TradeData objects.

    Args:
        trades: A list of TradeData objects.
        contract_multipliers: A dictionary mapping symbol to its contract multiplier.

    Returns:
        A dictionary containing calculated performance metrics.
    """
    if not trades:
        # Replace print with logger.warning
        logger.warning("没有成交记录，无法计算性能指标。")
        # Use current time as a fallback placeholder if no trades exist
        placeholder_start_time = datetime.now()

        return {
            "total_trades": 0,
            "total_commission": 0.0,
            "total_pnl": 0.0,
            "final_equity": INITIAL_CAPITAL,
            "total_return": 0.0,
            "max_drawdown": 0.0,
            "sharpe_ratio": 0.0,
            "win_rate": 0.0,
            "profit_loss_ratio": 0.0,
            "equity_curve": pd.Series([INITIAL_CAPITAL], index=[placeholder_start_time]),
            "pnl_curve": pd.Series([], dtype=float)
        }

    # --- Data Preparation ---
    trades.sort(key=lambda x: x.datetime) # Ensure trades are time-sorted

    equity = INITIAL_CAPITAL
    max_equity = INITIAL_CAPITAL
    max_drawdown_pct = 0.0
    total_commission = 0.0
    total_pnl = 0.0
    win_trades = 0
    loss_trades = 0
    total_profit = 0.0
    total_loss = 0.0

    # Use first trade time minus a small delta as the start for equity curve
    start_time = trades[0].datetime
    if hasattr(start_time, 'tzinfo') and start_time.tzinfo is not None:
        start_time = start_time.tz_localize(None) # Make timezone naive if needed
    start_time -= pd.Timedelta(seconds=1)

    equity_history = {start_time: INITIAL_CAPITAL} # Use dict for easier updates
    pnl_history = {} # Use dict for PnL realization time

    open_trades = {} # key: symbol, value: list of open trade details (FIFO queue)

    for trade in trades:
        symbol = trade.symbol
        dt = trade.datetime
        # Ensure datetime is timezone-naive or consistent
        if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
             dt = dt.tz_localize(None)

        direction_val = trade.direction.value # Get enum value
        offset_val = trade.offset.value     # Get enum value
        price = trade.price
        volume = trade.volume
        # Get commission from the object, fallback to 0 if not present
        commission = getattr(trade, 'commission', 0.0)
        multiplier = contract_multipliers.get(symbol, 1)

        # Apply commission cost immediately
        total_commission += commission
        equity -= commission
        equity_history[dt] = equity # Record equity change due to commission

        if offset_val == Offset.OPEN.value:
            # Record open trade details
            trade_details = {
                'datetime': dt,
                'price': price,
                'volume': volume,
                'direction': direction_val
            }
            if symbol not in open_trades:
                open_trades[symbol] = []
            open_trades[symbol].append(trade_details)

        elif offset_val in [Offset.CLOSE.value, Offset.CLOSETODAY.value, Offset.CLOSEYESTERDAY.value]:
            # Process close trade
            if symbol not in open_trades or not open_trades[symbol]:
                # Replace print with logger.warning
                logger.warning(f"在 {dt} 收到 {symbol} 的平仓成交，但没有找到对应的开仓记录。跳过此平仓的盈亏计算。")
                continue

            # Match closing trade with the earliest open trade (FIFO)
            open_trade = open_trades[symbol].pop(0)
            open_price = open_trade['price']
            open_direction_val = open_trade['direction']

            # Calculate PnL for this closed pair
            trade_pnl = 0.0
            if open_direction_val == Direction.LONG.value: # Closing a long position
                trade_pnl = (price - open_price) * volume * multiplier
            elif open_direction_val == Direction.SHORT.value: # Closing a short position
                trade_pnl = (open_price - price) * volume * multiplier

            # Update total PnL and equity
            total_pnl += trade_pnl
            equity += trade_pnl
            equity_history[dt] = equity # Record equity change due to PnL realization
            pnl_history[dt] = trade_pnl # Record PnL at this time

            # Update win/loss statistics
            if trade_pnl > 0:
                win_trades += 1
                total_profit += trade_pnl
            elif trade_pnl < 0:
                loss_trades += 1
                total_loss += abs(trade_pnl)

        # Update maximum drawdown after every equity change event
        if equity > max_equity:
            max_equity = equity
        drawdown = max_equity - equity
        drawdown_pct = (drawdown / max_equity) * 100 if max_equity > 0 else 0
        if drawdown_pct > max_drawdown_pct:
            max_drawdown_pct = drawdown_pct

    # --- Convert history to Pandas Series ---
    equity_curve = pd.Series(equity_history).sort_index()
    pnl_curve = pd.Series(pnl_history).sort_index()


    # --- Calculate final metrics ---
    total_trades_closed = win_trades + loss_trades
    final_equity = equity_curve.iloc[-1] if not equity_curve.empty else INITIAL_CAPITAL
    total_return_pct = ((final_equity / INITIAL_CAPITAL) - 1) * 100 if INITIAL_CAPITAL > 0 else 0.0
    win_rate_pct = (win_trades / total_trades_closed) * 100 if total_trades_closed > 0 else 0.0

    avg_profit = total_profit / win_trades if win_trades > 0 else 0.0
    avg_loss = total_loss / loss_trades if loss_trades > 0 else 0.0
    profit_loss_ratio = avg_profit / avg_loss if avg_loss > 0 else float('inf') if avg_profit > 0 else 0.0 # Handle zero loss

    # --- Calculate Sharpe Ratio ---
    resample_freq = 'D' # Default to daily
    daily_equity = equity_curve.resample(resample_freq).last().ffill()
    daily_returns = daily_equity.pct_change().dropna()


    sharpe_ratio = 0.0
    if len(daily_returns) >= 3 :
        std_dev_daily_return = daily_returns.std()
        if std_dev_daily_return is not None and std_dev_daily_return != 0:
            excess_daily_returns = daily_returns - (RISK_FREE_RATE / ANNUAL_DAYS)
            avg_excess_return = excess_daily_returns.mean()
            sharpe_ratio = (avg_excess_return / std_dev_daily_return) * np.sqrt(ANNUAL_DAYS)


    return {
        "total_trades": total_trades_closed,
        "total_commission": total_commission,
        "total_pnl": total_pnl,
        "final_equity": final_equity,
        "total_return": total_return_pct,
        "max_drawdown": max_drawdown_pct,
        "sharpe_ratio": sharpe_ratio,
        "win_rate": win_rate_pct,
        "profit_loss_ratio": profit_loss_ratio,
        "equity_curve": equity_curve,
        "pnl_curve": pnl_curve
    }

def print_performance_report(results: Dict[str, Any]):
    """Prints a formatted performance report to the console."""
    # --- Keep print statements for formatted report output --- 
    print("\n--- Performance Report ---")
    print(f"Initial Capital:    {INITIAL_CAPITAL:,.2f}")
    print(f"Final Equity:       {results.get('final_equity', 0.0):,.2f}")
    print("--------------------------")
    print(f"Total Return:       {results.get('total_return', 0.0):.2f}%")
    print(f"Max Drawdown:       {results.get('max_drawdown', 0.0):.2f}%")
    print(f"Sharpe Ratio:       {results.get('sharpe_ratio', 0.0):.3f}")
    print("--------------------------")
    print(f"Total Trades Closed: {results.get('total_trades', 0)}")
    print(f"Win Rate:           {results.get('win_rate', 0.0):.2f}%")
    print(f"Profit/Loss Ratio:  {results.get('profit_loss_ratio', 0.0):.2f}")
    print("--------------------------")
    print(f"Total PnL:          {results.get('total_pnl', 0.0):,.2f}")
    print(f"Total Commission:   {results.get('total_commission', 0.0):,.2f}")
    print("--------------------------\n")

    # --- Plotting Section (Optional, depends on matplotlib availability) ---
    # Removed plotting call, but keeping the function structure
    # try:
    #     import matplotlib.pyplot as plt
    #     equity_curve = results.get('equity_curve')
    #     if equity_curve is not None and not equity_curve.empty:
    #         plt.figure(figsize=(12, 6))
    #         equity_curve.plot(title='Equity Curve')
    #         plt.ylabel('Equity')
    #         plt.grid(True)
    #         plt.show()
    #     else:
    #         print("Equity curve data is empty or missing, cannot plot.")

    #     # Optionally plot PnL curve if needed
    #     # pnl_curve = results.get('pnl_curve')
    #     # if pnl_curve is not None and not pnl_curve.empty:
    #     #     plt.figure(figsize=(12, 4))
    #     #     pnl_curve.cumsum().plot(title='Cumulative Realized PnL')
    #     #     plt.ylabel('Cumulative PnL')
    #     #     plt.grid(True)
    #     #     plt.show()

    # except ImportError:
    #     print("\nMatplotlib not found. Skipping equity curve plot.")
    #     print("Install it using: pip install matplotlib")
    # except Exception as e:
    #     print(f"\nError during plotting: {e}")

# Example Usage (for testing inside this file)
if __name__ == '__main__':
    # ... (example usage remains the same, but now expects TradeData objects if run standalone) ...
    # Note: Standalone example needs adjustment to create TradeData objects
    pass # Skip standalone execution for now as example data needs update