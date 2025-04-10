from datetime import datetime, date, timedelta
from typing import List, Dict, Optional
import math
import numpy as np # <--- 导入 numpy

# 尝试导入 vnpy 常量，如果失败则使用字符串比较（需要确保传入的 trades 字典中值为字符串）
try:
    from vnpy.trader.constant import Direction, Offset
except ImportError:
    print("Warning: vnpy.trader.constant not found in performance.py. Using string comparison for Direction/Offset.")
    class Direction: LONG = "多"; SHORT = "空"
    class Offset: OPEN = "开"; CLOSE = "平"; CLOSETODAY="平今"; CLOSEYESTERDAY="平昨"

def calculate_performance(
    trades: List[Dict],
    contract_multipliers: Dict[str, float],
    initial_capital: float = 1_000_000.0, # 默认初始资金 100 万
    risk_free_rate: float = 0.02, # 默认无风险利率 2% (用于后续夏普比率计算)
    trading_days_per_year: int = 252 # 年化因子
) -> Optional[Dict]:
    """
    Calculates performance metrics from a list of trade records.

    Args:
        trades: List of trade dictionaries, each containing keys like
                'datetime', 'symbol', 'direction', 'offset', 'price',
                'volume', 'commission', etc.
        contract_multipliers: Dictionary mapping symbol to its contract multiplier.
        initial_capital: Starting capital for calculating returns.
        risk_free_rate: Annual risk-free rate for Sharpe ratio calculation.
        trading_days_per_year: Number of trading days in a year for annualization.

    Returns:
        A dictionary containing calculated performance metrics, or None if no trades.
    """
    if not trades:
        return None

    # --- 0. Sort trades by datetime ---
    trades.sort(key=lambda x: x['datetime'])

    # --- 1. Basic Metrics ---
    total_trades = len(trades)
    total_commission = sum(trade.get('commission', 0.0) for trade in trades)

    # --- 2. Calculate Realized PnL & Track Positions ---
    realized_pnl = 0.0
    positions = {} # {symbol: {'volume': float, 'cost': float, 'pnl': float}}

    # --- Equity Curve Calculation ---
    equity = initial_capital
    equity_curve = [(trades[0]['datetime'].replace(hour=0, minute=0, second=0, microsecond=0), initial_capital)] # Start of first trade day
    daily_equity = {equity_curve[0][0].date(): initial_capital} # Track closing equity per day

    # --- Lists to store round trip results ---
    # Moved initialization outside the loop
    if 'round_trip_pnls' not in locals(): round_trip_pnls = []

    for trade in trades:
        symbol = trade['symbol']
        direction = trade['direction']
        offset = trade['offset']
        price = trade['price']
        volume = trade['volume']
        commission = trade.get('commission', 0.0)
        multiplier = contract_multipliers.get(symbol, 1.0) # Default to 1 if not found

        # Initialize position if first time seeing this symbol
        if symbol not in positions:
            positions[symbol] = {'volume': 0.0, 'cost': 0.0, 'entry_price_avg': 0.0} # Add avg entry price tracking

        pos = positions[symbol]
        trade_value = price * volume * multiplier
        position_change = 0.0

        # Update PnL based on trade type
        if offset == Offset.OPEN.value:
            # --- Opening logic (update cost and average price) ---
            current_total_cost = pos['volume'] * pos['entry_price_avg'] * multiplier # Cost before this trade
            if direction == Direction.LONG.value:
                position_change = volume
                new_total_volume = pos['volume'] + volume
                new_total_cost = current_total_cost + trade_value
            else: # Short open
                position_change = -volume
                new_total_volume = pos['volume'] - volume
                new_total_cost = current_total_cost - trade_value # Cost is negative for shorts

            pos['volume'] = new_total_volume
            # Update average entry price (weighted average)
            if abs(pos['volume']) > 1e-9:
                pos['entry_price_avg'] = (new_total_cost / pos['volume']) / multiplier
            else: # Should not happen on open, but safeguard
                 pos['entry_price_avg'] = 0.0

            realized_pnl -= commission
            equity -= commission

        elif offset in [Offset.CLOSE.value, Offset.CLOSETODAY.value, Offset.CLOSEYESTERDAY.value]:
            if abs(pos['volume']) < 1e-9: # Check if position exists
                 print(f"警告: 尝试平仓 {symbol} 时无持仓...")
                 realized_pnl -= commission
                 equity -= commission
                 continue

            close_volume = min(volume, abs(pos['volume']))
            if volume > abs(pos['volume']) + 1e-9: # Allow for small float errors
                print(f"警告: 平仓量 {volume} 大于持仓量 {abs(pos['volume']):.2f} for {symbol}...")

            # Determine if closing long or short position
            close_long = (direction == Direction.SHORT.value and pos['volume'] > 0)
            close_short = (direction == Direction.LONG.value and pos['volume'] < 0)

            if close_long or close_short:
                # --- Closing logic (calculate PnL based on average entry) ---
                entry_price = pos['entry_price_avg']
                pnl_per_share_gross = 0.0
                if close_long: # Closing a long position (sell)
                    pnl_per_share_gross = price - entry_price
                    position_change = -close_volume
                else: # Closing a short position (buy)
                    pnl_per_share_gross = entry_price - price
                    position_change = close_volume

                round_trip_pnl = pnl_per_share_gross * close_volume * multiplier - commission # PnL for this round trip portion
                round_trip_pnls.append(round_trip_pnl) # Store PnL for win rate/profit factor

                realized_pnl += round_trip_pnl # Accumulate total realized PnL
                equity += round_trip_pnl # Update equity

                # Update position volume (cost basis update is implicitly handled by avg price)
                pos['volume'] += position_change
                if abs(pos['volume']) < 1e-9: # Reset average price if flat
                    pos['volume'] = 0.0
                    pos['entry_price_avg'] = 0.0

            else: # Mismatched direction/offset
                print(f"警告: 平仓方向与持仓不符...")
                realized_pnl -= commission
                equity -= commission
                continue

        # --- Update Equity Curve & Daily Equity ---
        trade_dt = trade['datetime']
        equity_curve.append((trade_dt, equity))
        daily_equity[trade_dt.date()] = equity # Update closing equity for the day

    # --- Calculate Max Drawdown ---
    max_drawdown = 0.0
    peak_equity = initial_capital
    for dt, current_equity in equity_curve:
        peak_equity = max(peak_equity, current_equity)
        drawdown = (peak_equity - current_equity) / peak_equity if peak_equity > 0 else 0
        max_drawdown = max(max_drawdown, drawdown)

    # --- Calculate Sharpe Ratio (based on daily returns) ---
    sharpe_ratio = 0.0
    annualized_sharpe = 0.0

    # Sort daily equity by date
    sorted_dates = sorted(daily_equity.keys())
    daily_equity_values = [daily_equity[d] for d in sorted_dates]

    if len(daily_equity_values) > 1:
        daily_returns = np.diff(daily_equity_values) / daily_equity_values[:-1]
        # Handle potential division by zero if equity drops to 0
        daily_returns = np.nan_to_num(daily_returns)

        if len(daily_returns) > 0 and np.std(daily_returns) != 0:
            # Calculate daily statistics
            mean_daily_return = np.mean(daily_returns)
            std_daily_return = np.std(daily_returns)

            # Calculate daily risk-free rate
            daily_risk_free = (1 + risk_free_rate)**(1/trading_days_per_year) - 1

            # Calculate Sharpe Ratio
            sharpe_ratio = (mean_daily_return - daily_risk_free) / std_daily_return

            # Annualize Sharpe Ratio
            annualized_sharpe = sharpe_ratio * math.sqrt(trading_days_per_year)

    # --- TODO: 4. Calculate Win Rate & Profit Factor ---
    # Moved calculation logic here from the loop
    winning_trades = sum(1 for pnl in round_trip_pnls if pnl > 0)
    losing_trades = sum(1 for pnl in round_trip_pnls if pnl < 0)
    total_round_trips = winning_trades + losing_trades

    win_rate = winning_trades / total_round_trips if total_round_trips > 0 else 0.0

    gross_profit = sum(pnl for pnl in round_trip_pnls if pnl > 0)
    gross_loss = abs(sum(pnl for pnl in round_trip_pnls if pnl < 0))

    profit_factor = gross_profit / gross_loss if gross_loss > 0 else math.inf # Handle division by zero

    # --- 5. Assemble Results ---
    results = {
        "total_trades": total_trades,
        "total_commission": round(total_commission, 2),
        "realized_pnl": round(realized_pnl, 2),
        # --- New metrics ---
        "end_equity": round(equity, 2),
        "total_return": (equity - initial_capital) / initial_capital if initial_capital else 0,
        "max_drawdown": round(max_drawdown, 4),
        "sharpe_ratio": round(annualized_sharpe, 3),
        # --- New metrics ---
        "win_rate": round(win_rate, 3),
        "profit_factor": round(profit_factor, 3),
    }

    return results

def print_performance_report(results: Optional[Dict]):
    """Prints the performance report in a readable format."""
    print("\n--- 回测性能报告 ---")
    if not results:
        print("没有计算任何性能指标 (无成交记录)。")
        print("--------------------")
        return

    print(f"总成交次数:     {results.get('total_trades', 'N/A')}")
    print(f"总手续费:       {results.get('total_commission', 'N/A'):.2f}")
    print(f"已实现盈亏:     {results.get('realized_pnl', 'N/A'):.2f}")
    print(f"期末权益:       {results.get('end_equity', 'N/A'):.2f}")
    print(f"总回报率:       {results.get('total_return', 'N/A'):.2%}") # Format as percentage
    print("---")
    print(f"最大回撤:       {results.get('max_drawdown', 'N/A'):.2%}") # Format as percentage
    print(f"夏普比率:       {results.get('sharpe_ratio', 'N/A'):.3f}")
    print("---")
    # --- Print new metrics ---
    print(f"胜率:           {results.get('win_rate', 'N/A'):.1%}") # Format as percentage
    print(f"盈亏比:         {results.get('profit_factor', 'N/A'):.2f}")
    # --- End print ---
    print("--------------------")

# Example Usage (for testing inside this file)
if __name__ == '__main__':
    example_trades = [
        {'datetime': datetime(2025, 4, 9, 21, 0, 13), 'symbol': 'SA505', 'direction': '多', 'offset': '开', 'price': 1312.0, 'volume': 1, 'commission': 5.20},
        {'datetime': datetime(2025, 4, 9, 21, 30, 0), 'symbol': 'SA505', 'direction': '空', 'offset': '平', 'price': 1320.0, 'volume': 1, 'commission': 5.20},
        {'datetime': datetime(2025, 4, 9, 22, 0, 5), 'symbol': 'rb2510', 'direction': '空', 'offset': '开', 'price': 3080.0, 'volume': 1, 'commission': 3.08},
        {'datetime': datetime(2025, 4, 9, 22, 15, 10), 'symbol': 'rb2510', 'direction': '多', 'offset': '平', 'price': 3070.0, 'volume': 1, 'commission': 3.07},
         {'datetime': datetime(2025, 4, 9, 22, 30, 0), 'symbol': 'SA505', 'direction': '多', 'offset': '开', 'price': 1315.0, 'volume': 2, 'commission': 10.40}, # Open more
         {'datetime': datetime(2025, 4, 9, 22, 45, 0), 'symbol': 'SA505', 'direction': '空', 'offset': '平', 'price': 1310.0, 'volume': 1, 'commission': 5.20}, # Close partial
         {'datetime': datetime(2025, 4, 9, 22, 50, 0), 'symbol': 'SA505', 'direction': '空', 'offset': '平', 'price': 1308.0, 'volume': 1, 'commission': 5.20}, # Close remaining
    ]
    example_multipliers = {'SA505': 20, 'rb2510': 10}
    performance = calculate_performance(example_trades, example_multipliers)
    print_performance_report(performance)

    print("\nTesting no trades:")
    print_performance_report(calculate_performance([], example_multipliers))