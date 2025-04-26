# zmq_services/strategies/sa509_strategy.py

import logging
from typing import Any, Dict, Optional

# Import base class and vnpy types
from zmq_services.strategy_base import BaseLiveStrategy
from vnpy.trader.object import TickData, OrderData, TradeData
from vnpy.trader.constant import Direction, Offset, Exchange, Status

# Import config for parameters (consider passing them via setting dict instead)
# Ideally, these should come from the 'setting' dict passed to __init__
# For now, we still access global config as a fallback or default
try:
    from config import zmq_config as config
except ImportError:
    # Provide dummy values if config is not found, log error
    logging.getLogger(__name__).error("Could not import zmq_config. Using default placeholder values for SA509 parameters.")
    class ConfigPlaceholder:
        SA509_ENTRY_THRESHOLD = 3000
        SA509_PROFIT_TARGET_TICKS = 20
        SA509_STOP_LOSS_TICKS = 10
        SA509_PRICE_TICK = 1.0
    config = ConfigPlaceholder()


class SA509LiveStrategy(BaseLiveStrategy):
    """
    Simple trend-following strategy for SA509 based on crossing a threshold.
    Inherits from BaseLiveStrategy for live trading via ZMQ/RPC.
    """

    # --- Strategy Parameters (Defaults can be overridden by 'setting' dict) ---
    entry_threshold: float = config.SA509_ENTRY_THRESHOLD
    profit_target_ticks: int = config.SA509_PROFIT_TARGET_TICKS
    stop_loss_ticks: int = config.SA509_STOP_LOSS_TICKS
    price_tick: float = config.SA509_PRICE_TICK
    order_volume: float = 1.0 # Default order volume

    # --- Strategy State Variables ---
    long_pending_or_open: bool = False
    close_pending: bool = False
    entry_price: float = 0.0
    target_price: float = 0.0
    stop_price: float = 0.0

    def __init__(
        self,
        strategy_engine: Any, # Use Any temporarily, should be StrategyEngine
        strategy_name: str,
        vt_symbol: str,      # Should be "SA509.CZCE"
        setting: Dict[str, Any]
    ) -> None:
        """Constructor"""
        # Initialize base class first - this applies settings from the dict
        super().__init__(strategy_engine, strategy_name, vt_symbol, setting)

        # Verify vt_symbol matches expected after base init
        if self.vt_symbol != "SA509.CZCE":
            self.logger.error(f"SA509LiveStrategy initialized with incorrect vt_symbol: {self.vt_symbol}. Expected SA509.CZCE.")
            # Consider raising an error or marking strategy as non-tradable
            # For now, just log the error.

        # Log the parameters *after* they have been potentially overridden by settings
        self.logger.info("SA509 Strategy Parameters Used:")
        self.logger.info(f"  vt_symbol: {self.vt_symbol}") # Log symbol too
        self.logger.info(f"  entry_threshold: {getattr(self, 'entry_threshold', 'N/A')}")
        self.logger.info(f"  profit_target_ticks: {getattr(self, 'profit_target_ticks', 'N/A')}")
        self.logger.info(f"  stop_loss_ticks: {getattr(self, 'stop_loss_ticks', 'N/A')}")
        self.logger.info(f"  price_tick: {getattr(self, 'price_tick', 'N/A')}")
        self.logger.info(f"  order_volume: {getattr(self, 'order_volume', 'N/A')}")


    # --- Lifecycle Callbacks ---
    def on_init(self) -> None:
        """Initialize strategy state."""
        self.long_pending_or_open = False
        self.close_pending = False
        self.entry_price = 0.0
        self.target_price = 0.0
        self.stop_price = 0.0
        # Reset position from base class in case engine provides initial pos later
        self.pos = 0.0
        self.write_log("SA509 strategy state initialized.")

    def on_start(self) -> None:
        """Called when strategy starts trading."""
        self.write_log("SA509 strategy started.")
        # Potentially load initial position state if resuming? (More advanced)

    def on_stop(self) -> None:
        """Called when strategy stops trading."""
        self.write_log("SA509 strategy stopped.")
        # Persist state if needed? (More advanced)

    # --- Event Callbacks ---
    def on_tick(self, tick: TickData) -> None:
        """Process new tick data."""
        # Ensure tick is for the correct symbol (engine should handle dispatch, but useful check)
        if tick.vt_symbol != self.vt_symbol:
            return

        # Ensure strategy is trading
        if not self.trading:
            return

        last_price = getattr(tick, 'last_price', None)
        ask_price = getattr(tick, 'ask_price_1', None)
        bid_price = getattr(tick, 'bid_price_1', None)

        # Validate parameters exist before using them
        if not all([hasattr(self, attr) for attr in [
            'entry_threshold', 'profit_target_ticks', 'stop_loss_ticks', 'price_tick', 'order_volume'
        ]]):
             self.write_log("Strategy parameters not fully loaded, cannot process tick.", logging.ERROR)
             self.trading = False # Stop trading if params missing
             return

        if not all([last_price, ask_price, bid_price]):
            # self.write_log(f"Tick data incomplete for {self.vt_symbol}. Skipping logic.", logging.DEBUG) # Reduce log noise
            return

        # --- Entry Logic ----
        # Can only enter if current position is flat (pos == 0), not pending open/close
        # Note: self.pos is updated by base class on_trade
        if self.pos == 0 and not self.long_pending_or_open and not self.close_pending:
            entry_condition_met = last_price > self.entry_threshold
            if entry_condition_met:
                self.write_log(f"触发 SA509 开多仓条件: Price {last_price} > {self.entry_threshold}")
                # Send buy order (use ask price for limit order)
                vt_orderid = self.buy(
                    price=ask_price, # Use ask price to likely get filled
                    volume=self.order_volume
                )
                if vt_orderid:
                    # State is now "pending open" until fill/cancel
                    self.long_pending_or_open = True
                    # Tentatively set targets based on order price, refine on fill
                    self._calculate_and_set_targets(ask_price, "Order Sent")
                # else: # Base class send_order handles logging failure
                    # self.write_log("发送 SA509 开多仓订单失败。", logging.ERROR)

        # --- Exit Logic (Take Profit / Stop Loss) ---
        # Can only exit if holding a long position (pos > 0) and not already pending close
        elif self.pos > 0 and not self.close_pending:
            # Check Take Profit (use bid price for selling)
            if self.target_price > 0 and bid_price >= self.target_price:
                self.write_log(f"触发 SA509 止盈条件: Bid {bid_price} >= Target {self.target_price:.2f}")
                vt_orderid = self.sell(
                    price=bid_price, # Use bid price for selling
                    volume=abs(self.pos) # Close the entire position
                )
                if vt_orderid:
                    self.close_pending = True # Waiting for close order confirmation/fill
                # else: # Base class send_order handles logging failure
                    # self.write_log("发送 SA509 止盈平仓订单失败。", logging.ERROR)

            # Check Stop Loss (use bid price for selling)
            elif self.stop_price > 0 and bid_price <= self.stop_price:
                self.write_log(f"触发 SA509 止损条件: Bid {bid_price} <= Stop {self.stop_price:.2f}")
                vt_orderid = self.sell(
                    price=bid_price, # Use bid price for selling
                    volume=abs(self.pos) # Close the entire position
                )
                if vt_orderid:
                    self.close_pending = True # Waiting for close order confirmation/fill
                # else: # Base class send_order handles logging failure
                    # self.write_log("发送 SA509 止损平仓订单失败。", logging.ERROR)

    def on_order(self, order: OrderData) -> None:
        """Process order updates."""
        # Call base class implementation first to update active_orders
        super().on_order(order)

        # Only process orders relevant to this strategy instance
        if order.vt_symbol != self.vt_symbol:
            return

        # If an open order is no longer active (filled, cancelled, rejected)
        if order.offset == Offset.OPEN and not order.is_active():
            # Check if the strategy thought it was pending open based on this order ID
            # Note: Order ID is removed from self.active_orders by super().on_order IF it was there
            if self.long_pending_or_open: # If we were waiting for an open fill
                if order.status not in [Status.ALLTRADED, Status.PARTTRADED]:
                    self.long_pending_or_open = False # Reset flag only if NOT filled
                    self.write_log(f"SA509 开仓订单 {order.vt_orderid} 未成交 ({order.status.value}), 重置开仓标志。")
            # If filled, the flag remains True, and on_trade confirms the position

        # If a close order is no longer active
        elif order.offset == Offset.CLOSE and not order.is_active():
             # Check if the strategy thought it was pending close
             if self.close_pending: # If we were waiting for a close fill
                self.close_pending = False # Reset flag regardless of fill status for close orders
                if order.status not in [Status.ALLTRADED, Status.PARTTRADED]:
                     self.write_log(f"SA509 平仓订单 {order.vt_orderid} 未成交 ({order.status.value}), 重置平仓等待标志。持仓可能仍存在。")
                # If filled, flag is reset, and on_trade confirms the flat position


    def on_trade(self, trade: TradeData) -> None:
        """Process trade/fill updates."""
        # Call base class implementation first to update self.pos
        super().on_trade(trade)

        # Only process trades relevant to this strategy instance
        if trade.vt_symbol != self.vt_symbol:
            return

        # Refine state based on fills
        if trade.offset == Offset.OPEN:
            # Confirm position is open and refine entry price/targets
            if self.pos > 0: # Check position based on base class update
                 self.long_pending_or_open = True # Position is definitely open now
                 self.close_pending = False # Should not be pending close if just opened
                 # Refine entry price using actual trade price
                 # TODO: Handle partial fills correctly (use average price?)
                 self._calculate_and_set_targets(trade.price, "Trade Filled")
            else:
                 # This case might happen with complex order handling or delayed updates
                 self.write_log(f"Received OPEN trade {trade.vt_tradeid} but position is {self.pos}. State might be inconsistent.", logging.WARNING)

        elif trade.offset == Offset.CLOSE:
            # Position closed, reset all flags if position is now flat
            if self.pos == 0: # Check position based on base class update
                self.long_pending_or_open = False
                self.close_pending = False
                self._reset_targets("Trade Closed")
            else:
                 # This case might happen with partial close fills
                 self.write_log(f"Received CLOSE trade {trade.vt_tradeid} but position is {self.pos}. Not fully closed yet.", logging.INFO)
                 # Keep flags as they are, wait for next trade or manage partial close

    # --- Helper Methods ---
    def _calculate_and_set_targets(self, entry_price: float, context: str) -> None:
        """Calculates and updates profit/stop targets based on entry price."""
        if not all([hasattr(self, attr) for attr in [
            'profit_target_ticks', 'stop_loss_ticks', 'price_tick'
        ]]):
             self.write_log("Cannot calculate targets, parameter(s) missing.", logging.ERROR)
             return

        self.entry_price = entry_price
        self.target_price = entry_price + self.profit_target_ticks * self.price_tick
        self.stop_price = entry_price - self.stop_loss_ticks * self.price_tick
        self.write_log(
            f"({context}) 更新入场价: {self.entry_price:.2f}, 新目标: "
            f"Target={self.target_price:.2f}, Stop={self.stop_price:.2f}"
        )

    def _reset_targets(self, context: str) -> None:
        """Resets entry price and targets."""
        self.entry_price = 0.0
        self.target_price = 0.0
        self.stop_price = 0.0
        self.write_log(f"({context}) 重置入场价和目标。") 