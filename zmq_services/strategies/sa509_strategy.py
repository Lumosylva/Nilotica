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

        # +++ Add missing attribute +++
        self.min_pos_threshold: float = 1e-8 # Threshold for float comparison of position
        # +++ End Add +++

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

        # Check if this order was initiated by this strategy (using the base class set)
        # Order might already be removed from active_orders by super().on_order if it just finished
        # So, checking if it *was* active might require looking at the order before super call,
        # or relying on the fact that only our orders should trigger significant state changes.
        # Let's simplify: assume if the order matches our symbol, we process its state change impact.
        # We rely on self.long_pending_or_open / self.close_pending flags for *intent*.
        is_my_order_intent = True # Assume relevant if symbol matches for state logic

        # --- Update State based on Order Status ---
        if not order.is_active(): # Process only terminal states
            if order.offset == Offset.OPEN:
                # If we were waiting for an open order to finish
                if self.long_pending_or_open:
                     if order.status in [Status.ALLTRADED, Status.PARTTRADED]:
                         # If filled (partially or fully), let on_trade handle the final state confirmation
                         self.write_log(f"SA509 开仓订单 {order.vt_orderid} 已成交 (状态: {order.status.value})。等待 on_trade 确认。")
                     else:
                         # If cancelled, rejected, etc., reset the pending flag
                         self.long_pending_or_open = False
                         self.write_log(f"SA509 开仓订单 {order.vt_orderid} 最终状态为 {order.status.value} (未成交), 重置开仓等待标志。")
                         self._reset_targets(f"Open Order {order.vt_orderid} Failed/Cancelled") # Also reset targets if open failed
                # else: # Order finished but we weren't pending? Log maybe.
                     # self.write_log(f"收到非预期的开仓订单结束状态: {order.vt_orderid} ({order.status.value})", logging.WARNING)

            elif order.offset == Offset.CLOSE:
                # If we were waiting for a close order to finish
                if self.close_pending:
                    self.close_pending = False # Reset pending flag regardless of fill status for close
                    if order.status in [Status.ALLTRADED, Status.PARTTRADED]:
                        # If filled, let on_trade handle the position confirmation and state reset
                        self.write_log(f"SA509 平仓订单 {order.vt_orderid} 已成交 (状态: {order.status.value})。等待 on_trade 确认。")
                    else:
                        # If close failed/cancelled, log it. Position likely still exists.
                        # Don't reset long_pending_or_open here, as position is likely still held.
                        self.write_log(f"SA509 平仓订单 {order.vt_orderid} 最终状态为 {order.status.value} (未成交), 重置平仓等待标志。当前持仓 {self.pos}", logging.WARNING)
                # else: # Order finished but we weren't pending? Log maybe.
                    # self.write_log(f"收到非预期的平仓订单结束状态: {order.vt_orderid} ({order.status.value})", logging.WARNING)

    def on_trade(self, trade: TradeData) -> None:
        """Process trade/fill updates."""
        # Call base class implementation first to update self.pos and active_traded_volume
        super().on_trade(trade)

        # Only process trades relevant to this strategy instance
        if trade.vt_symbol != self.vt_symbol:
            return

        # Check if this trade resulted from an order we were tracking as active
        # Note: Base class doesn't track originating order ID in trade inherently
        # We rely on the offset and position change logic here.
        # Assume trade for vt_symbol affects our state.

        # --- Refine State based on Fills --- 
        if trade.offset == Offset.OPEN:
            # Confirm position is open and refine entry price/targets
            if self.pos != 0: # Use self.pos updated by base class
                 # Regardless of previous pending state, an open fill means we are now in a position
                 self.long_pending_or_open = True
                 self.close_pending = False # Cannot be pending close if just opened/added
                 self.write_log(f"SA509 开仓成交确认: VTOrderID={trade.vt_orderid}, Price={trade.price}, Volume={trade.volume}. 新持仓: {self.pos}")
                 # Recalculate entry price and targets based on average fill price
                 # We need average price if handling partial fills; assume full fill for now
                 # TODO: Implement average price calculation for partial fills
                 avg_entry_price = trade.price # Use current trade price as approximation for now
                 self._calculate_and_set_targets(avg_entry_price, f"Open Trade {trade.vt_tradeid} Confirmed")
            else:
                 # This *shouldn't* happen if base class updates pos correctly
                 self.write_log(f"收到 OPEN trade {trade.vt_tradeid} 但 base class 更新后 pos 仍为 0。检查 BaseLiveStrategy.on_trade 实现。", logging.ERROR)

        elif trade.offset == Offset.CLOSE:
            self.write_log(f"SA509 平仓成交确认: VTOrderID={trade.vt_orderid}, Price={trade.price}, Volume={trade.volume}. 新持仓: {self.pos}")
            self.close_pending = False # No longer pending close after a fill

            # Position closed, reset flags only if position is now fully flat
            if abs(self.pos) < self.min_pos_threshold: # Use a small threshold for float comparison
                self.write_log(f"仓位已完全平掉 (持仓: {self.pos})。重置策略状态。")
                self.long_pending_or_open = False
                self._reset_targets(f"Close Trade {trade.vt_tradeid} Confirmed - Flat")
            else:
                 # Partial close fill
                 self.write_log(f"收到部分平仓成交，当前持仓 {self.pos}。保持开仓状态和目标。")
                 # Keep long_pending_or_open = True, keep targets.
                 # Stop loss/take profit logic in on_tick will continue based on remaining position.

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