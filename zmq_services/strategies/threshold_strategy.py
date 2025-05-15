import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Dict

from vnpy.trader.constant import Offset, Status
from vnpy.trader.object import OrderData, TickData, TradeData

# Import base class and vnpy types
from zmq_services.strategy_base import BaseLiveStrategy


class ThresholdLiveStrategy(BaseLiveStrategy):
    """
    Simple threshold-based strategy.
    Inherits from BaseLiveStrategy for live trading via ZMQ/RPC.
    Uses Decimal for price/position calculations.
    """

    # --- Strategy Parameters (Types only - Values must be set via 'setting' dict) ---
    entry_threshold: Decimal
    profit_target_ticks: int
    stop_loss_ticks: int
    price_tick: Decimal
    order_volume: Decimal # Use Decimal for volume too
    order_price_offset_ticks: int

    # --- Strategy State Variables --- 
    long_entry_active: bool = False 
    close_pending: bool = False
    min_pos_threshold: Decimal # Will be initialized in __init__

    def __init__(
        self,
        strategy_engine: Any, # Use Any temporarily, should be StrategyEngine
        strategy_name: str,
        vt_symbol: str,      # Symbol determined by config
        setting: Dict[str, Any]
    ) -> None:
        """Constructor"""
        # Initialize base class first - this applies settings from the dict
        super().__init__(strategy_engine, strategy_name, vt_symbol, setting)

        # --- Verify required parameters are set (post-super().__init__) ---
        required_params = [
            'entry_threshold', 'profit_target_ticks', 'stop_loss_ticks', 
            'price_tick', 'order_volume',
            'order_price_offset_ticks'
        ]
        missing_params = []
        for param in required_params:
            if not hasattr(self, param):
                missing_params.append(param)
            else:
                 # Additionally check type (simple check for Decimal/int)
                 value = getattr(self, param)
                 expected_type = None
                 if param in ['entry_threshold', 'price_tick', 'order_volume']:
                      expected_type = Decimal
                 elif param in ['profit_target_ticks', 'stop_loss_ticks', 'order_price_offset_ticks']:
                      expected_type = int
                 
                 if expected_type and not isinstance(value, expected_type):
                      self.write_log(f"Parameter '{param}' has incorrect type: {type(value)}. Expected {expected_type}.", logging.ERROR)
                      # Add to missing params to trigger error, or try conversion?
                      # For simplicity, trigger error if type is wrong after base init.
                      missing_params.append(f"{param} (Wrong Type: {type(value)})" )


        if missing_params:
            error_msg = f"Strategy '{self.strategy_name}' cannot initialize. Missing or incorrect type for required parameters in 'setting' dict: {missing_params}"
            self.write_log(error_msg, logging.CRITICAL)
            # self.trading = False # Mark as non-trading immediately
            # self.inited = False # Mark as not inited
            raise ValueError(error_msg) # Raise error to stop engine loading this strategy
        # --- End Parameter Verification ---

        # Log the parameters that were successfully loaded from settings
        self.write_log(f"{self.__class__.__name__} ({self.strategy_name}) Parameters Loaded:") # Use class name
        for param in required_params:
             self.write_log(f"  {param}: {getattr(self, param)}")

        # Initialize state variables here (not in on_init anymore)
        self.long_entry_active = False
        self.close_pending = False
        self.min_pos_threshold: Decimal = Decimal("1e-8") # Threshold for comparing position near zero

    # --- Lifecycle Callbacks --- 
    def on_init(self) -> None:
        """Called once after __init__. Strategy parameters should be ready."""
        # Most initialization moved to __init__ after param verification
        self.write_log(f"{self.__class__.__name__} initialized (on_init called).") # Use class name

    def on_start(self) -> None:
        """Called when strategy starts trading."""
        self.write_log(f"{self.__class__.__name__} started.") # Use class name

    def on_stop(self) -> None:
        """Called when strategy stops trading."""
        self.write_log(f"{self.__class__.__name__} stopped.") # Use class name

    # --- Event Callbacks --- 
    def on_tick(self, tick: TickData) -> None:
        """Process new tick data using Decimal."""
        # +++ Add detailed DEBUG log at the beginning +++
        try:
            # Attempt to get last_price for logging early
            log_last_price = Decimal(str(getattr(tick, 'last_price', 'NaN')))
            # Log current state and relevant prices
            self.write_log(
                f"on_tick received: Time={tick.datetime.strftime('%H:%M:%S.%f')[:-3]} "
                f"vt={tick.vt_symbol} Last={log_last_price} Ask={getattr(tick, 'ask_price_1', 'N/A')} Bid={getattr(tick, 'bid_price_1', 'N/A')} | "
                f"State: Pos={self.pos} EntryActive={self.long_entry_active} ClosePending={self.close_pending} Trading={self.trading}",
                logging.DEBUG
            )
        except (InvalidOperation, TypeError):
             self.write_log(f"on_tick received for {tick.vt_symbol} at {tick.datetime.strftime('%H:%M:%S.%f')[:-3]}, but price conversion failed.", logging.DEBUG)
             return # Don't proceed if initial price check fails
        # +++ End Add +++

        if tick.vt_symbol != self.vt_symbol or not self.trading:
            return

        # Get prices as Decimal
        try:
            last_price = Decimal(str(getattr(tick, 'last_price', 'NaN'))) # Already tried above, but keep for main logic
            ask_price = Decimal(str(getattr(tick, 'ask_price_1', 'NaN')))
            bid_price = Decimal(str(getattr(tick, 'bid_price_1', 'NaN')))

            # +++ REMOVE TEMPORARY DEBUG LOG FOR ASK PRICE +++
            # if ask_price <= Decimal("1329.0"):
            #     self.write_log(f"@@@ POTENTIAL FILL TICK @@@ ask_price_1 ({ask_price}) <= 1329.0. Tick: {tick.__dict__}", logging.CRITICAL)
            # +++ END REMOVAL +++

            current_entry_price = self.get_entry_price() # 获取当前基类中的入场价
            log_msg_core = (
                f"on_tick: Time={tick.datetime.strftime('%H:%M:%S.%f')[:-3]} "
                f"Last={last_price} Ask={ask_price} Bid={bid_price} | "
                f"Pos={self.pos} EntryActive={self.long_entry_active} ClosePending={self.close_pending} | "
                f"EntryThresh={self.entry_threshold} BaseEntryPx={current_entry_price}"
            )
        except InvalidOperation:
            # self.write_log(f"Tick data price conversion error for {self.vt_symbol}. Skipping logic.", logging.DEBUG)
            return # Skip logic if prices are invalid

        # --- Entry Logic ----
        if self.pos.is_zero() and not self.long_entry_active and not self.close_pending:
            entry_condition_met = False
            if current_entry_price:
                if last_price > current_entry_price: # Price has moved above base, consider entry
                    # For LONG: entry if current price is HIGHER than threshold (e.g. breakout)
                    entry_condition_met = last_price > self.entry_threshold
            else: # No base_entry_px, direct threshold check
                entry_condition_met = last_price > self.entry_threshold

            if entry_condition_met:
                self.write_log(f"ENTRY TRIGGERED ({self.vt_symbol}, 突破): Last {last_price} vs Thresh {self.entry_threshold}", level=logging.INFO)
                if not self.trading:
                    self.write_log("Trading is False, cannot send entry order.", level=logging.WARNING)
                    return
                
                price = ask_price + Decimal(str(self.order_price_offset_ticks)) * self.price_tick
                self.write_log(f"计算开多仓限价: ask_price({ask_price}) + offset_ticks({self.order_price_offset_ticks}) * price_tick({self.price_tick}) = {price}", logging.DEBUG)
                self.write_log(f"Attempting to BUY {self.order_volume} at {price} or better. Current ask: {tick.ask_price_1}, bid: {tick.bid_price_1}", logging.INFO)
                vt_orderid = self.buy(price=float(price), volume=float(self.order_volume))
                self.long_entry_active = True # Set unconditionally to reflect intent

                # Log based on mode for clarity
                is_backtest_mode = getattr(self.strategy_engine, 'is_backtest_mode', False)
                if is_backtest_mode:
                    self.write_log("Set long_entry_active=True after sending BUY request (Mode: Backtest).", logging.DEBUG)
                elif vt_orderid: # Live mode
                    self.write_log("Set long_entry_active=True after sending BUY request (Mode: Live, vtOrderID received).", logging.DEBUG)
                else: # Live mode, but order send might have failed (no vt_orderid)
                    self.write_log("Set long_entry_active=True after sending BUY request (Mode: Live, vtOrderID NOT received - order send likely failed).", logging.DEBUG)

        # --- Exit Logic (Take Profit / Stop Loss) --- 
        elif self.pos > 0 and not self.close_pending:
            if current_entry_price is None:
                self.write_log("Position > 0 but base entry price is None. Cannot calculate exit targets.", logging.ERROR)
                return
            target_price = current_entry_price + Decimal(str(self.profit_target_ticks)) * self.price_tick
            stop_price = current_entry_price - Decimal(str(self.stop_loss_ticks)) * self.price_tick
            tp_condition_met = bid_price >= target_price
            sl_condition_met = bid_price <= stop_price
            self.write_log(f"  Exit Check: Pos>0={self.pos > 0} NotPending={not self.close_pending} -> Can Exit? {self.pos > 0 and not self.close_pending}", logging.DEBUG)
            self.write_log(f"  Exit Targets: TP={target_price}, SL={stop_price} (based on Entry={current_entry_price})", logging.DEBUG)
            self.write_log(f"  Exit Conditions: Bid({bid_price}) >= TP? {tp_condition_met} | Bid({bid_price}) <= SL? {sl_condition_met}", logging.DEBUG)

            if tp_condition_met or sl_condition_met: 
                 exit_reason = "止盈" if tp_condition_met else "止损"
                 self.write_log(f"EXIT TRIGGERED ({self.vt_symbol}, {exit_reason}): Bid {bid_price} vs {'TP' if exit_reason=='止盈' else 'SL'} {target_price if exit_reason=='止盈' else stop_price}", logging.INFO) # Add symbol/reason
                 self.write_log(f"触发 {self.strategy_name} {exit_reason} 条件: Bid {bid_price} ...") # Use strategy_name
                 sell_order_price = bid_price - Decimal(str(self.order_price_offset_ticks)) * self.price_tick
                 self.write_log(f"计算平多仓限价: bid_price({bid_price}) - offset_ticks({self.order_price_offset_ticks}) * price_tick({self.price_tick}) = {sell_order_price}", logging.DEBUG)
                 vt_orderid = self.sell(price=float(sell_order_price), volume=float(self.pos.copy_abs()))
                 # --- Set close pending flag IMMEDIATELY after sending request --- 
                 is_backtest_mode = getattr(self.strategy_engine, 'is_backtest_mode', False)
                 # Assume send attempt means we are now pending close, regardless of mode or return ID
                 # The actual state (pos=0) will be confirmed by on_trade
                 self.close_pending = True
                 self.write_log(f"Set close_pending=True after sending SELL request (Mode: {'Backtest' if is_backtest_mode else 'Live'}).", logging.DEBUG)
                 # --- End Set Flag --- 

    def on_order(self, order: OrderData) -> None:
        """Process order updates."""
        super().on_order(order) # Updates base class active_orders set

        if order.vt_symbol != self.vt_symbol:
            return

        # --- Update State based on Order Status --- 
        if not order.is_active(): # Process only terminal states (filled, cancelled, rejected, etc.)
            if order.offset == Offset.OPEN:
                # If we were intending to enter
                if self.long_entry_active:
                     if order.status not in [Status.ALLTRADED, Status.PARTTRADED]:
                         # --- State Transition: Open order failed/cancelled, reset entry intent --- 
                         self.long_entry_active = False
                         self.write_log(f"Open order {order.vt_orderid} final status {order.status.value}, resetting entry intent.") # Simplified log
                         # --- End State Transition ---
                     # else: Filled - let on_trade confirm state based on pos
            elif order.offset == Offset.CLOSE:
                # If we were intending to close
                if self.close_pending:
                    # --- State Transition: Reset close intent regardless of fill status --- 
                    # Whether filled or failed, we are no longer *pending* this specific close order.
                    self.close_pending = False
                    # --- End State Transition ---
                    if order.status not in [Status.ALLTRADED, Status.PARTTRADED]:
                        self.write_log(f"Close order {order.vt_orderid} final status {order.status.value}, resetting close pending flag. Current pos {self.pos}", logging.WARNING)
                    # else: Filled - let on_trade confirm state based on pos

    def on_trade(self, trade: TradeData) -> None:
        """Process trade/fill updates using Decimal."""
        # self.write_log(f"on_trade: Received TradeID={trade.vt_tradeid} Offset={trade.offset} Price={trade.price} Vol={trade.volume}", logging.DEBUG)
        super().on_trade(trade) # Updates self.pos and self.entry_price

        if trade.vt_symbol != self.vt_symbol:
            return

        # self.write_log(f"on_trade: After super().on_trade, self.pos = {self.pos}, self.entry_price = {self.entry_price}", logging.DEBUG)

        # --- Refine State based on Fills --- 
        if trade.offset == Offset.OPEN:
            if not self.pos.is_zero():
                 # --- State Transition: Open fill confirms we are in an active long entry --- 
                 self.long_entry_active = True # Confirmed in position
                 self.close_pending = False    # Cannot be pending close if just opened/added
                 # --- End State Transition ---
                 self.write_log(f"Open fill confirmed: VTOrderID={trade.vt_orderid}, Price={trade.price}, Vol={trade.volume}. New Pos: {self.pos}, AvgEntry: {self.entry_price}") # Simplified
            else:
                 self.write_log(f"收到 OPEN trade {trade.vt_tradeid} 但 base class 更新后 pos 仍为 0。检查 BaseLiveStrategy._update_pos 实现。", logging.ERROR)

        elif trade.offset == Offset.CLOSE:
            # --- State Transition: Close fill means we are no longer pending the close order --- 
            self.close_pending = False
            # --- End State Transition ---
            self.write_log(f"Close fill confirmed: VTOrderID={trade.vt_orderid}, Price={trade.price}, Vol={trade.volume}. New Pos: {self.pos}") # Simplified
            if self.pos.is_zero(): # Position is now flat
                # --- State Transition: Position fully closed, reset active entry flag --- 
                self.long_entry_active = False
                # --- End State Transition ---
                self.write_log(f"Position fully closed. Resetting state.") # Simplified
            # else: Partial close - long_entry_active remains True (still holding position)

    # --- Helper Methods ---
    def _calculate_and_set_targets(self, entry_price: float, context: str) -> None:
        """Calculates and updates profit/stop targets based on entry price."""
        if not all([hasattr(self, attr) for attr in [
            'profit_target_ticks', 'stop_loss_ticks', 'price_tick'
        ]]):
             self.write_log("Cannot calculate targets, parameter(s) missing.", logging.ERROR)
             return

        self.entry_price = entry_price
        self.target_price = entry_price + float(self.profit_target_ticks * self.price_tick)
        self.stop_price = entry_price - float(self.stop_loss_ticks * self.price_tick)
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