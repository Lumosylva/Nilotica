# zmq_services/strategy_base.py
from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Dict, Set, Union
import logging
from decimal import Decimal, InvalidOperation
from utils.logger import setup_logging, logger
import pickle
import time
import zmq
import msgpack
from vnpy.trader.constant import Direction, Offset, Exchange, Status, OrderType

# Import necessary vnpy types
from vnpy.trader.object import TickData, OrderData, TradeData

# Type hint for the engine/interface, avoid circular import
if TYPE_CHECKING:
    # Assume we rename StrategySubscriber to StrategyEngine later
    # This import will likely cause issues until the rename happens or placed differently
    # For now, using Any to avoid immediate error, but should be fixed
    # from zmq_services.strategy_subscriber import StrategyEngine
    StrategyEngine = Any


class BaseLiveStrategy(metaclass=ABCMeta):
    """
    Abstract base class for live trading strategies operating in the ZMQ/RPC environment.
    Designed to be driven by tick data.
    """

    def __init__(
        self,
        strategy_engine: "StrategyEngine", # The engine instance providing trading functions
        strategy_name: str,              # Unique name for this strategy instance
        vt_symbol: str,                  # The primary symbol this strategy trades
        setting: Dict[str, Any]          # Strategy parameters
    ) -> None:
        """Constructor"""
        self.strategy_engine: "StrategyEngine" = strategy_engine
        self.strategy_name: str = strategy_name
        self.vt_symbol: str = vt_symbol
        self.setting: Dict[str, Any] = setting

        setup_logging(service_name=f"{__class__.__name__}.{self.strategy_name}", level=logging.INFO)

        # Strategy state variables
        self.inited: bool = False
        self.trading: bool = False
        self.pos: Decimal = Decimal("0.0")               # Current position for vt_symbol
        self.active_orders: Set[str] = set() # Active vt_orderids managed by this strategy

        # +++ Add entry price and cost tracking +++
        self.entry_price: Optional[Decimal] = None # Average entry price of the current position
        self.entry_cost: Decimal = Decimal("0.0")  # Total cost of the current position
        # +++ End Add +++

        # --- Apply settings to strategy attributes (Corrected Indentation & Logic) ---
        self.write_log("Applying strategy settings...", logging.DEBUG)
        for key, value in setting.items():
            try:
                target_type = None
                final_value_to_set = value # Start with the original value
                
                # Get expected type from class annotations
                annotations = getattr(self.__class__, '__annotations__', {})
                detected_annotation = annotations.get(key)
                self.write_log(f"[Setting Loop] Key: '{key}', Value: '{value}' (Type: {type(value)}), Detected Annotation: {detected_annotation}", logging.DEBUG)

                if detected_annotation:
                    target_type = detected_annotation
                    # Handle Optional[X] types
                    origin = getattr(target_type, '__origin__', None)
                    args = getattr(target_type, '__args__', None)
                    is_optional = getattr(origin, '_name__', None) == 'Optional' # Basic check for Optional
                    if is_optional and args:
                         # If value is None and type is Optional, allow it
                         if value is None:
                              target_type = None # Skip conversion, will set None
                         else:
                              target_type = args[0] # Get the inner type (e.g., Decimal from Optional[Decimal])
                    elif origin is Union and type(None) in args: # Handle Union[X, None]
                         if value is None:
                             target_type = None
                         else:
                             # Find the non-None type in Union[X, None]
                             non_none_args = [t for t in args if t is not type(None)]
                             if len(non_none_args) == 1:
                                 target_type = non_none_args[0]
                             # Else: Complex Union, handle as needed or default

                # Perform type conversion if target_type is known
                if target_type:
                    if target_type is Decimal:
                        if isinstance(value, (int, float, str)):
                            final_value_to_set = Decimal(str(value))
                        elif isinstance(value, Decimal):
                            final_value_to_set = value # Already correct type
                        else:
                            raise TypeError(f"Cannot convert setting '{key}' value type {type(value)} to Decimal.")
                    elif target_type is int:
                         if isinstance(value, str):
                             final_value_to_set = int(value)
                         elif isinstance(value, float):
                              self.write_log(f"[Setting Loop] Converting float setting '{key}' value {value} to int.", logging.DEBUG)
                              final_value_to_set = int(value)
                         elif isinstance(value, int):
                               final_value_to_set = value
                         else:
                             raise TypeError(f"Cannot convert setting '{key}' value type {type(value)} to int.")
                    elif target_type is float:
                         if isinstance(value, str):
                             final_value_to_set = float(value)
                         elif isinstance(value, int):
                             final_value_to_set = float(value)
                         elif isinstance(value, float):
                             final_value_to_set = value
                         else:
                             raise TypeError(f"Cannot convert setting '{key}' value type {type(value)} to float.")
                    elif target_type is bool:
                         if isinstance(value, str):
                              lower_val = value.lower()
                              if lower_val in ['true', '1', 'yes', 'y']:
                                   final_value_to_set = True
                              elif lower_val in ['false', '0', 'no', 'n']:
                                   final_value_to_set = False
                              else:
                                   raise ValueError(f"Cannot convert string setting '{key}' value '{value}' to bool.")
                         else:
                              final_value_to_set = bool(value)
                    # Add more type handlers if needed (e.g., for lists, dicts)
                    else:
                         # If target_type is known but not handled above, assign original value
                         # Or potentially raise an error if strict typing is desired
                         final_value_to_set = value 
                # else: target_type is None (no annotation or Optional/None allowed)
                # final_value_to_set remains the original value (or None if allowed)

                # Set the attribute
                setattr(self, key, final_value_to_set)
                # Debug log after setting
                actual_value = getattr(self, key)
                self.write_log(f"[Setting Loop] Successfully set '{key}' to {actual_value} (Type: {type(actual_value)})", logging.DEBUG)

            except (ValueError, TypeError, InvalidOperation) as e:
                 self.write_log(f"[Setting Loop] Failed to apply setting '{key}'='{value}'. Error: {e}", logging.WARNING)
        self.write_log("Finished applying strategy settings.", logging.DEBUG)
        # --- End Apply Settings ---

    #----------------------------------------------------------------------
    # Lifecycle Abstract Methods (Must be implemented by subclasses)
    #----------------------------------------------------------------------
    @abstractmethod
    def on_init(self) -> None:
        """Callback when strategy is initialized."""
        pass

    @abstractmethod
    def on_start(self) -> None:
        """Callback when strategy is started."""
        pass

    @abstractmethod
    def on_stop(self) -> None:
        """Callback when strategy is stopped."""
        pass

    #----------------------------------------------------------------------
    # Data/Event Abstract Methods (Must be implemented by subclasses)
    #----------------------------------------------------------------------
    @abstractmethod
    def on_tick(self, tick: TickData) -> None:
        """Callback of new tick data update."""
        # Note: The engine might pass tick as a dict, conversion might be needed here or in engine
        pass

    @abstractmethod
    def on_order(self, order: OrderData) -> None:
        """Callback of new order data update."""
        # Note: The engine might pass order as a dict
        # Base implementation to update active orders set
        self._update_active_orders(order)
        pass

    @abstractmethod
    def on_trade(self, trade: TradeData) -> None:
        """Callback of new trade data update."""
        # Note: The engine might pass trade as a dict
        # Base implementation to update position
        self._update_pos(trade)
        pass

    #----------------------------------------------------------------------
    # Standard Trading Methods (Provided by Base Class)
    #----------------------------------------------------------------------
    def buy(self, price: float, volume: float, lock: bool = False, **kwargs) -> Optional[str]:
        """Send buy open order."""
        return self.send_order(Direction.LONG, Offset.OPEN, price, volume, lock, **kwargs)

    def sell(self, price: float, volume: float, lock: bool = False, **kwargs) -> Optional[str]:
        """Send sell close order."""
        return self.send_order(Direction.SHORT, Offset.CLOSE, price, volume, lock, **kwargs)

    def short(self, price: float, volume: float, lock: bool = False, **kwargs) -> Optional[str]:
        """Send short open order."""
        return self.send_order(Direction.SHORT, Offset.OPEN, price, volume, lock, **kwargs)

    def cover(self, price: float, volume: float, lock: bool = False, **kwargs) -> Optional[str]:
        """Send buy close order."""
        return self.send_order(Direction.LONG, Offset.CLOSE, price, volume, lock, **kwargs)

    def send_order(
        self,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        lock: bool = False, # TODO: Implement position locking if needed
        **kwargs
    ) -> Optional[str]:
        """Send a limit order to the trading engine."""
        if not self.trading:
            # Log removed as it might be noisy if strategy stops mid-tick processing
            return None

        # Basic check for symbol matching before calling engine
        symbol_part = self.vt_symbol.split('.')[0]
        exchange_part = self.vt_symbol.split('.')[-1]
        try:
             exchange_enum = Exchange(exchange_part)
        except ValueError:
             logger.error(f"Invalid exchange '{exchange_part}' in vt_symbol '{self.vt_symbol}'. Cannot send order.")
             return None

        # --- Detect Backtest Mode using engine flag --- 
        is_backtest_mode = getattr(self.strategy_engine, 'is_backtest_mode', False)
        # --- End Detect Backtest Mode --- 

        # --- Conditional Order Sending --- 
        vt_orderid = None
        if is_backtest_mode:
            # --- Backtest: Send pickled tuple directly via PUSH --- 
            logger.debug("Backtest Mode: Preparing to send order tuple directly via PUSH.")
            order_req_dict = {
                "symbol": symbol_part,
                "exchange": exchange_enum.value,
                "direction": direction.value,
                "type": OrderType.LIMIT.value, 
                "volume": float(volume),
                "price": float(price),
                "offset": offset.value,
                "reference": f"{self.strategy_name}_backtest" # Indicate source
            }
            req_tuple = ("send_order", (order_req_dict,), {})
            try:
                # packed_request = pickle.dumps(req_tuple) # --- Replaced with msgpack ---
                packed_request = msgpack.packb(req_tuple, use_bin_type=True)
                # --- Use order_pusher socket ---
                self.strategy_engine.order_pusher.send(packed_request)
                # --- End Use order_pusher ---
                vt_orderid = f"clientsim_{self.strategy_name}_{int(time.time_ns())}" 
                logger.info(f"Backtest Mode: Sent order request tuple for {vt_orderid}")
            except pickle.PicklingError as e:
                 logger.error(f"Backtest Mode: Failed to pickle order request: {e}")
            except (msgpack.PackException, TypeError) as e_pack:
                 logger.error(f"Backtest Mode: Failed to msgpack order request: {e_pack}")
            except zmq.ZMQError as e:
                 logger.error(f"Backtest Mode: ZMQ error sending order request: {e}")
            # --- End Backtest Send ---
        else:
            # --- Live Trading: Use RPC (via engine's method) --- 
            logger.debug("Live Mode: Sending order via RPC.")
            # Existing RPC logic using the engine's method
            vt_orderid = self.strategy_engine.send_limit_order(
                symbol=symbol_part,
                exchange=exchange_enum,
                direction=direction,
                offset=offset,
                price=price,
                volume=volume
            )
            # --- End Live Trading Send --- 

        # --- Common Logic: Add to active orders if ID generated --- 
        if vt_orderid:
            self.active_orders.add(vt_orderid) # Add either client-sim or real ID
            logger.info(f"Sent Order: {self.vt_symbol} {direction.value} {offset.value} {volume} @ {price} -> {vt_orderid} (Mode: {'Backtest' if is_backtest_mode else 'Live'})")
        # Error logging is handled within each branch or by the engine's live method

        return vt_orderid

    def cancel_order(self, vt_orderid: str) -> None:
        """Cancel an active order."""
        if not self.trading:
            logger.warning(f"Strategy not trading, cannot cancel order {vt_orderid}.")
            return

        if vt_orderid not in self.active_orders:
            # Avoid logging warning if order might have become inactive just before cancel call
            # logger.warning(f"Order {vt_orderid} not in active orders list for strategy {self.strategy_name}.")
            pass # Simply don't send cancel if not tracked as active

        # Delegate cancellation to the engine
        # Assuming the engine has a method like cancel_limit_order
        if not hasattr(self.strategy_engine, 'cancel_limit_order'):
             logger.error("Strategy engine does not have 'cancel_limit_order' method.")
             return

        logger.info(f"Requesting Cancel for order {vt_orderid}")
        self.strategy_engine.cancel_limit_order(vt_orderid)
        # Note: Order remains in active_orders until confirmed cancelled/inactive via on_order


    def cancel_all(self) -> None:
        """Cancel all active orders managed by this strategy."""
        if not self.trading:
            return

        if not self.active_orders:
            return

        logger.info(f"Cancelling all ({len(self.active_orders)}) active orders for {self.strategy_name}...")
        # Iterate over a copy as cancel_order might modify the set indirectly via on_order
        for vt_orderid in list(self.active_orders):
            self.cancel_order(vt_orderid)

    #----------------------------------------------------------------------
    # Helper Methods (Can be used by subclasses)
    #----------------------------------------------------------------------
    def get_pos(self) -> Decimal:
        """Get current calculated position for the strategy's symbol."""
        return self.pos

    # +++ Add getter for entry price +++
    def get_entry_price(self) -> Optional[Decimal]:
        """Get the average entry price of the current position, if any."""
        return self.entry_price
    # +++ End Add +++

    def write_log(self, msg: str, level: int = logging.INFO) -> None:
        """Write a log message prefixed with strategy name."""
        # Note: The logger already includes the strategy name
        logger.log(level, f"[{self.strategy_name}] {msg}")

    def _update_pos(self, trade: TradeData) -> None:
        """Helper to update position, entry cost and average entry price based on trade data using Decimal."""
        if trade.vt_symbol != self.vt_symbol:
            return

        # --- Get Trade Volume (Decimal) ---
        trade_volume_float = getattr(trade, 'volume', 0.0)
        if not isinstance(trade_volume_float, (int, float)):
             try:
                 trade_volume_float = float(trade_volume_float)
             except (ValueError, TypeError):
                 self.write_log(f"_update_pos: Could not convert trade volume '{trade.volume}' to float. Ignoring trade {trade.vt_tradeid}.", logging.ERROR)
                 return
        try:
            trade_volume = Decimal(str(trade_volume_float))
        except (InvalidOperation, ValueError, TypeError) as e:
             self.write_log(f"_update_pos: Could not convert trade volume float '{trade_volume_float}' to Decimal: {e}. Ignoring trade {trade.vt_tradeid}.", logging.ERROR)
             return
        if trade_volume.is_zero():
            # self.write_log(f"_update_pos: Ignoring trade {trade.vt_tradeid} with zero volume.", logging.DEBUG) # Often noisy
            return
        # --- End Get Trade Volume ---

        # --- Get Trade Price (Decimal) ---
        trade_price_float = getattr(trade, 'price', None)
        if trade_price_float is None:
            self.write_log(f"_update_pos: Trade {trade.vt_tradeid} has no price. Ignoring entry price update.", logging.WARNING)
            trade_price = None # Indicate price is unavailable
        else:
            try:
                trade_price = Decimal(str(trade_price_float))
            except (InvalidOperation, ValueError, TypeError) as e:
                 self.write_log(f"_update_pos: Could not convert trade price float '{trade_price_float}' to Decimal: {e}. Ignoring entry price update for trade {trade.vt_tradeid}.", logging.ERROR)
                 trade_price = None # Indicate price is unavailable/invalid
        # --- End Get Trade Price ---

        previous_pos = self.pos
        previous_entry_cost = self.entry_cost # Store previous cost
        previous_entry_price = self.entry_price # Store previous entry price
        pos_change = Decimal("0.0")

        # self.write_log(f"_update_pos: Processing Trade={trade.vt_tradeid}, Direction={repr(trade.direction)}, Offset={repr(trade.offset)}, Volume={trade_volume}, Price={trade_price}", logging.DEBUG)

        direction_value = getattr(trade.direction, 'value', None)
        offset_value = getattr(trade.offset, 'value', None)

        # Calculate position change based on direction
        if direction_value == Direction.LONG.value: # Buy
            pos_change = trade_volume
        elif direction_value == Direction.SHORT.value: # Sell
            pos_change = -trade_volume
        else:
            self.write_log(f"_update_pos: Unknown or invalid direction value: {direction_value} for trade {trade.vt_tradeid}", logging.ERROR)
            return

        # --- Apply position change --- 
        try:
            self.pos += pos_change
            current_pos = self.pos
        except Exception as e:
            self.write_log(f"_update_pos: Error during Decimal position update arithmetic: {e}. Pos: {previous_pos}, Change: {pos_change}", logging.ERROR)
            return

        # --- Update Entry Cost and Price (Only if trade_price is valid) --- 
        if trade_price is not None:
            try:
                cost_of_this_trade = trade_price * trade_volume.copy_abs()

                if previous_pos.is_zero() and not current_pos.is_zero():
                    # Case 1: Flat to Non-Flat (Opening first leg)
                    self.entry_cost = cost_of_this_trade
                    self.entry_price = trade_price
                    self.write_log(f"_update_pos: Position opened. EntryCost={self.entry_cost:.4f}, EntryPrice={self.entry_price:.4f}", logging.DEBUG)

                elif not previous_pos.is_zero() and not current_pos.is_zero() and (previous_pos * current_pos > 0):
                    # Case 2 & 3: Adding or Partially Closing (Same Direction)
                    if abs(current_pos) > abs(previous_pos):
                        # Case 2: Adding to position
                        self.entry_cost = previous_entry_cost + cost_of_this_trade
                        if not current_pos.is_zero(): # Should not be zero here, but check anyway
                            self.entry_price = self.entry_cost / current_pos.copy_abs()
                            self.write_log(f"_update_pos: Position increased. New EntryCost={self.entry_cost:.4f}, New EntryPrice={self.entry_price:.4f}", logging.DEBUG)
                        else: # Safety fallback
                             self.write_log(f"_update_pos: Position zero unexpectedly after adding. Resetting entry cost/price.", logging.WARNING)
                             self.entry_cost = Decimal("0.0")
                             self.entry_price = None

                    elif abs(current_pos) < abs(previous_pos):
                         # Case 3: Partially Closing
                         if previous_entry_price is not None:
                             # Reduce cost proportionally based on the previous average price
                             cost_reduction = previous_entry_price * trade_volume.copy_abs()
                             self.entry_cost = previous_entry_cost - cost_reduction
                             # Entry price remains unchanged
                             self.entry_price = previous_entry_price
                             self.write_log(f"_update_pos: Position partially closed. New EntryCost={self.entry_cost:.4f}, EntryPrice remains {self.entry_price:.4f}", logging.DEBUG)
                         else:
                             # Should not happen if pos > 0, but handle defensively
                             self.write_log(f"_update_pos: Cannot reduce cost - previous entry price is missing! Cost remains {previous_entry_cost:.4f}, Price remains None.", logging.WARNING)
                             self.entry_cost = previous_entry_cost
                             self.entry_price = None
                    # else: Volume unchanged? Should not happen with non-zero trade volume

                elif not previous_pos.is_zero() and current_pos.is_zero():
                    # Case 4: Non-Flat to Flat (Closing last leg)
                    self.entry_cost = Decimal("0.0")
                    self.entry_price = None
                    self.write_log(f"_update_pos: Position closed. EntryCost reset to 0, EntryPrice reset to None.", logging.DEBUG)

                # --- Replace .sign() check with multiplication check ---
                # elif previous_pos.sign() != current_pos.sign():
                # 当它们的符号相反时，乘积必然为负。
                elif previous_pos * current_pos < 0:
                # --- End Replace ---
                    # Case 5: Flipping Position
                    # New position's cost is based solely on the flip trade
                    # --- Corrected cost calculation for flips ---
                    # Cost should reflect the value of the new position at the flip price
                    self.entry_cost = trade_price * current_pos.copy_abs() 
                    # --- End Correction ---
                    self.entry_price = trade_price # New position entry price is the flip trade price
                    self.write_log(f"_update_pos: Position flipped. New EntryCost={self.entry_cost:.4f}, New EntryPrice={self.entry_price:.4f}", logging.DEBUG)

                # --- Sanity Check/Rounding (Optional but recommended) ---
                # If position is very close to zero, reset cost/price
                if current_pos.is_zero(): # Use is_zero() for robustness
                    self.entry_cost = Decimal("0.0")
                    self.entry_price = None
                # Optionally round entry price to a reasonable number of decimal places
                # if self.entry_price is not None:
                #     self.entry_price = self.entry_price.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP) # Adjust precision as needed

            except Exception as e_entry_price:
                self.write_log(f"_update_pos: Error calculating entry cost/price: {e_entry_price}", logging.ERROR)
                # Reset on error for safety?
                self.entry_cost = Decimal("0.0")
                self.entry_price = None
        # --- End Update Entry Cost and Price --- 

        # --- Log the position update --- 
        log_entry_price = f" EntryPrice={self.entry_price:.4f}" if self.entry_price is not None else " EntryPrice=None"
        log_entry_cost = f" EntryCost={self.entry_cost:.4f}" # Always log cost
        self.write_log(
            f"Position Update: Symbol={trade.vt_symbol}, TradeID={trade.vt_tradeid}, "
            f"DirectionValue={direction_value}, OffsetValue={offset_value}, Price={trade.price}, Volume={trade_volume}. "
            f"PrevPos={previous_pos}, Change={pos_change}, NewPos={current_pos}.{log_entry_cost}{log_entry_price}",
            logging.INFO
        )

    def _update_active_orders(self, order: OrderData) -> None:
        """Helper to update the set of active orders. Should be called in on_order."""
        # Ensure the order belongs to this strategy's symbol
        if order.vt_symbol == self.vt_symbol:
            is_active_order = order.vt_orderid in self.active_orders
            if not order.is_active() and is_active_order:
                self.active_orders.remove(order.vt_orderid)
                self.write_log(f"Order {order.vt_orderid} removed from active list (Status: {order.status}). Active count: {len(self.active_orders)}", logging.DEBUG)
            elif order.is_active() and not is_active_order and order.status not in [Status.CANCELLED, Status.REJECTED]:
                 # If an order becomes active unexpectedly (e.g., engine resends state), add it back if needed
                 # self.active_orders.add(order.vt_orderid) # Be cautious adding orders not sent by strategy
                 # self.write_log(f"Received update for active order {order.vt_orderid} not initially tracked by strategy.", logging.WARNING)
                 self.write_log(f"Received update for active order {order.vt_orderid} not initially tracked by strategy.", logging.DEBUG)
        # If order belongs to another symbol, ignore for active order tracking of this instance.

    #----------------------------------------------------------------------
    # Internal Lifecycle Management (Called by the Engine)
    #----------------------------------------------------------------------
    def _init_strategy(self) -> None:
        """Internal initialization. Called by the engine."""
        self.inited = True
        try:
            self.on_init()
            self.write_log("策略初始化完成")
        except Exception as e:
             logger.exception(f"Error during on_init: {e}")
             self.inited = False # Mark as failed initialization


    def _start_strategy(self) -> None:
        """Internal start. Called by the engine."""
        if not self.inited:
             logger.error("Strategy cannot be started because initialization failed or was skipped.")
             return
        if self.trading:
             logger.warning("Strategy already started.")
             return
        self.trading = True
        try:
            self.on_start()
            self.write_log("策略启动")
        except Exception as e:
            logger.exception(f"Error during on_start: {e}")
            self.trading = False # Mark as failed start

    def _stop_strategy(self) -> None:
        """Internal stop. Called by the engine."""
        if not self.trading and not self.inited: # Allow stopping even if start failed but was init'd
             return # Avoid multiple stops

        was_trading = self.trading
        self.trading = False # Set trading to False first

        if was_trading:
            try:
                self.cancel_all() # Ensure all orders are cancelled on stop
                self.on_stop()
                self.write_log("策略停止")
            except Exception as e:
                logger.exception(f"Error during on_stop or cancel_all: {e}")
        else:
             self.write_log("策略停止 (未曾启动或启动失败)")