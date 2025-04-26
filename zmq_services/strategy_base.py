# zmq_services/strategy_base.py

from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING, Any, Optional, Dict, Set
import logging

# Import necessary vnpy types
from vnpy.trader.object import TickData, OrderData, TradeData
from vnpy.trader.constant import Direction, Offset, Exchange

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

        self.logger = logging.getLogger(f"strategy.{self.strategy_name}") # Per-strategy logger

        # Strategy state variables
        self.inited: bool = False
        self.trading: bool = False
        self.pos: float = 0.0               # Current position for vt_symbol
        self.active_orders: Set[str] = set() # Active vt_orderids managed by this strategy

        # Apply settings to strategy attributes if they exist
        for key, value in setting.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                self.logger.warning(f"Setting '{key}' not found as an attribute in strategy '{self.strategy_name}'.")

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
            self.logger.warning(f"Strategy not trading, cannot send order for {self.vt_symbol}.")
            return None

        # Basic check for symbol matching before calling engine
        symbol_part = self.vt_symbol.split('.')[0]
        exchange_part = self.vt_symbol.split('.')[-1]
        try:
             exchange_enum = Exchange(exchange_part)
        except ValueError:
             self.logger.error(f"Invalid exchange '{exchange_part}' in vt_symbol '{self.vt_symbol}'. Cannot send order.")
             return None

        # Delegate the actual sending to the strategy engine's trading interface
        # Assuming the engine has a method like send_limit_order
        if not hasattr(self.strategy_engine, 'send_limit_order'):
             self.logger.error("Strategy engine does not have 'send_limit_order' method.")
             return None

        vt_orderid = self.strategy_engine.send_limit_order(
            symbol=symbol_part,
            exchange=exchange_enum,
            direction=direction,
            offset=offset,
            price=price,
            volume=volume
        )

        if vt_orderid:
            self.active_orders.add(vt_orderid)
            self.logger.info(f"Sent Order: {self.vt_symbol} {direction.value} {offset.value} {volume} @ {price} -> {vt_orderid}")
        # Error logging is handled by the engine's send_limit_order method

        return vt_orderid

    def cancel_order(self, vt_orderid: str) -> None:
        """Cancel an active order."""
        if not self.trading:
            self.logger.warning(f"Strategy not trading, cannot cancel order {vt_orderid}.")
            return

        if vt_orderid not in self.active_orders:
            # Avoid logging warning if order might have become inactive just before cancel call
            # self.logger.warning(f"Order {vt_orderid} not in active orders list for strategy {self.strategy_name}.")
            pass # Simply don't send cancel if not tracked as active

        # Delegate cancellation to the engine
        # Assuming the engine has a method like cancel_limit_order
        if not hasattr(self.strategy_engine, 'cancel_limit_order'):
             self.logger.error("Strategy engine does not have 'cancel_limit_order' method.")
             return

        self.logger.info(f"Requesting Cancel for order {vt_orderid}")
        self.strategy_engine.cancel_limit_order(vt_orderid)
        # Note: Order remains in active_orders until confirmed cancelled/inactive via on_order


    def cancel_all(self) -> None:
        """Cancel all active orders managed by this strategy."""
        if not self.trading:
            return

        if not self.active_orders:
            return

        self.logger.info(f"Cancelling all ({len(self.active_orders)}) active orders for {self.strategy_name}...")
        # Iterate over a copy as cancel_order might modify the set indirectly via on_order
        for vt_orderid in list(self.active_orders):
            self.cancel_order(vt_orderid)

    #----------------------------------------------------------------------
    # Helper Methods (Can be used by subclasses)
    #----------------------------------------------------------------------
    def get_pos(self) -> float:
        """Get current calculated position for the strategy's symbol."""
        return self.pos

    def write_log(self, msg: str, level: int = logging.INFO) -> None:
        """Write a log message prefixed with strategy name."""
        # Note: The logger already includes the strategy name
        self.logger.log(level, msg)

    def _update_pos(self, trade: TradeData) -> None:
        """Helper to update position based on trade data. Should be called in on_trade."""
        # Ensure the trade belongs to this strategy's symbol
        if trade.vt_symbol == self.vt_symbol:
            if trade.direction == Direction.LONG:
                self.pos += trade.volume
            else:
                self.pos -= trade.volume
            self.write_log(f"Position updated by trade {trade.vt_tradeid}: {self.pos}", logging.DEBUG)
        else:
             self.write_log(f"Received trade for different symbol {trade.vt_symbol}, position not updated.", logging.WARNING)


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
                 self.write_log(f"Received update for active order {order.vt_orderid} not initially tracked by strategy.", logging.WARNING)
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
             self.logger.exception(f"Error during on_init: {e}")
             self.inited = False # Mark as failed initialization


    def _start_strategy(self) -> None:
        """Internal start. Called by the engine."""
        if not self.inited:
             self.logger.error("Strategy cannot be started because initialization failed or was skipped.")
             return
        if self.trading:
             self.logger.warning("Strategy already started.")
             return
        self.trading = True
        try:
            self.on_start()
            self.write_log("策略启动")
        except Exception as e:
            self.logger.exception(f"Error during on_start: {e}")
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
                self.logger.exception(f"Error during on_stop or cancel_all: {e}")
        else:
             self.write_log("策略停止 (未曾启动或启动失败)") 