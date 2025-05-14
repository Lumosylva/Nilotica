#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : strategy_base.py
@Date       : 2025/5/9 12:09
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 在 ZMQ/RPC 环境中运行的实时交易策略的抽象基类，设计为由报价数据驱动。
Abstract base class for live trading strategies operating in the ZMQ/RPC environment.
Designed to be driven by tick data.
"""
from abc import ABCMeta, abstractmethod
from typing import Any, Optional, Dict, Set, Union
import logging
from decimal import Decimal, InvalidOperation
from utils.logger import setup_logging, logger
import pickle
import zmq
import msgpack
from vnpy.trader.constant import Direction, Offset, Exchange, Status, OrderType

from vnpy.trader.object import TickData, OrderData, TradeData
from zmq_services.strategy_engine import StrategyEngine


class BaseLiveStrategy(metaclass=ABCMeta):
    """
    在 ZMQ/RPC 环境中运行的实时交易策略的抽象基类。
    设计为由报价数据驱动。

    Abstract base class for live trading strategies operating in the ZMQ/RPC environment.
    Designed to be driven by tick data.
    """

    def __init__(
        self,
        strategy_engine: StrategyEngine, # 提供交易功能的引擎实例(The engine instance providing trading functions)
        strategy_name: str,              # 此策略实例的唯一名称(Unique name for this strategy instance)
        vt_symbol: str,                  # 该策略交易的主要合约(The primary symbol this strategy trades)
        setting: Dict[str, Any]          # 策略参数(Strategy parameters)
    ) -> None:
        """Constructor"""
        self.strategy_engine: StrategyEngine = strategy_engine
        self.strategy_name: str = strategy_name
        self.vt_symbol: str = vt_symbol
        self.setting: Dict[str, Any] = setting

        setup_logging(service_name=f"{self.__class__.__name__}.{self.strategy_name}", level=logging.INFO)

        # 策略状态变量(Strategy state variables)
        self.inited: bool = False
        self.trading: bool = False
        self.pos: Decimal = Decimal("0.0")               # vt_symbol 的当前仓位(Current position for vt_symbol)
        self.active_orders: Set[str] = set() # 此策略管理的活动 vt_orderids(Active vt_orderids managed by this strategy)

        # +++ 添加入场价格和成本跟踪(Add entry price and cost tracking) +++
        self.entry_price: Optional[Decimal] = None # 当前仓位的平均入场价(Average entry price of the current position)
        self.entry_cost: Decimal = Decimal("0.0")  # 当前仓位总成本(Total cost of the current position)

        self.write_log("Applying strategy settings...", logging.DEBUG)
        for key, value in setting.items():
            try:
                target_type = None
                final_value_to_set = value # 从原始值开始(Start with the original value)
                
                # 从类注释中获取预期类型(Get expected type from class annotations)
                annotations = getattr(self.__class__, '__annotations__', {})
                detected_annotation = annotations.get(key)
                self.write_log(f"[Setting Loop] Key: '{key}', Value: '{value}' (Type: {type(value)}), Detected Annotation: {detected_annotation}", logging.DEBUG)

                if detected_annotation:
                    target_type = detected_annotation
                    # 处理 Optional[X] 类型(Handle Optional[X] types)
                    origin = getattr(target_type, '__origin__', None)
                    args = getattr(target_type, '__args__', None)
                    is_optional = getattr(origin, '_name__', None) == 'Optional' # Basic check for Optional
                    if is_optional and args:
                         # 如果值为 None 且类型为 Optional，则允许(If value is None and type is Optional, allow it)
                         if value is None:
                              target_type = None # 跳过转换，将设置 None(Skip conversion, will set None)
                         else:
                              target_type = args[0] # Get the inner type (e.g., Decimal from Optional[Decimal])
                    elif origin is Union and type(None) in args: # Handle Union[X, None]
                         if value is None:
                             target_type = None
                         else:
                             # 在 Union[X, None] 中查找非 None 类型
                             # Find the non-None type in Union[X, None]
                             non_none_args = [t for t in args if t is not type(None)]
                             if len(non_none_args) == 1:
                                 target_type = non_none_args[0]
                             # 否则：复杂联合，根据需要处理或默认
                             # Else: Complex Union, handle as needed or default

                # 如果 target_type 已知，则执行类型转换(Perform type conversion if target_type is known)
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
                    # 如果需要，添加更多类型处理程序（例如，列表、字典）[Add more type handlers if needed (e.g., for lists, dicts)]
                    else:
                         # 如果 target_type 已知但未在上文处理，则赋原始值
                         # 如果需要严格类型，则可能引发错误
                         # If target_type is known but not handled above, assign original value
                         # Or potentially raise an error if strict typing is desired
                         final_value_to_set = value
                # 否则：target_type 为 None（无注释或允许 Optional/None）
                # final_value_to_set 保留原始值（如果允许，则为 None）
                # else: target_type is None (no annotation or Optional/None allowed)
                # final_value_to_set remains the original value (or None if allowed)

                # 设置属性(Set the attribute)
                setattr(self, key, final_value_to_set)
                # 设置后的调试日志(Debug log after setting)
                actual_value = getattr(self, key)
                self.write_log(f"[Setting Loop] Successfully set '{key}' to {actual_value} (Type: {type(actual_value)})", logging.DEBUG)

            except (ValueError, TypeError, InvalidOperation) as e:
                 self.write_log(f"[Setting Loop] Failed to apply setting '{key}'='{value}'. Error: {e}", logging.WARNING)
        self.write_log("Finished applying strategy settings.", logging.DEBUG)
        # --- End Apply Settings ---

    #----------------------------------------------------------------------
    # 生命周期抽象方法（必须由子类实现）
    # Lifecycle Abstract Methods (Must be implemented by subclasses)
    #----------------------------------------------------------------------
    @abstractmethod
    def on_init(self) -> None:
        """
        策略初始化时回调。

        Callback when strategy is initialized.
        :return:
        """
        pass

    @abstractmethod
    def on_start(self) -> None:
        """
        策略启动时回调。

        Callback when strategy is started.
        :return:
        """
        pass

    @abstractmethod
    def on_stop(self) -> None:
        """
        策略停止时回调。

        Callback when strategy is stopped.
        :return:
        """
        pass

    #----------------------------------------------------------------------
    # 数据/事件抽象方法（必须由子类实现）
    # Data/Event Abstract Methods (Must be implemented by subclasses)
    #----------------------------------------------------------------------
    @abstractmethod
    def on_tick(self, tick: TickData) -> None:
        """
        新报价数据更新的回调。

        Callback of new tick data update.
        :param tick:
        :return:
        """
        # 注意：引擎可能会将 tick 作为字典传递，可能需要在此处或在引擎中进行转换
        # Note: The engine might pass tick as a dict, conversion might be needed here or in engine
        pass

    @abstractmethod
    def on_order(self, order: OrderData) -> None:
        """
        新订单数据更新回调。

        Callback of new order data update.
        :param order:
        :return:
        """
        # 注意：引擎可能会将订单作为字典传递
        # 更新活跃订单集的基本实现
        # Note: The engine might pass order as a dict
        # Base implementation to update active orders set
        self._update_active_orders(order)
        pass

    @abstractmethod
    def on_trade(self, trade: TradeData) -> None:
        """
        新交易数据更新的回调。

        Callback of new trade data update.
        :param trade:
        :return:
        """
        # 注意：引擎可能会将 trade 作为字典传递
        # 更新仓位的基本实现
        # Note: The engine might pass trade as a dict
        # Base implementation to update position
        self._update_pos(trade)
        pass

    #----------------------------------------------------------------------
    # 标准交易方法（由基类提供）
    # Standard Trading Methods (Provided by Base Class)
    #----------------------------------------------------------------------
    def buy(self, price: float, volume: float, lock: bool = False, **kwargs) -> Optional[str]:
        """
        发送买入未平仓订单。

        Send buy open order.
        :param price:
        :param volume:
        :param lock:
        :param kwargs:
        :return:
        """
        return self.send_order(Direction.LONG, Offset.OPEN, price, volume, lock, **kwargs)

    def sell(self, price: float, volume: float, lock: bool = False, **kwargs) -> Optional[str]:
        """
        发送卖出平仓订单。

        Send sell close order.
        :param price:
        :param volume:
        :param lock:
        :param kwargs:
        :return:
        """
        return self.send_order(Direction.SHORT, Offset.CLOSE, price, volume, lock, **kwargs)

    def short(self, price: float, volume: float, lock: bool = False, **kwargs) -> Optional[str]:
        """
        发送空仓订单。

        Send short open order.
        :param price:
        :param volume:
        :param lock:
        :param kwargs:
        :return:
        """
        return self.send_order(Direction.SHORT, Offset.OPEN, price, volume, lock, **kwargs)

    def cover(self, price: float, volume: float, lock: bool = False, **kwargs) -> Optional[str]:
        """
        发送买入平仓订单。

        Send buy close order.
        :param price:
        :param volume:
        :param lock:
        :param kwargs:
        :return:
        """
        return self.send_order(Direction.LONG, Offset.CLOSE, price, volume, lock, **kwargs)

    def send_order(
        self,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        lock: bool = False, # Notice: 如果需要，实现位置锁定(Implement position locking if needed)
        **kwargs
    ) -> Optional[str]:
        """
        向交易引擎发送限价订单。

        Send a limit order to the trading engine.
        :param direction:
        :param offset:
        :param price:
        :param volume:
        :param lock:
        :param kwargs:
        :return:
        """
        if not self.trading:
            return None

        # 调用引擎之前进行符号匹配的基本检查(Basic check for symbol matching before calling engine)
        symbol_part = self.vt_symbol.split('.')[0]
        exchange_part = self.vt_symbol.split('.')[-1]
        try:
             exchange_enum = Exchange(exchange_part)
        except ValueError:
             logger.error(f"Invalid exchange '{exchange_part}' in vt_symbol '{self.vt_symbol}'. Cannot send order.")
             return None

        # --- 使用引擎标志检测回测模式(Detect Backtest Mode using engine flag) ---
        is_backtest_mode = getattr(self.strategy_engine, 'is_backtest_mode', False)
        # --- End Detect Backtest Mode --- 

        # --- 条件订单发送(Conditional Order Sending) ---
        vt_orderid = None
        if is_backtest_mode:
            # --- 回测：通过 PUSH 直接发送 pickled tuple(Backtest: Send pickled tuple directly via PUSH) ---
            logger.debug("Backtest Mode: Preparing to send order tuple directly via PUSH.")
            order_req_dict = {
                "symbol": symbol_part,
                "exchange": exchange_enum.value,
                "direction": direction.value,
                "type": OrderType.LIMIT.value, 
                "volume": float(volume),
                "price": float(price),
                "offset": offset.value,
                "reference": f"{self.strategy_name}_backtest" # 注明来源(Indicate source)
            }
            req_tuple = ("send_order", (order_req_dict,), {})
            try:
                # packed_request = pickle.dumps(req_tuple) # --- 替换为 msgpack(Replaced with msgpack) ---
                packed_request = msgpack.packb(req_tuple, use_bin_type=True)
                # --- Use order_pusher socket ---
                self.strategy_engine.order_pusher.send(packed_request)
                # +++ 添加日志：确认 PUSH 发送操作已执行 +++
                logger.debug(f"[BaseLiveStrategy SendOrder Backtest] PUSH send executed for request tuple: {req_tuple}")
                # +++ 结束添加 +++
                # --- End Use order_pusher ---
                vt_orderid = None # ID 将通过 OrderData 从 SimulationEngine 获取(ID will come from SimulationEngine via OrderData)
            except pickle.PicklingError as e:
                 logger.error(f"Backtest Mode: Failed to pickle order request: {e}")
            except (msgpack.PackException, TypeError) as e_pack:
                 logger.error(f"Backtest Mode: Failed to msgpack order request: {e_pack}")
            except zmq.ZMQError as e:
                 logger.error(f"Backtest Mode: ZMQ error sending order request: {e}")
            # --- End Backtest Send ---
        else:
            # --- 实时交易：使用 RPC（通过引擎的方法）[Live Trading: Use RPC (via engine's method)] ---
            logger.debug("Live Mode: Sending order via RPC.")
            # 使用引擎方法的现有 RPC 逻辑(Existing RPC logic using the engine's method)
            vt_orderid = self.strategy_engine.send_limit_order(
                symbol=symbol_part,
                exchange=exchange_enum,
                direction=direction,
                offset=offset,
                price=price,
                volume=volume
            )
            # --- End Live Trading Send --- 

        # --- 通用逻辑：仅在实时模式下添加到有效订单(Common Logic: Add to active orders ONLY in live mode) ---
        if vt_orderid and not is_backtest_mode:
            self.active_orders.add(vt_orderid)
            logger.info(f"Sent Order: {self.vt_symbol} {direction.value} {offset.value} {volume} @ {price} -> {vt_orderid} (Mode: Live)")
        elif is_backtest_mode:
            # 记录请求已发送但 ID 待处理(Log that request was sent, but ID is pending)
            logger.info(f"Sent Order Request: {self.vt_symbol} {direction.value} {offset.value} {volume} @ {price} (Mode: Backtest, ID Pending)")
        # Error logging is handled within each branch or by the engine's live method
        # 错误日志在每个分支内处理，或通过引擎的实时方法处理
        return vt_orderid # Returns real ID in live, None in backtest

    def cancel_order(self, vt_orderid: str) -> None:
        """
        取消活动状态订单。

        Cancel an active order.
        :param vt_orderid:
        :return:
        """
        if not self.trading:
            logger.warning(f"Strategy not trading, cannot cancel order {vt_orderid}.")
            return

        if vt_orderid not in self.active_orders:
            logger.debug(f"Order {vt_orderid} not in active orders list for strategy {self.strategy_name}. Maybe already inactive? Skipping cancel request.")
            # 如果订单在取消调用之前可能变为非活动状态，则避免记录警告
            # Avoid logging warning if order might have become inactive just before cancel call
            # logger.warning(f"Order {vt_orderid} not in active orders list for strategy {self.strategy_name}.")
            return # 如果没有被追踪为活跃状态，则不发送取消(Simply don't send cancel if not tracked as active)

        # --- 检测回测模式(Detect Backtest Mode) ---
        is_backtest_mode = getattr(self.strategy_engine, 'is_backtest_mode', False)

        logger.info(f"Requesting Cancel for order {vt_orderid} (Mode: {'Backtest' if is_backtest_mode else 'Live'}) ")
        
        if is_backtest_mode:
            # --- 回测：通过 PUSH 发送取消请求(Backtest: Send cancel request via PUSH) ---
            cancel_req_dict = {
                "action": "cancel",
                "vt_orderid": vt_orderid
            }
            try:
                packed_request = msgpack.packb(cancel_req_dict, use_bin_type=True)
                self.strategy_engine.order_pusher.send(packed_request)
                logger.info(f"Backtest Mode: Sent cancel request for {vt_orderid}")
            except (msgpack.PackException, TypeError) as e_pack:
                 logger.error(f"Backtest Mode: Failed to msgpack cancel request: {e_pack}")
            except zmq.ZMQError as e:
                 logger.error(f"Backtest Mode: ZMQ error sending cancel request: {e}")
            # --- End Backtest Send --- 
        else:
            # --- 实时交易：使用 RPC(Live Trading: Use RPC) ---
            # 通过 RPC 将取消委托给引擎(Delegate cancellation to the engine via RPC)
            if not hasattr(self.strategy_engine, 'cancel_limit_order'):
                 logger.error("Strategy engine does not have 'cancel_limit_order' method for live trading.")
                 return
            self.strategy_engine.cancel_limit_order(vt_orderid)
            # --- End Live Trading Send --- 

        # 注意：订单将保留在 active_orders 中，直到通过 on_order 确认取消/处于非活动状态
        # Note: Order remains in active_orders until confirmed cancelled/inactive via on_order

    def cancel_all(self) -> None:
        """
        取消该策略管理的所有有效订单。

        Cancel all active orders managed by this strategy.
        :return:
        """
        if not self.trading:
            return

        if not self.active_orders:
            return

        logger.info(f"Cancelling all ({len(self.active_orders)}) active orders for {self.strategy_name}...")
        # 迭代副本，因为cancel_order可能会通过on_order间接修改集合
        # Iterate over a copy as cancel_order might modify the set indirectly via on_order
        for vt_orderid in list(self.active_orders):
            self.cancel_order(vt_orderid)

    #----------------------------------------------------------------------
    # 辅助方法（可供子类使用）
    # Helper Methods (Can be used by subclasses)
    #----------------------------------------------------------------------
    def get_pos(self) -> Decimal:
        """
        获取策略合约的当前计算位置。

        Get current calculated position for the strategy's symbol.
        :return:
        """
        return self.pos

    def get_entry_price(self) -> Optional[Decimal]:
        """
        获取当前仓位的平均入场价格（如果有）。

        Get the average entry price of the current position, if any.
        :return:
        """
        return self.entry_price

    def write_log(self, msg: str, level: int = logging.INFO) -> None:
        """
        编写以策略名称为前缀的日志消息。

        Write a log message prefixed with strategy name.
        :param msg:
        :param level:
        :return:
        """
        # 注意：记录器已经包含策略名称
        # Note: The logger already includes the strategy name
        logger.log(level, f"[{self.strategy_name}] {msg}")

    def _update_pos(self, trade: TradeData) -> None:
        """
        帮助使用 Decimal 根据交易数据更新位置、入场成本和平均入场价格。

        Helper to update position, entry cost and average entry price based on trade data using Decimal.
        :param trade:
        :return:
        """
        if trade.vt_symbol != self.vt_symbol:
            return

        # --- 获取交易量（Decimal）[Get Trade Volume (Decimal)] ---
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

        # --- 获取交易价格（Decimal）[Get Trade Price (Decimal)] ---
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

        # 根据方向计算位置变化(Calculate position change based on direction)
        if direction_value == Direction.LONG.value: # Buy
            pos_change = trade_volume
        elif direction_value == Direction.SHORT.value: # Sell
            pos_change = -trade_volume
        else:
            self.write_log(f"_update_pos: Unknown or invalid direction value: {direction_value} for trade {trade.vt_tradeid}", logging.ERROR)
            return

        # --- 应用仓位变更(Apply position change) ---
        try:
            self.pos += pos_change
            current_pos = self.pos
        except Exception as e:
            self.write_log(f"_update_pos: Error during Decimal position update arithmetic: {e}. Pos: {previous_pos}, Change: {pos_change}", logging.ERROR)
            return

        # --- 更新入场成本和价格（仅当 trade_price 有效时）[Update Entry Cost and Price (Only if trade_price is valid)] ---
        if trade_price is not None:
            try:
                cost_of_this_trade = trade_price * trade_volume.copy_abs()

                if previous_pos.is_zero() and not current_pos.is_zero():
                    # 情况 1：平至非平（开盘第一回合）[Case 1: Flat to Non-Flat (Opening first leg)]
                    self.entry_cost = cost_of_this_trade
                    self.entry_price = trade_price
                    self.write_log(f"_update_pos: Position opened. EntryCost={self.entry_cost:.4f}, EntryPrice={self.entry_price:.4f}", logging.DEBUG)

                elif not previous_pos.is_zero() and not current_pos.is_zero() and (previous_pos * current_pos > 0):
                    # 情况 2 和 3：添加或部分关闭（同方向）[Case 2 & 3: Adding or Partially Closing (Same Direction)]
                    if abs(current_pos) > abs(previous_pos):
                        # 情况 2：添加到位置(Case 2: Adding to position)
                        self.entry_cost = previous_entry_cost + cost_of_this_trade
                        if not current_pos.is_zero(): # 这里不应该为零，但无论如何都要检查(Should not be zero here, but check anyway)
                            self.entry_price = self.entry_cost / current_pos.copy_abs()
                            self.write_log(f"_update_pos: Position increased. New EntryCost={self.entry_cost:.4f}, New EntryPrice={self.entry_price:.4f}", logging.DEBUG)
                        else: # Safety fallback
                             self.write_log(f"_update_pos: Position zero unexpectedly after adding. Resetting entry cost/price.", logging.WARNING)
                             self.entry_cost = Decimal("0.0")
                             self.entry_price = None

                    elif abs(current_pos) < abs(previous_pos):
                         # 案例三：部分关闭(Case 3: Partially Closing)
                         if previous_entry_price is not None:
                             # 根据之前的平均价格按比例降低成本(Reduce cost proportionally based on the previous average price)
                             cost_reduction = previous_entry_price * trade_volume.copy_abs()
                             self.entry_cost = previous_entry_cost - cost_reduction
                             # 入场价格保持不变(Entry price remains unchanged)
                             self.entry_price = previous_entry_price
                             self.write_log(f"_update_pos: Position partially closed. New EntryCost={self.entry_cost:.4f}, EntryPrice remains {self.entry_price:.4f}", logging.DEBUG)
                         else:
                             # 如果 pos > 0 则不应该发生，但要采取防御措施(Should not happen if pos > 0, but handle defensively)
                             self.write_log(f"_update_pos: Cannot reduce cost - previous entry price is missing! Cost remains {previous_entry_cost:.4f}, Price remains None.", logging.WARNING)
                             self.entry_cost = previous_entry_cost
                             self.entry_price = None
                    # 否则：交易量不变？交易量非零时不应发生这种情况
                    # else: Volume unchanged? Should not happen with non-zero trade volume

                elif not previous_pos.is_zero() and current_pos.is_zero():
                    # 情况 4：非平缓至平缓（收盘最后一段）[Case 4: Non-Flat to Flat (Closing last leg)]
                    self.entry_cost = Decimal("0.0")
                    self.entry_price = None
                    self.write_log(f"_update_pos: Position closed. EntryCost reset to 0, EntryPrice reset to None.", logging.DEBUG)

                # --- 用乘法检查代替 .sign() 检查(Replace .sign() check with multiplication check) ---
                # elif previous_pos.sign() != current_pos.sign():
                # 当它们的符号相反时，乘积必然为负。
                elif previous_pos * current_pos < 0:
                    # 案例5：翻转仓位(Case 5: Flipping Position)
                    # 新仓位的成本完全取决于翻转交易
                    # --- 更正了翻转的成本计算 ---
                    # 成本应反映新仓位在翻转价格时的价值
                    # New position's cost is based solely on the flip trade
                    # --- Corrected cost calculation for flips ---
                    # Cost should reflect the value of the new position at the flip price
                    self.entry_cost = trade_price * current_pos.copy_abs() 
                    # --- End Correction ---
                    self.entry_price = trade_price # New position entry price is the flip trade price
                    self.write_log(f"_update_pos: Position flipped. New EntryCost={self.entry_cost:.4f}, New EntryPrice={self.entry_price:.4f}", logging.DEBUG)

                # --- 健全性检查/舍入（可选但推荐）[Sanity Check/Rounding (Optional but recommended)] ---
                # 如果位置非常接近零，则重置成本/价格(If position is very close to zero, reset cost/price)
                if current_pos.is_zero(): # Use is_zero() for robustness
                    self.entry_cost = Decimal("0.0")
                    self.entry_price = None
                # 可选择将入场价格四舍五入到合理的小数位数
                # Optionally round entry price to a reasonable number of decimal places
                # if self.entry_price is not None:
                #     self.entry_price = self.entry_price.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP) # Adjust precision as needed

            except Exception as e_entry_price:
                self.write_log(f"_update_pos: Error calculating entry cost/price: {e_entry_price}", logging.ERROR)
                # 为安全起见，发生错误时重置？(Reset on error for safety?)
                self.entry_cost = Decimal("0.0")
                self.entry_price = None
        # --- End Update Entry Cost and Price --- 

        # --- 记录仓位更新(Log the position update) ---
        log_entry_price = f" EntryPrice={self.entry_price:.4f}" if self.entry_price is not None else " EntryPrice=None"
        log_entry_cost = f" EntryCost={self.entry_cost:.4f}" # Always log cost
        self.write_log(
            f"Position Update: Symbol={trade.vt_symbol}, TradeID={trade.vt_tradeid}, "
            f"DirectionValue={direction_value}, OffsetValue={offset_value}, Price={trade.price}, Volume={trade_volume}. "
            f"PrevPos={previous_pos}, Change={pos_change}, NewPos={current_pos}.{log_entry_cost}{log_entry_price}",
            logging.INFO
        )

    def _update_active_orders(self, order: OrderData) -> None:
        """
        用于更新活跃订单集的辅助函数。应在 on_order 中调用。

        Helper to update the set of active orders. Should be called in on_order.
        :param order:
        :return:
        """
        # 确保订单属于该策略的合约(Ensure the order belongs to this strategy's symbol)
        if order.vt_symbol == self.vt_symbol:
            is_active_order = order.vt_orderid in self.active_orders
            # 如果订单现在处于非活动状态并且我们正在跟踪它，请将其删除。
            # If order is now inactive, and we were tracking it, remove it.
            if not order.is_active() and is_active_order:
                self.active_orders.remove(order.vt_orderid)
                self.write_log(f"Order {order.vt_orderid} removed from active list (Status: {order.status}). Active count: {len(self.active_orders)}.", logging.DEBUG)
            # 如果订单现在处于活动状态并且我们尚未跟踪它，请添加它。
            # 这处理了真实 ID 在回测中到达或实时订单出现的情况。
            # If order is now active, and we were NOT tracking it, add it.
            # This handles the case where the real ID arrives in backtest, or live orders appearing.
            elif order.is_active() and not is_active_order:
                 # 检查状态以避免在错过初始更新时添加已取消/拒绝的订单
                 # Check status to avoid adding already cancelled/rejected orders if initial update was missed
                 if order.status not in [Status.CANCELLED, Status.REJECTED]:
                      self.active_orders.add(order.vt_orderid)
                      self.write_log(f"Order {order.vt_orderid} added to active list (Status: {order.status}). Active count: {len(self.active_orders)}.", logging.DEBUG)
                 else:
                      self.write_log(f"Order {order.vt_orderid} update received but status is {order.status}, not adding to active list.", logging.DEBUG)
        # 如果订单属于另一个合约，则忽略此实例的活动订单跟踪。
        # If order belongs to another symbol, ignore for active order tracking of this instance.

    #----------------------------------------------------------------------
    # 内部生命周期管理（由引擎调用）
    # Internal Lifecycle Management (Called by the Engine)
    #----------------------------------------------------------------------
    def _init_strategy(self) -> None:
        """
        内部初始化。由引擎调用。

        Internal initialization. Called by the engine.
        :return:
        """
        self.inited = True
        try:
            self.on_init()
            self.write_log("策略初始化完成")
        except Exception as e:
             logger.exception(f"Error during on_init: {e}")
             self.inited = False # Mark as failed initialization


    def _start_strategy(self) -> None:
        """
        内部启动。由引擎调用。

        Internal start. Called by the engine.
        :return:
        """
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
        """
        内部停止。由引擎调用。

        Internal stop. Called by the engine.
        :return:
        """
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