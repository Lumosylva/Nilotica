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
import pickle
from abc import ABCMeta, abstractmethod
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, Optional, Set, Union, TYPE_CHECKING

import msgpack
import zmq

from utils.logger import logger, setup_logging, INFO
from utils.i18n import _
from vnpy.trader.constant import Direction, Exchange, Offset, OrderType, Status
from vnpy.trader.object import OrderData, TickData, TradeData, BarData, Interval
from vnpy.trader.utility import BarGenerator
from datetime import datetime, time as dt_time

# 仅在类型检查时导入 StrategyEngine 以避免循环导入
if TYPE_CHECKING:
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
        strategy_engine: 'StrategyEngine', # 使用字符串类型提示
        strategy_name: str,              # 此策略实例的唯一名称(Unique name for this strategy instance)
        vt_symbol: str,                  # 该策略交易的主要合约(The primary symbol this strategy trades)
        setting: Dict[str, Any]          # 策略参数(Strategy parameters)
    ) -> None:
        self.strategy_engine: 'StrategyEngine' = strategy_engine # 类型提示也使用字符串
        self.strategy_name: str = strategy_name
        self.vt_symbol: str = vt_symbol
        self.setting: Dict[str, Any] = setting
        setup_logging(service_name=f"{self.__class__.__name__}[{self.strategy_name}]", level=INFO)

        # 策略状态变量(Strategy state variables)
        self.inited: bool = False
        self.trading: bool = False
        self.pos: Decimal = Decimal("0.0")               # vt_symbol 的当前仓位(Current position for vt_symbol)
        self.active_orders: Set[str] = set() # 此策略管理的活动 vt_orderids(Active vt_orderids managed by this strategy)

        # +++ 添加入场价格和成本跟踪(Add entry price and cost tracking) +++
        self.entry_price: Optional[Decimal] = None # 当前仓位的平均入场价(Average entry price of the current position)
        self.entry_cost: Decimal = Decimal("0.0")  # 当前仓位总成本(Total cost of the current position)

        # +++ K线生成相关 +++
        self.bar_generator: Optional[BarGenerator] = None
        bar_period = setting.get("bar_period", None)  # 例如 '1m', '5m', '1h', '1d'
        bar_daily_end = setting.get("bar_daily_end", None)  # 例如 '15:00:00'
        if bar_period:
            # 解析周期
            if bar_period.endswith("m"):
                window = int(bar_period[:-1])
                interval = Interval.MINUTE
            elif bar_period.endswith("h"):
                window = int(bar_period[:-1])
                interval = Interval.HOUR
            elif bar_period.endswith("d"):
                window = 1
                interval = Interval.DAILY
            else:
                window = 1
                interval = Interval.MINUTE
            # 日K线需要daily_end
            daily_end = None
            if interval == Interval.DAILY and bar_daily_end:
                try:
                    daily_end = dt_time.fromisoformat(bar_daily_end)
                except Exception:
                    daily_end = None
            # 1分钟K线直接on_bar，其它周期用on_window_bar
            if interval == Interval.MINUTE and window == 1:
                self.bar_generator = BarGenerator(
                    on_bar=self._on_generated_bar,
                    window=1,
                    interval=Interval.MINUTE
                )
            else:
                self.bar_generator = BarGenerator(
                    on_bar=self._noop_1m_bar_handler,
                    window=window,
                    on_window_bar=self._on_generated_bar,
                    interval=interval,
                    daily_end=daily_end
                )
        # +++ End K线生成相关 +++

        logger.debug(_("正在应用策略设置..."))
        for key, value in setting.items():
            try:
                target_type = None
                final_value_to_set = value # 从原始值开始(Start with the original value)
                
                # 从类注释中获取预期类型(Get expected type from class annotations)
                annotations = getattr(self.__class__, '__annotations__', {})
                detected_annotation = annotations.get(key)
                logger.debug(_("[设置循环] 键：'{}'，值：'{}'（类型：{}），检测到的注释：{}").format(key, value, type(value), detected_annotation))

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
                              logger.debug(_("[设置循环] 将浮点设置'{}'值{}转换为整数。").format(key, value))
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
                logger.debug(_("[设置循环] 成功将'{}'设置为{}（类型：{}）").format(key, actual_value, type(actual_value)))

            except (ValueError, TypeError, InvalidOperation) as e:
                 logger.debug(_("[设置循环] 无法应用设置'{}'='{}'。错误：{}").format(key, value, e))
        logger.debug(_("已完成应用策略设置。"))
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

    @abstractmethod
    def on_bar(self, bar: BarData) -> None:
        """
        新K线生成时的回调，由策略实现。
        :param bar: BarData对象
        """
        pass

    #----------------------------------------------------------------------
    # 标准交易方法（由基类提供）
    # Standard Trading Methods (Provided by Base Class)
    #----------------------------------------------------------------------
    def buy(self, price: float, volume: float) -> Optional[str]:
        """
        发送买入未平仓订单。

        Send buy open order.
        :param price:
        :param volume:
        :return:
        """
        return self.send_order(Direction.LONG, Offset.OPEN, price, volume)

    def sell(self, price: float, volume: float) -> Optional[str]:
        """
        发送卖出平仓订单。

        Send sell close order.
        :param price:
        :param volume:
        :return:
        """
        return self.send_order(Direction.SHORT, Offset.CLOSE, price, volume)

    def short(self, price: float, volume: float) -> Optional[str]:
        """
        发送空仓订单。

        Send short open order.
        :param price:
        :param volume:
        :return:
        """
        return self.send_order(Direction.SHORT, Offset.OPEN, price, volume)

    def cover(self, price: float, volume: float) -> Optional[str]:
        """
        发送买入平仓订单。

        Send buy close order.
        :param price:
        :param volume:
        :return:
        """
        return self.send_order(Direction.LONG, Offset.CLOSE, price, volume)

    def send_order(
        self,
        direction: Direction,
        offset: Offset,
        price: float,
        volume: float,
        # Notice: 如果需要，实现位置锁定(Implement position locking if needed)
    ) -> Optional[str]:
        """
        向交易引擎发送限价订单。

        Send a limit order to the trading engine.
        :param direction:
        :param offset:
        :param price:
        :param volume:
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
             logger.error(_("vt_symbol'{}'中的交易所'{}'无效。无法发送订单。").format(exchange_part, self.vt_symbol))
             return None

        # --- 使用引擎标志检测回测模式(Detect Backtest Mode using engine flag) ---
        is_backtest_mode = getattr(self.strategy_engine, 'is_backtest_mode', False)
        # --- End Detect Backtest Mode --- 

        # --- 条件订单发送(Conditional Order Sending) ---
        vt_orderid = None
        if is_backtest_mode:
            # --- 回测：通过 PUSH 直接发送 pickled tuple(Backtest: Send pickled tuple directly via PUSH) ---
            logger.debug(_("回测模式：准备通过PUSH直接发送订单元组。"))
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
                logger.debug(_("[BaseLiveStrategy SendOrder 回测] 针对请求元组 {} 执行 PUSH 发送").format(req_tuple))
                # +++ 结束添加 +++
                # --- End Use order_pusher ---
                vt_orderid = None # ID 将通过 OrderData 从 SimulationEngine 获取(ID will come from SimulationEngine via OrderData)
            except pickle.PicklingError as e:
                 logger.error(_("回测模式：pickle 订单请求失败：{}").format(e))
            except (msgpack.PackException, TypeError) as e_pack:
                 logger.error(_("回测模式：无法发送订单请求消息：{}").format(e_pack))
            except zmq.ZMQError as e:
                 logger.error(_("回测模式：ZMQ发送订单请求时出错：{}").format(e))
            # --- End Backtest Send ---
        else:
            # --- 实时交易：使用 RPC（通过引擎的方法）[Live Trading: Use RPC (via engine's method)] ---
            logger.debug(_("实时模式：通过 RPC 发送订单。"))
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
            logger.info(_("已发送订单：{} {} {} {} @ {} -> {}（模式：实时）").format(self.vt_symbol, direction.value, offset.value, volume, price, vt_orderid))
        elif is_backtest_mode:
            # 记录请求已发送但 ID 待处理(Log that request was sent, but ID is pending)
            logger.info(_("已发送订单请求：{} {} {} {} @ {}（模式：回测，ID 待定）").format(self.vt_symbol, direction.value, offset.value, volume, price))
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
            logger.warning(_("策略未交易，无法取消订单{}。").format(vt_orderid))
            return

        if vt_orderid not in self.active_orders:
            logger.debug(_("订单 {} 不在策略 {} 的有效订单列表中。可能已经处于非活动状态？跳过取消请求。").format(vt_orderid, self.strategy_name))
            # 如果订单在取消调用之前可能变为非活动状态，则避免记录警告
            # Avoid logging warning if order might have become inactive just before cancel call
            return # 如果没有被追踪为活跃状态，则不发送取消(Simply don't send cancel if not tracked as active)

        # --- 检测回测模式(Detect Backtest Mode) ---
        is_backtest_mode = getattr(self.strategy_engine, 'is_backtest_mode', False)

        logger.info(_("请求取消订单 {}（模式：{}）").format(vt_orderid, ('Backtest' if is_backtest_mode else 'Live')))
        
        if is_backtest_mode:
            # --- 回测：通过 PUSH 发送取消请求(Backtest: Send cancel request via PUSH) ---
            cancel_req_dict = {
                "action": "cancel",
                "vt_orderid": vt_orderid
            }
            try:
                packed_request = msgpack.packb(cancel_req_dict, use_bin_type=True)
                self.strategy_engine.order_pusher.send(packed_request)
                logger.info(_("回测模式：已发送 {} 的取消请求").format(vt_orderid))
            except (msgpack.PackException, TypeError) as e_pack:
                 logger.error(_("回测模式：无法发送 msgpack 取消请求：{}").format(e_pack))
            except zmq.ZMQError as e:
                 logger.error(_("回测模式：ZMQ 发送取消请求时出错：{}").format(e))
            # --- End Backtest Send --- 
        else:
            # --- 实时交易：使用 RPC(Live Trading: Use RPC) ---
            # 通过 RPC 将取消委托给引擎(Delegate cancellation to the engine via RPC)
            if not hasattr(self.strategy_engine, 'cancel_limit_order'):
                 logger.error(_("策略引擎没有用于实时交易的'cancel_limit_order'方法。"))
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

        logger.info(_("正在取消所有 ({}) 个 {} 的有效订单...").format(len(self.active_orders), self.strategy_name))
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
                 logger.error(_("_update_pos：无法将交易量'{}'转换为浮点数。忽略交易'{}'。").format(trade.volume, trade.vt_tradeid))
                 return
        try:
            trade_volume = Decimal(str(trade_volume_float))
        except (InvalidOperation, ValueError, TypeError) as e:
             logger.error(_("_update_pos：无法将交易量浮点数'{}'转换为十进制：{}。忽略交易{}。").format(trade_volume_float, e, trade.vt_tradeid))
             return
        if trade_volume.is_zero():
            return
        # --- End Get Trade Volume ---

        # --- 获取交易价格（Decimal）[Get Trade Price (Decimal)] ---
        trade_price_float = getattr(trade, 'price', None)
        if trade_price_float is None:
            logger.warning(_("_update_pos：交易 {} 无价格。忽略入场价格更新。").format(trade.vt_tradeid))
            trade_price = None # Indicate price is unavailable
        else:
            try:
                trade_price = Decimal(str(trade_price_float))
            except (InvalidOperation, ValueError, TypeError) as e:
                 logger.error(_("_update_pos：无法将交易价格浮点数'{}'转换为十进制：{}。忽略交易'{}'的入场价格更新。").format(trade_price_float, e, trade.vt_tradeid))
                 trade_price = None # Indicate price is unavailable/invalid
        # --- End Get Trade Price ---

        previous_pos = self.pos
        previous_entry_cost = self.entry_cost # Store previous cost
        previous_entry_price = self.entry_price # Store previous entry price

        direction_value = getattr(trade.direction, 'value', None)
        offset_value = getattr(trade.offset, 'value', None)

        # 根据方向计算位置变化(Calculate position change based on direction)
        if direction_value == Direction.LONG.value: # Buy
            pos_change = trade_volume
        elif direction_value == Direction.SHORT.value: # Sell
            pos_change = -trade_volume
        else:
            logger.error(_("_update_pos：交易方向值未知或无效：{}").format(direction_value, trade.vt_tradeid))
            return

        # --- 应用仓位变更(Apply position change) ---
        try:
            self.pos += pos_change
            current_pos = self.pos
        except Exception as e:
            logger.error(_("_update_pos：小数位置更新算法出错：{}。位置：{}，变化：{}").format(e, previous_pos, pos_change))
            return

        # --- 更新入场成本和价格（仅当 trade_price 有效时）[Update Entry Cost and Price (Only if trade_price is valid)] ---
        if trade_price is not None:
            try:
                cost_of_this_trade = trade_price * trade_volume.copy_abs()

                if previous_pos.is_zero() and not current_pos.is_zero():
                    # 情况 1：平至非平（开盘第一回合）[Case 1: Flat to Non-Flat (Opening first leg)]
                    self.entry_cost = cost_of_this_trade
                    self.entry_price = trade_price
                    logger.debug(_("_update_pos：已开仓位。EntryCost={:.4f}，EntryPrice={:.4f}").format(self.entry_cost, self.entry_price))

                elif not previous_pos.is_zero() and not current_pos.is_zero() and (previous_pos * current_pos > 0):
                    # 情况 2 和 3：添加或部分关闭（同方向）[Case 2 & 3: Adding or Partially Closing (Same Direction)]
                    if abs(current_pos) > abs(previous_pos):
                        # 情况 2：添加到位置(Case 2: Adding to position)
                        self.entry_cost = previous_entry_cost + cost_of_this_trade
                        if not current_pos.is_zero(): # 这里不应该为零，但无论如何都要检查(Should not be zero here, but check anyway)
                            self.entry_price = self.entry_cost / current_pos.copy_abs()
                            logger.debug(_("_update_pos: 位置增加。新的 EntryCost={:.4f}，新的 EntryPrice={:.4f}").format(self.entry_cost, self.entry_price))
                        else: # Safety fallback
                             logger.warning(_("_update_pos：添加后位置意外为零。重置入场成本/价格。"))
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
                             logger.debug(_("_update_pos：仓位部分平仓。新的 EntryCost={:.4f}，EntryPrice 保持不变 {:.4f}").format(self.entry_cost, self.entry_price))
                         else:
                             # 如果 pos > 0 则不应该发生，但要采取防御措施(Should not happen if pos > 0, but handle defensively)
                             logger.warning(_("_update_pos：无法降低成本 - 之前的入场价格缺失！成本仍为 {:.4f}，价格仍为 None。").format(previous_entry_cost))
                             self.entry_cost = previous_entry_cost
                             self.entry_price = None
                    # 否则：交易量不变？交易量非零时不应发生这种情况
                    # else: Volume unchanged? Should not happen with non-zero trade volume

                elif not previous_pos.is_zero() and current_pos.is_zero():
                    # 情况 4：非平缓至平缓（收盘最后一段）[Case 4: Non-Flat to Flat (Closing last leg)]
                    self.entry_cost = Decimal("0.0")
                    self.entry_price = None
                    logger.debug(_("_update_pos：仓位已平仓。EntryCost 重置为 0，EntryPrice 重置为 None。"))

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
                    logger.debug(_("_update_pos：位置已反转。新的 EntryCost={:.4f}，新的 EntryPrice={:.4f}").format(self.entry_cost, self.entry_price))

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
                logger.error(_("_update_pos：计算入场成本/价格时出错：{}").format(e_entry_price))
                # 为安全起见，发生错误时重置？(Reset on error for safety?)
                self.entry_cost = Decimal("0.0")
                self.entry_price = None
        # --- End Update Entry Cost and Price --- 

        # --- 记录仓位更新(Log the position update) ---
        log_entry_price = f" EntryPrice={self.entry_price:.4f}" if self.entry_price is not None else " EntryPrice=None"
        log_entry_cost = f" EntryCost={self.entry_cost:.4f}" # Always log cost
        logger.info(_("持仓更新：Symbol={}, TradeID={}, DirectionValue={}, OffsetValue={}, Price={}, Volume={}. PrevPos={}, Change={}, NewPos={}.{}")
            .format(
                trade.vt_symbol, trade.vt_tradeid,
                direction_value, offset_value, trade.price, trade_volume,
                previous_pos, pos_change, current_pos,
                log_entry_cost, log_entry_price
            ))

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
                logger.debug(_("订单 {} 已从活跃列表中移除（状态：{}）。活跃数量：{}。").format(order.vt_orderid, order.status, len(self.active_orders)))
            # 如果订单现在处于活动状态并且我们尚未跟踪它，请添加它。
            # 这处理了真实 ID 在回测中到达或实时订单出现的情况。
            # If order is now active, and we were NOT tracking it, add it.
            # This handles the case where the real ID arrives in backtest, or live orders appearing.
            elif order.is_active() and not is_active_order:
                 # 检查状态以避免在错过初始更新时添加已取消/拒绝的订单
                 # Check status to avoid adding already cancelled/rejected orders if initial update was missed
                 if order.status not in [Status.CANCELLED, Status.REJECTED]:
                      self.active_orders.add(order.vt_orderid)
                      logger.debug(_("订单 {} 已添加到有效列表（状态：{}）。有效数量：{}。").format(order.vt_orderid, order.status, len(self.active_orders)))
                 else:
                      logger.debug(_("已收到订单 {} 更新但状态为 {}，未添加到活动列表。").format(order.vt_orderid, order.status))
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
            logger.info(_("策略初始化完成"))
        except Exception as e:
             logger.exception(_("on_init 期间出错：{}").format(e))
             self.inited = False # Mark as failed initialization


    def _start_strategy(self) -> None:
        """
        内部启动。由引擎调用。

        Internal start. Called by the engine.
        :return:
        """
        if not self.inited:
             logger.error(_("由于初始化失败或被跳过，无法启动策略。"))
             return
        if self.trading:
             logger.warning(_("策略已开始。"))
             return
        self.trading = True
        try:
            self.on_start()
            logger.info(_("策略启动"))
        except Exception as e:
            logger.exception(_("on_start 期间出错：{}").format(e))
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
                logger.info(_("策略停止"))
            except Exception as e:
                logger.exception(_("on_stop 或 cancellation_all 期间出错：{}").format(e))
        else:
             logger.info(_("策略停止 (未曾启动或启动失败)"))

    def _noop_1m_bar_handler(self, bar: BarData) -> None:
        """占位方法：仅用于多周期K线时丢弃1分钟K线。"""
        pass

    def _on_generated_bar(self, bar: BarData) -> None:
        """BarGenerator回调，调用策略的on_bar。"""
        try:
            self.on_bar(bar)
        except Exception as e:
            logger.exception(_("on_bar处理K线时出错: {}".format(e)))