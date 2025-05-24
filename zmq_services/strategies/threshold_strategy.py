import logging
from decimal import Decimal, InvalidOperation
from typing import Any, Dict

from vnpy.trader.constant import Offset, Status
from vnpy.trader.object import OrderData, TickData, TradeData, BarData

from utils.i18n import _
from utils.logger import logger, setup_logging, INFO

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

        setup_logging(service_name=__class__.__name__, level=INFO)

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
                      logger.error(_("参数 '{}' 的类型不正确：{}。预期类型：{}").format(param, type(value), expected_type))
                      missing_params.append(f"{param} (Wrong Type: {type(value)})" )


        if missing_params:
            error_msg = _("策略 '{}' 无法初始化。'setting' 字典中缺少必需参数或参数类型不正确：{}").format(self.strategy_name, missing_params)
            logger.critical(error_msg)
            raise ValueError(error_msg) # Raise error to stop engine loading this strategy
        # --- End Parameter Verification ---

        # Log the parameters that were successfully loaded from settings
        logger.info(_("{} ({}) 参数已加载:").format(self.__class__.__name__, self.strategy_name))
        for param in required_params:
             logger.info(_("{}: {}").format(param, getattr(self, param)))

        # Initialize state variables here (not in on_init anymore)
        self.long_entry_active = False
        self.close_pending = False
        self.min_pos_threshold: Decimal = Decimal("1e-8") # Threshold for comparing position near zero

    # --- Lifecycle Callbacks --- 
    def on_init(self) -> None:
        """Called once after __init__. Strategy parameters should be ready."""
        logger.info(_("{} 已初始化 (on_init 已调用).").format(self.__class__.__name__))

    def on_start(self) -> None:
        """Called when strategy starts trading."""
        logger.info(_("{} 已启动.").format(self.__class__.__name__))

    def on_stop(self) -> None:
        """Called when strategy stops trading."""
        logger.info(_("{} 已停止.").format(self.__class__.__name__))

    # --- Event Callbacks --- 
    def on_tick(self, tick: TickData) -> None:
        """Process new tick data using Decimal."""
        if tick.vt_symbol != self.vt_symbol or not self.trading:
            return

        try:
            log_last_price = Decimal(str(getattr(tick, 'last_price', 'NaN')))
            logger.info(
                _("on_tick 轻量处理: 时间={} vt={} 最新价={}").format(
                    tick.datetime.strftime('%H:%M:%S.%f')[:-3],
                    tick.vt_symbol, 
                    log_last_price
                )
            )
        except (InvalidOperation, TypeError):
            logger.debug(_("on_tick 轻量处理 {} 时间 {},价格转换失败.").format(tick.vt_symbol, tick.datetime.strftime('%H:%M:%S.%f')[:-3]))

    def on_bar(self, bar: BarData) -> None:
        """Process new bar data using Decimal."""
        pass # 保留注释掉的逻辑，根据需要取消注释和修改

    def on_order(self, order: OrderData) -> None:
        """Process order updates."""
        super().on_order(order) 

        if order.vt_symbol != self.vt_symbol:
            return

        if not order.is_active(): 
            if order.offset == Offset.OPEN:
                if self.long_entry_active:
                     if order.status not in [Status.ALLTRADED, Status.PARTTRADED]:
                         self.long_entry_active = False
                         logger.info(_("开仓订单 {} 最终状态 {}，重置进场意图.").format(order.vt_orderid, order.status.value))
            elif order.offset == Offset.CLOSE:
                if self.close_pending:
                    self.close_pending = False
                    if order.status not in [Status.ALLTRADED, Status.PARTTRADED]:
                        logger.warning(_("平仓订单 {} 最终状态 {}，重置平仓等待标志。当前持仓 {}").format(order.vt_orderid, order.status.value, self.pos))

    def on_trade(self, trade: TradeData) -> None:
        """Process trade/fill updates using Decimal."""
        super().on_trade(trade) 

        if trade.vt_symbol != self.vt_symbol:
            return

        if trade.offset == Offset.OPEN:
            if not self.pos.is_zero():
                 self.long_entry_active = True 
                 self.close_pending = False    
                 logger.info(_("开仓成交确认: VTOrderID={}, 价格={}, 数量={}。新持仓: {}, 平均进场价: {}").format(
                     trade.vt_orderid, 
                     trade.price, 
                     trade.volume, 
                     self.pos, 
                     self.entry_price
                 ))
            else:
                 logger.error(_("收到 OPEN 成交 {} 但基类更新后 pos 仍为 0。检查 BaseLiveStrategy._update_pos 实现.").format(trade.vt_tradeid))

        elif trade.offset == Offset.CLOSE:
            self.close_pending = False
            logger.info(_("平仓成交确认: VTOrderID={}, 价格={}, 数量={}。新持仓: {}").format(
                trade.vt_orderid, 
                trade.price, 
                trade.volume, 
                self.pos
            ))
            if self.pos.is_zero(): 
                self.long_entry_active = False
                logger.info(_("持仓已全部平仓。重置状态.")) 

    # --- Helper Methods ---
    def _calculate_and_set_targets(self, entry_price: float, context: str) -> None:
        """Calculates and updates profit/stop targets based on entry price."""
        if not all([hasattr(self, attr) for attr in [
            'profit_target_ticks', 'stop_loss_ticks', 'price_tick'
        ]]):
             logger.error(_("无法计算目标，缺少参数."))
             return

        self.entry_price = entry_price
        self.target_price = entry_price + float(self.profit_target_ticks * self.price_tick)
        self.stop_price = entry_price - float(self.stop_loss_ticks * self.price_tick)
        logger.info(
            _("({}) 更新入场价: {:.2f}, 新目标: Target={:.2f}, Stop={:.2f}").format(
                context, 
                self.entry_price, 
                self.target_price, 
                self.stop_price
            )
        )

    def _reset_targets(self, context: str) -> None:
        """Resets entry price and targets."""
        self.entry_price = 0.0
        self.target_price = 0.0
        self.stop_price = 0.0
        logger.info(_("({}) 重置入场价和目标.").format(context))