#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : backtesting_no_ui
@Date       : 2025/4/2 16:48
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: description
"""
from datetime import datetime

from vnpy.trader.constant import Interval
from vnpy_ctastrategy.backtesting import BacktestingEngine
from vnpy_ctastrategy.strategies.test_strategy import TestStrategy

engine = BacktestingEngine()

engine.set_parameters(
    vt_symbol="SA505.CZCE",
    interval=Interval.MINUTE,
    start=datetime(2025, 3, 1),
    end=datetime(2025, 4, 1),
    rate=0.3 / 10000,
    slippage=0.2,
    size=300,
    pricetick=0.2,
    capital=1_000_000,
)

engine.add_strategy(TestStrategy, {})

engine.load_data()

engine.run_backtesting()

df = engine.calculate_result()

engine.calculate_statistics()

