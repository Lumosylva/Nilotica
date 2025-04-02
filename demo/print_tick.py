#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : print_tick
@Date       : 2025/4/1 21:42
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 获取tick数据的示例
"""
import logging
from time import sleep

from vnpy.event import Event, EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.event import EVENT_TICK, EVENT_CONTRACT
from vnpy.trader.object import TickData, ContractData, SubscribeRequest
from vnpy.trader.utility import load_json
from vnpy_ctp import CtpGateway


logging.getLogger("TickPrinter").setLevel(logging.INFO)

class TickPrinter:
    def __init__(self):
        self.main_engine: MainEngine | None = None

    def run(self):
        event_engine = EventEngine()

        event_engine.register(EVENT_CONTRACT, self.process_contract_event)
        event_engine.register(EVENT_TICK, self.process_tick_event)

        self.main_engine = MainEngine(event_engine)
        self.main_engine.add_gateway(CtpGateway)

        try:
            setting = load_json("connect_ctp.json")
            self.main_engine.connect(setting, "CTP")
        except Exception as e:
            print(f"连接失败: {e}")
            return

        try:
            while True:
                sleep(2)
        except KeyboardInterrupt:
            print("Exit")
        finally:
            if self.main_engine:
                self.main_engine.close()


    def process_contract_event(self, event: Event) -> None:
        try:
            contract: ContractData = event.data
            req = SubscribeRequest(symbol=contract.symbol, exchange=contract.exchange)

            if self.main_engine:
                self.main_engine.subscribe(req, contract.gateway_name)
        except Exception as e:
            logging.error(f"处理合约事件失败: {e}")


    @staticmethod
    def process_tick_event(event: Event) -> None:
        try:
            tick: TickData = event.data
            logging.info(f"Tick Data: {tick.name}, {tick.symbol}, {tick.last_price}, {tick.datetime}")
            print(tick.name, tick.symbol, tick.last_price, tick.datetime)
        except Exception as e:
            logging.error(f"处理Tick事件失败: {e}")


if __name__ == "__main__":
    printer = TickPrinter()
    printer.run()
