@echo off
title run_data_recorder
:: 打印信息
call .venv\Scripts\activate
:: 对今天的数据运行回测
python zmq_services/backtester/run_backtest.py
pause