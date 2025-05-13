@echo off
title run_data_recorder
:: 打印信息
call .venv\Scripts\activate
echo %CD%
:: 对今天的数据以最大速度进行回测
python -m zmq_services.backtester.run_backtest
pause