@echo off
title Run data recorder
cd ..
call .venv\Scripts\activate
:: 对今天的数据以最大速度进行回测
python -m zmq_services.backtester.run_backtest
pause