@echo off
title Run data recorder
cd ..
call .venv\Scripts\activate
:: �Խ��������������ٶȽ��лز�
python -m zmq_services.backtester.run_backtest
pause