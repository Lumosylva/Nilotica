@echo off
title run_data_recorder
:: ��ӡ��Ϣ
call .venv\Scripts\activate
echo %CD%
:: �Խ��������������ٶȽ��лز�
python -m zmq_services.backtester.run_backtest
pause