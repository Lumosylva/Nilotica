@echo off
title run_data_recorder
:: ��ӡ��Ϣ
call .venv\Scripts\activate
echo %CD%
:: �Խ�����������лز�
python zmq_services/backtester/run_backtest.py
pause