@echo off
title run_data_recorder
:: ��ӡ��Ϣ
call .venv\Scripts\activate
:: �Խ�����������лز�
python zmq_services/backtester/run_backtest.py
pause