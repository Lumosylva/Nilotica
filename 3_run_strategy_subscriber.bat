@echo off
setlocal enabledelayedexpansion
title run_strategy_subscriber
:: ��ӡ��Ϣ
call .venv\Scripts\activate
python zmq_services/run_strategy_subscriber.py
pause