@echo off
title run_strategy_subscriber
:: ��ӡ��Ϣ
call .venv\Scripts\activate
echo %CD%
python -m zmq_services.run_strategy_engine
pause