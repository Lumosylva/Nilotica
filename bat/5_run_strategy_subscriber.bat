@echo off
title run_strategy_subscriber
:: ¥Ú”°–≈œ¢
call .venv\Scripts\activate
echo %CD%
python zmq_services/run_strategy_subscriber.py
pause