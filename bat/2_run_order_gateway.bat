@echo off
title run_order_gateway
:: ¥Ú”°–≈œ¢
call .venv\Scripts\activate
echo %CD%
python zmq_services/run_order_gateway.py
pause