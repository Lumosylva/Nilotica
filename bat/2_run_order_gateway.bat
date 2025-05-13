@echo off
title run_order_gateway
:: ¥Ú”°–≈œ¢
call .venv\Scripts\activate
echo %CD%
python -m zmq_services.run_order_gateway
pause