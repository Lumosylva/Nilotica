@echo off
title run_order_gateway
:: ��ӡ��Ϣ
call .venv\Scripts\activate
echo %CD%
python zmq_services/run_order_gateway.py
pause