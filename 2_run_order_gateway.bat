@echo off
setlocal enabledelayedexpansion
title run_order_gateway
:: ��ӡ��Ϣ
call .venv\Scripts\activate
python zmq_services/run_order_gateway.py
pause