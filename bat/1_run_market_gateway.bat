@echo off
title run_market_gateway
:: ��ӡ��Ϣ
call .venv\Scripts\activate
echo %CD%
python zmq_services/run_market_gateway.py
pause