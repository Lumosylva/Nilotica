@echo off
setlocal enabledelayedexpansion
title run_market_gateway
:: ��ӡ��Ϣ
call .venv\Scripts\activate
python zmq_services/run_market_gateway.py
pause