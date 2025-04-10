@echo off
setlocal enabledelayedexpansion
title run_market_gateway
:: ¥Ú”°–≈œ¢
call .venv\Scripts\activate
python zmq_services/run_market_gateway.py
pause