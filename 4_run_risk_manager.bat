@echo off
setlocal enabledelayedexpansion
title run_risk_manager
:: ¥Ú”°–≈œ¢
call .venv\Scripts\activate
python zmq_services/run_risk_manager.py
pause