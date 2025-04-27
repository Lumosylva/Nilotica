@echo off
title run_risk_manager
:: ¥Ú”°–≈œ¢
call .venv\Scripts\activate
echo %CD%
python zmq_services/run_risk_manager.py
pause