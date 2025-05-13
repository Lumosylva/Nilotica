@echo off
title run_risk_manager
:: ¥Ú”°–≈œ¢
call .venv\Scripts\activate
echo %CD%
python -m zmq_services.run_risk_manager
pause