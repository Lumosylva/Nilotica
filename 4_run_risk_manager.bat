@echo off
setlocal enabledelayedexpansion
title run_risk_manager
:: ��ӡ��Ϣ
call .venv\Scripts\activate
python zmq_services/run_risk_manager.py
pause