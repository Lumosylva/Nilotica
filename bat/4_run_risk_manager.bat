@echo off
title Run risk manager
call .venv\Scripts\activate
python -m zmq_services.run_risk_manager
pause