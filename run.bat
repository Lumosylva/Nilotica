@echo off
title Start all services
REM Start All Services
:: Activate Python
call .venv\Scripts\activate
echo Start all services......
set current_path=%~dp0
echo Current path: %current_path%
start %current_path%/bat/1_run_market_gateway.bat
start %current_path%/bat/2_run_order_gateway.bat
REM Wait for the market gateway and order execution gateway to be fully started
ping 127.0.0.1 -n 14 >nul
start %current_path%/bat/3_run_data_recorder.bat
start %current_path%/bat/4_run_risk_manager.bat
start %current_path%/bat/5_run_strategy_engine.bat
echo Finished.
exit