@echo off
title Start all services
:: activate Python
call .venv\Scripts\activate
echo Start all services......
set current_path=%~dp0
echo Current path: %current_path%
start %current_path%/bat/1_run_market_gateway.bat
start %current_path%/bat/2_run_order_gateway.bat
REM 等待行情网关和订单执行网关完全启动后再启动其他服务
ping 127.0.0.1 -n 12 >nul
start %current_path%/bat/3_run_strategy_subscriber.bat
start %current_path%/bat/4_run_risk_manager.bat
start %current_path%/bat/5_run_data_recorder.bat
echo Finished.
exit