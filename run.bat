@echo off
title run
:: ´òÓ¡ĞÅÏ¢
call .venv\Scripts\activate
start 1_run_market_gateway.bat
start 2_run_order_gateway.bat
start 3_run_strategy_subscriber.bat
start 4_run_risk_manager.bat
start 5_run_data_recorder.bat
echo finished.
exit