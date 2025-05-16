@echo off
title Run strategy engine
cd ..
call .venv\Scripts\activate
python -m zmq_services.run_strategy_engine
pause