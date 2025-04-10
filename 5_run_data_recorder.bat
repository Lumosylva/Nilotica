@echo off
setlocal enabledelayedexpansion
title run_data_recorder
:: ¥Ú”°–≈œ¢
call .venv\Scripts\activate
python zmq_services/run_data_recorder.py
pause