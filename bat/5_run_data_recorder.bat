@echo off
title run_data_recorder
:: ��ӡ��Ϣ
call .venv\Scripts\activate
python zmq_services/run_data_recorder.py
pause