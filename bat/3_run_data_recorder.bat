@echo off
title run_data_recorder
:: ��ӡ��Ϣ
call .venv\Scripts\activate
echo %CD%
python -m zmq_services.run_data_recorder
pause