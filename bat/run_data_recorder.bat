@echo off
title Run data recorder
cd ..
call .venv\Scripts\activate
python -m zmq_services.run_data_recorder
pause