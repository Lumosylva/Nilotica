@echo off
title Run order gateway
cd ..
call .venv\Scripts\activate
python -m zmq_services.run_order_gateway
pause