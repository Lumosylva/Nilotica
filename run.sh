#!/bin/bash
# Start All Services
# Activate Python
source ".venv/bin/activate"
echo "Start all services......"
# Get current path
current_path=$(cd "$(dirname "$0")" && pwd)
echo "Current path: $current_path"
"$current_path/bat/1_run_market_gateway.bat" &
"$current_path/bat/2_run_order_gateway.bat" &
# Wait for the market gateway and order execution gateway to be fully started
sleep 14
"$current_path/bat/3_run_data_recorder.bat" &
"$current_path/bat/4_run_risk_manager.bat" &
"$current_path/bat/5_run_strategy_engine.bat" &
echo "Finished."
wait
exit 0