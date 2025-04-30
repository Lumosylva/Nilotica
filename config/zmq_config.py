# ZeroMQ Configuration
MARKET_DATA_PUB_ADDRESS = "tcp://*:5555" # 行情发布地址
MARKET_DATA_REP_ADDRESS = "tcp://*:2015"
# ORDER_REQUEST_PULL_URL = "tcp://*:5556" # 订单请求接收地址 (PULL)
ORDER_GATEWAY_PUB_ADDRESS = "tcp://*:5557"   # 订单/成交回报发布地址 (PUB)

# Risk Management Configuration
MAX_POSITION_LIMITS = {
    "SA509.CZCE": 5,  # SA509.CZCE 最大持仓量（多头或空头绝对值）
    "rb2510.SHFE": 10, # rb2510.SHFE 最大持仓量
    "MA509.CZCE": 5,  # SA505.CZCE 最大持仓量（多头或空头绝对值）
    # 可以添加更多合约的限制
}

# Data Recording Configuration
# Corrected path to be relative to project root or absolute
# Assuming the script running this config is in the project root or similar context
# If run from zmq_services, the original path might be intended
# For broader usability, making it relative to project root is often better.
# Option 1: Keep original (relative to zmq_services) - uncomment below if needed
# DATA_RECORDING_PATH = "zmq_services/recorded_data/" 
# Option 2: Make relative to project root (assuming config package is at root level)
import os
_project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) 
DATA_RECORDING_PATH = os.path.join(_project_root, "zmq_services", "recorded_data")
# Option 3: Use absolute path if needed
# DATA_RECORDING_PATH = "/path/to/your/project/zmq_services/recorded_data/"

# Backtesting Configuration
BACKTEST_DATA_SOURCE_PATH = DATA_RECORDING_PATH # 回测使用的数据源路径 (默认使用记录的数据)
BACKTEST_DATA_PUB_URL = "tcp://*:5560"          # 回测行情发布地址 (PUB)
BACKTEST_ORDER_REPORT_PUB_URL = "tcp://*:5561"   # 回测订单/成交回报发布地址 (PUB)
BACKTEST_ORDER_REQUEST_PULL_URL = "tcp://*:5562" # 回测订单请求接收地址 (PULL)

# 需要订阅的合约列表
SUBSCRIBE_SYMBOLS = ["SA509.CZCE", "rb2510.SHFE", "MA509.CZCE"] # 示例合约，请修改为您需要的

# --- Strategy Specific Parameters (SA509 Example) ---
SA509_ENTRY_THRESHOLD = 1330.0       # SA509 入场价格阈值 (示例)
SA509_PROFIT_TARGET_TICKS = 10       # SA509 止盈目标 (跳动点数)
SA509_STOP_LOSS_TICKS = 5          # SA509 止损距离 (跳动点数)
SA509_PRICE_TICK = 1.0               # SA509 最小价格变动 (示例，请根据实际合约修改)
ORDER_COOL_DOWN_SECONDS = 2        # 同一合约连续下单的冷却时间（秒）

# --- Enhancements for Risk Manager --- 

# Command channel for Order Gateway (e.g., Risk Manager sends cancel orders)
ORDER_GATEWAY_REP_ADDRESS = "tcp://*:5558"  # 订单网关指令接收地址 (建议 REQ/REP)

# (Optional) Risk Alert Publishing
RISK_ALERT_PUB_URL = "tcp://*:5559"        # 风险管理警报发布地址 (PUB)

# New Risk Parameters (Examples, adjust as needed)
MAX_PENDING_ORDERS_PER_CONTRACT = 5       # 单合约最大挂单笔数
GLOBAL_MAX_PENDING_ORDERS = 20            # 全局最大挂单笔数
MARGIN_USAGE_LIMIT = 0.8                  # 保证金占用率上限 (80%)
# Add more rules like order rate limits etc. 

# --- Trading Session Configuration --- 
# List of (start_time, end_time) tuples in HH:MM format
# Used by Risk Manager to check market data timeliness during active hours
FUTURES_TRADING_SESSIONS = [
    ("09:00", "11:00"), # Morning session 1
    ("10:15", "11:30"), # Morning session 2 (adjust if needed)
    ("13:30", "15:00"), # Afternoon session
    ("21:00", "23:00"), # Evening session
] 

# --- Gateway Service Constants ---

# Heartbeat interval for RPC clients/servers (if applicable, usually handled by RPC lib)
# HEARTBEAT_INTERVAL_S = 1.0 

# Batch size for processing messages in publisher queue
PUBLISH_BATCH_SIZE = 1000

# Timezone for timestamping events (Requires Python 3.9+ and zoneinfo)
try:
    from zoneinfo import ZoneInfo
    CHINA_TZ = ZoneInfo("Asia/Shanghai")
except ImportError:
    print("Warning: 'zoneinfo' module not found. Timestamps might lack proper timezone info. Requires Python 3.9+.")
    # Fallback or error handling if needed
    CHINA_TZ = None 

# Log level for gateways (e.g., "INFO", "DEBUG")
# GATEWAY_LOG_LEVEL = "INFO" 

# --- Engine/Communication Parameters ---
MARKET_DATA_TIMEOUT_SECONDS = 30.0  # (Strategy Engine) 行情数据被视为超时的秒数
INITIAL_TICK_GRACE_PERIOD_MULTIPLIER = 2.0 # (Strategy Engine) 初始连接后，判断行情超时的宽限期倍数 (乘以 MARKET_DATA_TIMEOUT_SECONDS)
PING_INTERVAL_SECONDS = 5.0       # (Strategy Engine) 发送 PING 到订单网关的间隔秒数
PING_TIMEOUT_MS = 2500            # (Strategy Engine) 等待 PING 回复的超时毫秒数
RPC_TIMEOUT_MS = 3000             # (Strategy Engine) 默认 RPC 请求的超时毫秒数
RPC_RETRIES = 1                   # (Strategy Engine) RPC 请求失败后的重试次数
HEARTBEAT_TIMEOUT_SECONDS = 10.0    # (Strategy Engine) 接收订单网关心跳的超时秒数