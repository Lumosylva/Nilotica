# ZeroMQ Configuration
MARKET_DATA_PUB_URL = "tcp://*:5555" # 行情发布地址
ORDER_REQUEST_PULL_URL = "tcp://*:5556" # 订单请求接收地址 (PULL)
ORDER_REPORT_PUB_URL = "tcp://*:5557"   # 订单/成交回报发布地址 (PUB)

# CTP Gateway Configuration (与 vnpy 配置文件类似，需要您填写实际信息)
CTP_TD_ADDRESS = "tcp://180.168.146.187:10130"  # CTP 交易服务器地址
CTP_MD_ADDRESS = "tcp://180.168.146.187:10131"  # CTP 行情服务器地址
# CTP_TD_ADDRESS = "tcp://180.168.146.187:10201"  # CTP 交易服务器地址
# CTP_MD_ADDRESS = "tcp://180.168.146.187:10211"  # CTP 行情服务器地址
CTP_USER_ID = "160219"             # CTP 用户ID
CTP_PASSWORD = "Mdd103010$"           # CTP 密码
CTP_BROKER_ID = "9999"         # CTP Broker ID
CTP_PRODUCT_INFO = "simnow_client_test"       # 产品信息 (可选)
CTP_AUTH_CODE = "0000000000000000"           # 认证码 (可选)

# Risk Management Configuration
MAX_POSITION_LIMITS = {
    "SA505.CZCE": 5,  # SA505.CZCE 最大持仓量（多头或空头绝对值）
    "rb2510.SHFE": 10, # rb2510.SHFE 最大持仓量
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
SUBSCRIBE_SYMBOLS = ["SA505.CZCE", "rb2510.SHFE"] # 示例合约，请修改为您需要的

# --- Enhancements for Risk Manager --- 

# Command channel for Order Gateway (e.g., Risk Manager sends cancel orders)
ORDER_GATEWAY_COMMAND_URL = "tcp://*:5558"  # 订单网关指令接收地址 (建议 REQ/REP)

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
    # ("10:15", "11:30"), # Morning session 2 (adjust if needed)
    ("13:30", "15:00"), # Afternoon session
    ("21:00", "23:00"), # Evening session
] 