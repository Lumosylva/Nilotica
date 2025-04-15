# ZeroMQ Configuration
MARKET_DATA_PUB_URL = "tcp://*:5555" # 行情发布地址
ORDER_REQUEST_PULL_URL = "tcp://*:5556" # 订单请求接收地址 (PULL)
ORDER_REPORT_PUB_URL = "tcp://*:5557"   # 订单/成交回报发布地址 (PUB)

# CTP Gateway Configuration (与 vnpy 配置文件类似，需要您填写实际信息)
# CTP_TD_ADDRESS = "tcp://180.168.146.187:10130"  # CTP 交易服务器地址
# CTP_MD_ADDRESS = "tcp://180.168.146.187:10131"  # CTP 行情服务器地址
CTP_TD_ADDRESS = "tcp://180.168.146.187:10201"  # CTP 交易服务器地址
CTP_MD_ADDRESS = "tcp://180.168.146.187:10211"  # CTP 行情服务器地址
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
DATA_RECORDING_PATH = "zmq_services/recorded_data/" # 数据文件存储路径

# Backtesting Configuration
BACKTEST_DATA_SOURCE_PATH = DATA_RECORDING_PATH # 回测使用的数据源路径 (默认使用记录的数据)
BACKTEST_DATA_PUB_URL = "tcp://*:5560"          # 回测行情发布地址 (PUB)
BACKTEST_ORDER_REPORT_PUB_URL = "tcp://*:5561"   # 回测订单/成交回报发布地址 (PUB)
BACKTEST_ORDER_REQUEST_PULL_URL = "tcp://*:5562" # 回测订单请求接收地址 (PULL)

# 需要订阅的合约列表
SUBSCRIBE_SYMBOLS = ["SA505.CZCE", "rb2510.SHFE"] # 示例合约，请修改为您需要的 