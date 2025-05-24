'''
Simple standalone script to test TTS MdApi connection.
'''
import time
from pathlib import Path
from vnpy_tts.api import MdApi
import os # Ensure os is imported
import sys # Ensure sys is imported

# --- Print CWD and sys.path for diagnostics ---
print(f"--- Diagnostics for test_tts_md_connection.py ---")
print(f"Current Working Directory (CWD): {os.getcwd()}")
print(f"sys.path:")
for p in sys.path:
    print(f"  {p}")
print(f"---------------------------------------------------")
# --- End diagnostics ---


# --- 配置参数 (请根据你的实际情况修改) ---
# 请确保这些信息与你用于 vnpy_tts 环境的 brokers.json 中的 tts7x24 配置一致
TTS_MD_ADDRESS = "tcp://121.37.80.177:20004" # TTS 行情前置地址
TTS_BROKER_ID = "9999"                       # 你的 Broker ID
TTS_USER_ID = "12733"                        # 你的 User ID
TTS_PASSWORD = "123456"           # 你的 TTS 密码

# API实例的流文件存放路径 (确保此目录存在或API有权限创建)
FLOW_PATH = Path("tts_md_flow/")
FLOW_PATH.mkdir(exist_ok=True) # 确保目录存在

REQ_ID = 0

class MyMdApi(MdApi):
    def __init__(self):
        super().__init__()
        self.connected = False
        self.logged_in = False
        self.req_id = 0

    def onFrontConnected(self) -> None:
        self.connected = True
        print(f"MyMdApi: [{time.strftime('%Y-%m-%d %H:%M:%S')}] 行情服务器前置已连接.")
        print(f"MyMdApi: [{time.strftime('%Y-%m-%d %H:%M:%S')}] 准备登录...")
        self.login()

    def onFrontDisconnected(self, reason: int) -> None:
        self.connected = False
        self.logged_in = False # 通常断开连接意味着也已登出
        print(f"MyMdApi: [{time.strftime('%Y-%m-%d %H:%M:%S')}] 行情服务器前置已断开. 原因代码: {reason} (十六进制: {reason:#0x})")

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        if error and error.get("ErrorID", 0) != 0:
            print(f"MyMdApi: [{time.strftime('%Y-%m-%d %H:%M:%S')}] 登录失败. 请求ID: {reqid}, ErrorID: {error.get('ErrorID')}, ErrorMsg: {error.get('ErrorMsg', '').encode('gbk').decode('gbk', 'ignore')}")
            self.logged_in = False
        else:
            self.logged_in = True
            print(f"MyMdApi: [{time.strftime('%Y-%m-%d %H:%M:%S')}] 登录成功. 请求ID: {reqid}, UserID: {data.get('UserID')}")
            # 登录成功后可以尝试订阅行情等操作
            # self.subscribe_market_data("SA509.CZCE") # 示例订阅

    def onRspError(self, error: dict, reqid: int, last: bool) -> None:
        print(f"MyMdApi: [{time.strftime('%Y-%m-%d %H:%M:%S')}] 收到错误回报. 请求ID: {reqid}, ErrorID: {error.get('ErrorID')}, ErrorMsg: {error.get('ErrorMsg', '').encode('gbk').decode('gbk', 'ignore')}")

    def onRspSubMarketData(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        if error and error.get("ErrorID", 0) != 0:
            print(f"MyMdApi: [{time.strftime('%Y-%m-%d %H:%M:%S')}] 订阅行情失败. 错误合约: {data.get('InstrumentID')}, ErrorID: {error.get('ErrorID')}, ErrorMsg: {error.get('ErrorMsg', '').encode('gbk').decode('gbk', 'ignore')}")
        else:
            print(f"MyMdApi: [{time.strftime('%Y-%m-%d %H:%M:%S')}] 订阅行情成功. 合约: {data.get('InstrumentID')}")

    def onRtnDepthMarketData(self, data: dict) -> None:
        # 简单打印收到的行情，避免刷屏，可以只打印特定合约或计数
        instrument_id = data.get("InstrumentID", "N/A")
        last_price = data.get("LastPrice", 0.0)
        update_time = data.get("UpdateTime", "HH:MM:SS")
        print(f"MyMdApi: [{time.strftime('%Y-%m-%d %H:%M:%S')}] 收到行情: {instrument_id}, 最新价: {last_price}, 时间: {update_time}.{data.get('UpdateMillisec')}")

    def login(self):
        if not self.connected:
            print(f"MyMdApi: [{time.strftime('%Y-%m-%d %H:%M:%S')}] 尚未连接到前置，无法登录.")
            return
        
        self.req_id += 1
        req = {
            "BrokerID": TTS_BROKER_ID,
            "UserID": TTS_USER_ID,
            "Password": TTS_PASSWORD,
        }
        print(f"MyMdApi: [{time.strftime('%Y-%m-%d %H:%M:%S')}] 发送登录请求 (ReqID: {self.req_id}): {req}")
        return_code = self.reqUserLogin(req, self.req_id)
        if return_code != 0:
            print(f"MyMdApi: [{time.strftime('%Y-%m-%d %H:%M:%S')}] reqUserLogin 调用失败，返回代码: {return_code}")


    def subscribe_market_data(self, symbol: str):
        if not self.logged_in:
            print(f"MyMdApi: [{time.strftime('%Y-%m-%d %H:%M:%S')}]尚未登录，无法订阅行情 {symbol}")
            return
        
        self.req_id += 1
        # 注意：TTS API 的 subscribeMarketData 可能只需要合约代码列表
        # 具体参数请参考 vnpy_tts 的实际用法或 TTS API 文档
        print(f"MyMdApi: [{time.strftime('%Y-%m-%d %H:%M:%S')}] 发送行情订阅请求 (ReqID: {self.req_id}) for {symbol}")
        return_code = self.subscribeMarketData(symbol) # 直接传递symbol字符串
        if return_code != 0:
            print(f"MyMdApi: [{time.strftime('%Y-%m-%d %H:%M:%S')}] subscribeMarketData 调用失败 for {symbol}，返回代码: {return_code}")

def main():
    print(f"开始TTS行情API连接测试 (PID: {os.getpid()})...")
    md_api = MyMdApi()

    print(f"创建 MdApi 实例，流路径: {FLOW_PATH.resolve()}")
    # 第一个参数是流文件路径，通常需要 .encode('gbk') 但在 Path 对象下可能直接用 str
    # 如果是 Windows 且路径包含中文，需要特别注意编码
    # Ensure the flow path string is passed correctly
    flow_path_api_str = str(FLOW_PATH.resolve())
    print(f"Calling createFtdcMdApi with flow path: {flow_path_api_str}")
    md_api.createFtdcMdApi(flow_path_api_str)

    print(f"注册行情前置地址: {TTS_MD_ADDRESS}")
    md_api.registerFront(TTS_MD_ADDRESS) # TTS API通常只接受一个字符串参数

    print("初始化行情API连接...")
    md_api.init()

    print("行情API已初始化，等待回调。按 Ctrl+C 退出。")

    try:
        while True:
            time.sleep(1) # 主线程保持运行以接收回调
            # 可以在这里添加一些状态检查或定时任务
            if md_api.logged_in and not hasattr(md_api, 'subscribed_once'):
                md_api.subscribe_market_data("SA509")
                md_api.subscribed_once = True # 避免重复订阅

    except KeyboardInterrupt:
        print("\n检测到 Ctrl+C，正在停止...")
    finally:
        print("释放 MdApi 实例...")
        md_api.release() # 或者 md_api.exit()，具体看 vnpy_tts 的 API
        print("测试脚本结束.")

if __name__ == "__main__":
    # import os # os is already imported at the top
    main() 