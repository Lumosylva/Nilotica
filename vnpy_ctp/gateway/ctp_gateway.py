import configparser
import re
import sys
import traceback
from datetime import datetime
from pathlib import Path
from time import sleep

from config.constants.path import GlobalPath
from utils.file_helper import load_json_file, write_json_file, read_ini_file
from vnpy.event import Event, EventEngine
from vnpy.trader.constant import Direction, Exchange, Offset, OptionType, OrderType, Product, Status
from vnpy.trader.event import EVENT_TIMER
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    AccountData,
    CancelRequest,
    ContractData,
    OrderData,
    OrderRequest,
    PositionData,
    SubscribeRequest,
    TickData,
    TradeData,
)
from vnpy.trader.utility import ZoneInfo, get_folder_path
from .ctp_gateway_helper import build_contract

from ..api import (
    THOST_FTDC_TC_GFD,
    THOST_FTDC_TC_IOC,
    THOST_FTDC_VC_AV,
    THOST_FTDC_VC_CV,
    MdApi,
    TdApi,
    THOST_FTDC_AF_Delete,
    THOST_FTDC_CC_Immediately,
    THOST_FTDC_CP_CallOptions,
    THOST_FTDC_CP_PutOptions,
    THOST_FTDC_D_Buy,
    THOST_FTDC_D_Sell,
    THOST_FTDC_FCC_NotForceClose,
    THOST_FTDC_HF_Speculation,
    THOST_FTDC_OF_Open,
    THOST_FTDC_OFEN_Close,
    THOST_FTDC_OFEN_CloseToday,
    THOST_FTDC_OFEN_CloseYesterday,
    THOST_FTDC_OPT_AnyPrice,
    THOST_FTDC_OPT_LimitPrice,
    THOST_FTDC_OST_AllTraded,
    THOST_FTDC_OST_Canceled,
    THOST_FTDC_OST_NoTradeQueueing,
    THOST_FTDC_OST_PartTradedQueueing,
    THOST_FTDC_OST_Unknown,
    THOST_FTDC_PC_Combination,
    THOST_FTDC_PC_Futures,
    THOST_FTDC_PC_Options,
    THOST_FTDC_PC_SpotOption,
    THOST_FTDC_PD_Long,
    THOST_FTDC_PD_Short,
)

from utils.i18n import _


# 委托状态映射
STATUS_CTP2VT: dict[str, Status] = {
    THOST_FTDC_OST_NoTradeQueueing: Status.NOTTRADED,
    THOST_FTDC_OST_PartTradedQueueing: Status.PARTTRADED,
    THOST_FTDC_OST_AllTraded: Status.ALLTRADED,
    THOST_FTDC_OST_Canceled: Status.CANCELLED,
    THOST_FTDC_OST_Unknown: Status.SUBMITTING
}

# 多空方向映射
DIRECTION_VT2CTP: dict[Direction, str] = {
    Direction.LONG: THOST_FTDC_D_Buy,
    Direction.SHORT: THOST_FTDC_D_Sell
}
DIRECTION_CTP2VT: dict[str, Direction] = {v: k for k, v in DIRECTION_VT2CTP.items()}
DIRECTION_CTP2VT[THOST_FTDC_PD_Long] = Direction.LONG
DIRECTION_CTP2VT[THOST_FTDC_PD_Short] = Direction.SHORT

# 委托类型映射
ORDERTYPE_VT2CTP: dict[OrderType, tuple] = {
    OrderType.LIMIT: (THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_GFD, THOST_FTDC_VC_AV),
    OrderType.MARKET: (THOST_FTDC_OPT_AnyPrice, THOST_FTDC_TC_GFD, THOST_FTDC_VC_AV),
    OrderType.FAK: (THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_IOC, THOST_FTDC_VC_AV),
    OrderType.FOK: (THOST_FTDC_OPT_LimitPrice, THOST_FTDC_TC_IOC, THOST_FTDC_VC_CV),
}
ORDERTYPE_CTP2VT: dict[tuple, OrderType] = {v: k for k, v in ORDERTYPE_VT2CTP.items()}

# 开平方向映射
OFFSET_VT2CTP: dict[Offset, str] = {
    Offset.OPEN: THOST_FTDC_OF_Open,
    Offset.CLOSE: THOST_FTDC_OFEN_Close,
    Offset.CLOSETODAY: THOST_FTDC_OFEN_CloseToday,
    Offset.CLOSEYESTERDAY: THOST_FTDC_OFEN_CloseYesterday,
}
OFFSET_CTP2VT: dict[str, Offset] = {v: k for k, v in OFFSET_VT2CTP.items()}

# 交易所映射
EXCHANGE_CTP2VT: dict[str, Exchange] = {
    "CFFEX": Exchange.CFFEX,
    "SHFE": Exchange.SHFE,
    "CZCE": Exchange.CZCE,
    "DCE": Exchange.DCE,
    "INE": Exchange.INE,
    "GFEX": Exchange.GFEX
}

# 产品类型映射
PRODUCT_CTP2VT: dict[str, Product] = {
    THOST_FTDC_PC_Futures: Product.FUTURES,
    THOST_FTDC_PC_Options: Product.OPTION,
    THOST_FTDC_PC_SpotOption: Product.OPTION,
    THOST_FTDC_PC_Combination: Product.SPREAD
}

# 期权类型映射
OPTIONTYPE_CTP2VT: dict[str, OptionType] = {
    THOST_FTDC_CP_CallOptions: OptionType.CALL,
    THOST_FTDC_CP_PutOptions: OptionType.PUT
}

# 其他常量
MAX_FLOAT = sys.float_info.max                  # 浮点数极限值
CHINA_TZ = ZoneInfo("Asia/Shanghai")       # 中国时区

# 合约数据全局缓存字典
symbol_contract_map: dict[str, ContractData] = {}


class CtpGateway(BaseGateway):
    """
    VeighNa 用于对接期货CTP柜台的交易接口。
    """

    default_name: str = "CTP"

    default_setting: dict[str, str] = {
        "userid": "",
        "password": "",
        "broker_id": "",
        "td_address": "",
        "md_address": "",
        "appid": "",
        "auth_code": ""
    }

    exchanges: list[str] = list(EXCHANGE_CTP2VT.values())

    def __init__(self, event_engine: EventEngine, gateway_name: str) -> None:
        """构造函数"""
        super().__init__(event_engine, gateway_name)

        self.query_functions = None
        self.td_api: CtpTdApi = CtpTdApi(self)
        self.md_api: CtpMdApi = CtpMdApi(self)

        self.count: int = 0

    def connect(self, setting: dict) -> None:
        """连接交易接口"""
        userid: str = setting["userid"]  # 用户名
        password: str = setting["password"]  # 密码
        broker_id: str = setting["broker_id"]  # 经纪商代码
        td_address: str = setting["td_address"]  # 交易服务器
        md_address: str = setting["md_address"]  # 行情服务器
        appid: str = setting["appid"]  # 产品名称
        auth_code: str = setting["auth_code"]  # 授权编码

        if (
            (not td_address.startswith("tcp://"))
            and (not td_address.startswith("ssl://"))
            and (not td_address.startswith("socks"))
        ):
            td_address = "tcp://" + td_address

        if (
            (not md_address.startswith("tcp://"))
            and (not md_address.startswith("ssl://"))
            and (not md_address.startswith("socks"))
        ):
            md_address = "tcp://" + md_address

        self.td_api.connect(td_address, userid, password, broker_id, auth_code, appid)
        self.md_api.connect(md_address, userid, password, broker_id)

        self.init_query()

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.md_api.subscribe(req)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        return self.td_api.send_order(req)

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        self.td_api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        self.td_api.query_account()

    def query_position(self) -> None:
        """查询持仓"""
        self.td_api.query_position()

    def close(self) -> None:
        """关闭接口"""
        self.td_api.close()
        self.md_api.close()

    def write_error(self, msg: str, error: dict) -> None:
        """输出错误信息日志"""
        error_id: int = error["ErrorID"]
        error_msg: str = error["ErrorMsg"]

        log_msg: str = f"{_(msg)}，{_('代码')}：{error_id}，{_('信息')}：{error_msg}"
        self.write_log(log_msg)

    def process_timer_event(self, event: Event) -> None:
        """定时事件处理"""
        self.count += 1
        if self.count < 2:
            return
        self.count = 0

        func = self.query_functions.pop(0)
        func()
        self.query_functions.append(func)

        self.md_api.update_date()

    def init_query(self) -> None:
        """初始化查询任务"""
        self.query_functions: list = [self.query_account, self.query_position]
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)


class CtpMdApi(MdApi):
    """CTP行情API"""

    def __init__(self, gateway: CtpGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: CtpGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.req_id: int = 0

        self.connect_status: bool = False
        self.login_status: bool = False
        self.subscribed: set = set()

        self.userid: str = ""
        self.password: str = ""
        self.broker_id: str = ""

        self.current_date: str = datetime.now().strftime("%Y%m%d")

    def onFrontConnected(self) -> None:
        """服务器连接成功回报"""
        self.gateway.write_log(_("行情服务器连接成功"))
        self.login()

    def onFrontDisconnected(self, reason: int) -> None:
        """服务器连接断开回报"""
        self.login_status = False
        self.gateway.write_log(_("行情服务器连接断开，原因{}").format(reason))

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """用户登录请求回报"""
        if not error["ErrorID"]:
            self.login_status = True
            self.gateway.write_log(_("行情服务器登录成功"))

            for symbol in self.subscribed:
                self.subscribeMarketData(symbol)
        else:
            self.gateway.write_error(_("行情服务器登录失败"), error)

    def onRspError(self, error: dict, reqid: int, last: bool) -> None:
        """请求报错回报"""
        self.gateway.write_error(_("行情接口报错"), error)

    def onRspSubMarketData(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """订阅行情回报"""
        if not error or not error["ErrorID"]:
            return

        self.gateway.write_error(_("行情订阅失败"), error)

    def onRtnDepthMarketData(self, data: dict) -> None:
        """行情数据推送"""
        # 过滤没有时间戳的异常行情数据
        if not data["UpdateTime"]:
            return

        # 过滤还没有收到合约数据前的行情推送
        symbol: str = data["InstrumentID"]
        contract: ContractData = symbol_contract_map.get(symbol, None)
        if not contract:
            return

        # 对大商所的交易日字段取本地日期
        if not data["ActionDay"] or contract.exchange == Exchange.DCE:
            date_str: str = self.current_date
        else:
            date_str = data["ActionDay"]

        timestamp: str = f"{date_str} {data['UpdateTime']}.{data['UpdateMillisec']}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S.%f")
        dt = dt.replace(tzinfo=CHINA_TZ)

        tick: TickData = TickData(
            symbol=symbol,
            exchange=contract.exchange,
            datetime=dt,
            name=contract.name,
            volume=data["Volume"],
            turnover=data["Turnover"],
            open_interest=data["OpenInterest"],
            last_price=adjust_price(data["LastPrice"]),
            limit_up=data["UpperLimitPrice"],
            limit_down=data["LowerLimitPrice"],
            open_price=adjust_price(data["OpenPrice"]),
            high_price=adjust_price(data["HighestPrice"]),
            low_price=adjust_price(data["LowestPrice"]),
            pre_close=adjust_price(data["PreClosePrice"]),
            bid_price_1=adjust_price(data["BidPrice1"]),
            ask_price_1=adjust_price(data["AskPrice1"]),
            bid_volume_1=data["BidVolume1"],
            ask_volume_1=data["AskVolume1"],
            gateway_name=self.gateway_name
        )

        if data["BidVolume2"] or data["AskVolume2"]:
            tick.bid_price_2 = adjust_price(data["BidPrice2"])
            tick.bid_price_3 = adjust_price(data["BidPrice3"])
            tick.bid_price_4 = adjust_price(data["BidPrice4"])
            tick.bid_price_5 = adjust_price(data["BidPrice5"])

            tick.ask_price_2 = adjust_price(data["AskPrice2"])
            tick.ask_price_3 = adjust_price(data["AskPrice3"])
            tick.ask_price_4 = adjust_price(data["AskPrice4"])
            tick.ask_price_5 = adjust_price(data["AskPrice5"])

            tick.bid_volume_2 = data["BidVolume2"]
            tick.bid_volume_3 = data["BidVolume3"]
            tick.bid_volume_4 = data["BidVolume4"]
            tick.bid_volume_5 = data["BidVolume5"]

            tick.ask_volume_2 = data["AskVolume2"]
            tick.ask_volume_3 = data["AskVolume3"]
            tick.ask_volume_4 = data["AskVolume4"]
            tick.ask_volume_5 = data["AskVolume5"]

        self.gateway.on_tick(tick)

    def OnRspUserLogout(self, data: dict, error: dict, reqid: int, last: bool):
        """
        登出请求响应，当 ReqUserLogout 后，该方法被调用。
        :param data: 用户登出请求
        :param error: 响应信息
        :param reqid: 返回用户操作请求的 ID，该 ID 由用户在操作请求时指定。
        :param last: 指示该次返回是否为针对 reqid 的最后一次返回。
        :return: 无
        """
        self.gateway.write_log(_("行情账户：{} 已登出").format(data['UserID']))

    def connect(self, address: str, userid: str, password: str, brokerid: str) -> None:
        """连接服务器"""
        self.userid = userid
        self.password = password
        self.broker_id = brokerid

        # 禁止重复发起连接，会导致异常崩溃
        if not self.connect_status:
            path: Path = get_folder_path(self.gateway_name.lower())
            self.createFtdcMdApi((str(path) + "\\Md").encode("GBK").decode("utf-8"))  # 加上utf-8编码，否则中文路径会乱码

            self.registerFront(address)
            self.init()

            self.connect_status = True

    def login(self) -> None:
        """用户登录"""
        ctp_req: dict = {
            "UserID": self.userid,
            "Password": self.password,
            "BrokerID": self.broker_id
        }

        self.req_id += 1
        self.reqUserLogin(ctp_req, self.req_id)

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        if self.login_status:
            self.subscribeMarketData(req.symbol)
        self.subscribed.add(req.symbol)

    def close(self) -> None:
        """关闭连接"""
        if self.connect_status:
            self.exit()

    def update_date(self) -> None:
        """更新当前日期"""
        self.current_date = datetime.now().strftime("%Y%m%d")


class CtpTdApi(TdApi):
    """CTP交易接口"""

    def __init__(self, gateway: CtpGateway) -> None:
        """构造函数"""
        super().__init__()

        self.gateway: CtpGateway = gateway
        self.gateway_name: str = gateway.gateway_name

        self.req_id: int = 0
        self.order_ref: int = 0

        self.connect_status: bool = False
        self.login_status: bool = False
        self.auth_status: bool = False
        self.login_failed: bool = False
        self.auth_failed: bool = False
        self.contract_inited: bool = False

        self.userid: str = ""
        self.password: str = ""
        self.broker_id: str = ""
        self.auth_code: str = ""
        self.appid: str = ""

        self.frontid: int = 0
        self.sessionid: int = 0
        self.order_data: list[dict] = []
        self.trade_data: list[dict] = []
        self.positions: dict[str, PositionData] = {}
        self.sysid_orderid_map: dict[str, str] = {}
        self.product_info = configparser.ConfigParser()
        self.parser = read_ini_file(GlobalPath.product_info_filepath)
        self.exchange_id_map = load_json_file(GlobalPath.exchange_id_filepath)

    def onFrontConnected(self) -> None:
        """
        服务器连接成功回报
        当客户端与交易托管系统建立起通信连接时（还未登录前），该方法被调用。
        本方法在完成初始化后调用，可以在其中完成用户登录任务。
        :return: 无
        """
        self.gateway.write_log(_("交易服务器连接成功"))
        if self.auth_code:
            self.authenticate()
        else:
            self.login()

    def onFrontDisconnected(self, reason: int) -> None:
        """
        服务器连接断开回报
        当客户端与交易托管系统通信连接断开时，该方法被调用。
        当发生这个情况后，API会自动重新连接，客户端可不做处理。
        自动重连地址，可能是原来注册的地址，也可能是系统支持的其它可用的通信地址，它由程序自动选择。
        注:重连之后需要重新认证、登录
        :param reason: 错误代号，连接断开原因，为10进制值，因此需要转成16进制后再参照下列代码：
                0x1001 网络读失败
                0x1002 网络写失败
                0x2001 接收心跳超时
                0x2002 发送心跳失败
                0x2003 收到错误报文
        :return: 无
        """
        self.login_status = False
        reason_text = ''
        if reason == 0x1001:
            reason_text = '网络读失败'
        elif reason == 0x1002:
            reason_text = '网络写失败'
        elif reason == 0x2001:
            reason_text = '接收心跳超时'
        elif reason == 0x2002:
            reason_text = '发送心跳失败'
        elif reason == 0x2003:
            reason_text = '收到错误报文'
        self.gateway.write_log(_("交易服务器连接断开，原因{}").format(reason_text))

    def onRspAuthenticate(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """
        用户授权验证回报，当执行 ReqAuthenticate 后，该方法被调用
        :param data: 客户端认证响应
        :param error: 响应信息
        :param reqid: 返回用户操作请求的 ID，该 ID 由用户在操作请求时指定。
        :param last: 指示该次返回是否为针对 reqid 的最后一次返回。
        :return: 无
        """
        if not error.get('ErrorID'):
            self.auth_status = True
            self.gateway.write_log(_("交易服务器授权验证成功"))
            self.login()
        else:
            if error.get('ErrorID') == 63:
                self.auth_failed = True
            self.gateway.write_error(_("交易服务器授权验证失败"), error)

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """
        用户登录请求回报，当执行 ReqUserLogin 后，该方法被调用。
        :param data: 用户登录应答
        :param error: 响应信息
        :param reqid: 返回用户操作请求的 ID，该 ID 由用户在操作请求时指定。
        :param last: 指示该次返回是否为针对 reqid 的最后一次返回。
        :return: 无
        """
        if not error.get("ErrorID"):
            self.frontid = data["FrontID"]
            self.sessionid = data["SessionID"]
            self.login_status = True
            self.gateway.write_log(_("交易服务器登录成功"))
            ctp_req: dict = {"BrokerID": self.broker_id, "InvestorID": self.userid}
            self.req_id += 1
            self.reqSettlementInfoConfirm(ctp_req, self.req_id)
        else:
            self.login_failed = True
            self.gateway.write_error(_("交易服务器登录失败"), error)

    def OnRspQryProduct(self, data: dict, error: dict, reqid: int, last: bool):
        """
        查询产品回报，当执行 ReqQryProduct 后，该方法被调用
        :param data: 产品信息
        :param error: 响应信息
        :param reqid: 返回用户操作请求的 ID，该 ID 由用户在操作请求时指定。
        :param last: 指示该次返回是否为针对 reqid 的最后一次返回。
        :return: 无
        """
        if not error.get("ErrorID"):
            sec = data['ProductID']
            opt = '合约乘数'

            # 需要判断section是否存在，如果不存在会报错，option不需要检查是否存在
            if not self.parser.has_section(sec):
                self.parser.add_section(sec)
            self.parser.set(sec, opt, str(data['VolumeMultiple']))

            opt = '最小变动价位'
            self.parser.set(sec, opt, str(data['PriceTick']))

            if last:
                self.parser.write(open(GlobalPath.product_info_filepath, "w", encoding='utf-8'))
                self.gateway.write_log(_("查询产品成功！"))
        else:
            self.gateway.write_error(_("查询产品失败"), error)



    def onRspOrderInsert(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """委托下单失败回报"""
        order_ref: str = data["OrderRef"]
        orderid: str = f"{self.frontid}_{self.sessionid}_{order_ref}"

        symbol: str = data["InstrumentID"]
        contract: ContractData = symbol_contract_map[symbol]

        order: OrderData = OrderData(
            symbol=symbol,
            exchange=contract.exchange,
            orderid=orderid,
            direction=DIRECTION_CTP2VT[data["Direction"]],
            offset=OFFSET_CTP2VT.get(data["CombOffsetFlag"], Offset.NONE),
            price=data["LimitPrice"],
            volume=data["VolumeTotalOriginal"],
            status=Status.REJECTED,
            gateway_name=self.gateway_name
        )
        self.gateway.on_order(order)

        self.gateway.write_error(_("交易委托失败"), error)

    def onRspOrderAction(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """委托撤单失败回报"""
        self.gateway.write_error(_("交易撤单失败"), error)

    def onRspSettlementInfoConfirm(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """
        确认结算单回报，当执行 ReqSettlementInfoConfirm 后，该方法被调用。
        :param data: 投资者结算结果确认信息
        :param error: 响应信息
        :param reqid: 返回用户操作请求的 ID，该 ID 由用户在操作请求时指定。
        :param last: 指示该次返回是否为针对 reqid 的最后一次返回。
        :return: 无
        """
        self.gateway.write_log(_("结算信息确认成功"))
        while True:
            self.req_id += 1
            n: int = self.reqQryInstrument({}, self.req_id)
            if not n:
                break
            else:
                self.gateway.write_log(_("CtpTdApi：reqQryInstrument 失败，代码为 {}，正在重试...").format(n))
                sleep(1)

    def onRspQryInvestorPosition(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """
        持仓查询回报，当执行 ReqQryInvestorPosition 后，该方法被调用
        CTP 系统将持仓明细记录按合约，持仓方向，开仓日期（仅针对上期所，区分昨仓、今仓）进行汇总。
        :param data: 投资者持仓
        :param error: 响应信息
        :param reqid: 返回用户操作请求的 ID，该 ID 由用户在操作请求时指定。
        :param last: 指示该次返回是否为针对 reqid 的最后一次返回。
        :return:
        """
        if last or (error and 'ErrorID' in error and error['ErrorID']):
            if not data:
                return
            else:
                self.gateway.write_log(_("查询持仓成功，所有查询已结束！"))

            # 必须已经收到了合约信息后才能处理
            symbol: str = data["InstrumentID"]
            contract: ContractData = symbol_contract_map.get(symbol, None)

            if contract:
                # 获取之前缓存的持仓数据缓存
                key: str = f"{data['InstrumentID'], data['PosiDirection']}"
                position: PositionData = self.positions.get(key, None)
                if not position:
                    position = PositionData(
                        symbol=data["InstrumentID"],
                        exchange=contract.exchange,
                        direction=DIRECTION_CTP2VT[data["PosiDirection"]],
                        gateway_name=self.gateway_name
                    )
                    self.positions[key] = position

                # 对于上期所昨仓需要特殊处理
                if position.exchange in {Exchange.SHFE, Exchange.INE}:
                    if data["YdPosition"] and not data["TodayPosition"]:
                        position.yd_volume = data["Position"]
                # 对于其他交易所昨仓的计算
                else:
                    position.yd_volume = data["Position"] - data["TodayPosition"]

                # 获取合约的乘数信息
                size: float = contract.size

                # 计算之前已有仓位的持仓总成本
                cost: float = position.price * position.volume * size

                # 累加更新持仓数量和盈亏
                position.volume += data["Position"]
                position.pnl += data["PositionProfit"]

                # 计算更新后的持仓总成本和均价
                if position.volume and size:
                    cost += data["PositionCost"]
                    position.price = cost / (position.volume * size)

                # 更新仓位冻结数量
                if position.direction == Direction.LONG:
                    position.frozen += data["ShortFrozen"]
                else:
                    position.frozen += data["LongFrozen"]

            if last:
                for position in self.positions.values():
                    self.gateway.on_position(position)

                self.positions.clear()


    def OnRspQryInvestorPositionDetail(self, data: dict, error: dict, reqid: int, last: bool):
        """
        请求查询投资者持仓明细响应，当执行 ReqQryInvestorPositionDetail 后，该方法被调用。
        :param data: 投资者持仓明细
        :param error: 响应信息
        :param reqid: 返回用户操作请求的 ID，该 ID 由用户在操作请求时指定。
        :param last: 指示该次返回是否为针对 reqid 的最后一次返回。
        :return: 无
        """
        if not error.get("ErrorID"):
            if last:
                self.gateway.write_log(_("查询持仓明细完成"))
            if data is None or data['Volume'] == 0:
                return
        else:
            self.gateway.write_error(_("查询持仓明细失败"), error)


    def onRspQryTradingAccount(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """资金查询回报"""
        if "AccountID" not in data:
            return
        account: AccountData = AccountData(
            accountid=data["AccountID"],
            balance=data["Balance"],
            frozen=data["FrozenMargin"] + data["FrozenCash"] + data["FrozenCommission"],
            gateway_name=self.gateway_name
        )
        account.available = data["Available"]
        self.gateway.on_account(account)

    def onRspQryInstrument(self, data: dict, error: dict, reqid: int, last: bool) -> None:
        """
        合约查询回报，当执行 ReqQryInstrument 后，该方法被调用
        :param data: 合约
        :param error: 响应信息
        :param reqid: 返回用户操作请求的 ID，该 ID 由用户在操作请求时指定。
        :param last: 指示该次返回是否为针对 reqid 的最后一次返回。
        :return:
        """
        try:
            if last or error.get("ErrorID"):
                self.gateway.write_log(_("CtpTdApi：onRspQryInstrument 已完成。最后：{}，错误 ID：{}").format(last, error.get('ErrorID', 'N/A')))
             # 1. 合约对象构建
            contract = build_contract(data)
            if contract:
                self.gateway.on_contract(contract)
                symbol_contract_map[contract.symbol] = contract

            # 2. 更新exchange_id_map
            if len(self.exchange_id_map) == 0:
                self.gateway.write_log(_("读取 {} 失败！已创建空白文件").format(GlobalPath.exchange_id_filepath))
            self.exchange_id_map[data.get("InstrumentID", "")] = data.get("ExchangeID", "")

            # 3. 最后一次回报时写文件、处理缓存
            if last:
                self.contract_inited = True
                self.gateway.write_log(_("合约信息查询成功"))
                try:
                    write_json_file(GlobalPath.exchange_id_filepath, self.exchange_id_map)
                except Exception as e:
                    self.gateway.write_error(_("写入 exchange_id.json 失败：{}".format(e)), error)

                for data in self.order_data:
                    self.onRtnOrder(data)
                self.order_data.clear()

                for data in self.trade_data:
                    self.onRtnTrade(data)
                self.trade_data.clear()
        except Exception as e:
            self.gateway.write_error(_("onRspQryInstrument 异常：{}".format(e)), error)

    def onRtnOrder(self, data: dict) -> None:
        """委托更新推送"""
        if not self.contract_inited:
            self.order_data.append(data)
            return

        symbol: str = data["InstrumentID"]
        contract: ContractData = symbol_contract_map[symbol]

        frontid: int = data["FrontID"]
        sessionid: int = data["SessionID"]
        order_ref: str = data["OrderRef"]
        orderid: str = f"{frontid}_{sessionid}_{order_ref}"

        status: Status = STATUS_CTP2VT.get(data["OrderStatus"], None)
        if not status:
            self.gateway.write_log(_("收到不支持的委托状态，委托号：{}").format(orderid))
            return

        timestamp: str = f"{data['InsertDate']} {data['InsertTime']}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S")
        dt = dt.replace(tzinfo=CHINA_TZ)

        tp: tuple = (data["OrderPriceType"], data["TimeCondition"], data["VolumeCondition"])
        order_type: OrderType = ORDERTYPE_CTP2VT.get(tp, None)
        if not order_type:
            self.gateway.write_log(_("收到不支持的委托类型，委托号：{}").format(orderid))
            return

        order: OrderData = OrderData(
            symbol=symbol,
            exchange=contract.exchange,
            orderid=orderid,
            type=order_type,
            direction=DIRECTION_CTP2VT[data["Direction"]],
            offset=OFFSET_CTP2VT[data["CombOffsetFlag"]],
            price=data["LimitPrice"],
            volume=data["VolumeTotalOriginal"],
            traded=data["VolumeTraded"],
            status=status,
            datetime=dt,
            gateway_name=self.gateway_name
        )
        self.gateway.on_order(order)

        self.sysid_orderid_map[data["OrderSysID"]] = orderid

    def onRtnTrade(self, data: dict) -> None:
        """
        成交数据推送，报单发出后有成交则通过此接口返回。私有流
        :param data:
        :return:
        """
        if not self.contract_inited:
            self.trade_data.append(data)
            return

        symbol: str = data["InstrumentID"]
        contract: ContractData = symbol_contract_map[symbol]

        orderid: str = self.sysid_orderid_map[data["OrderSysID"]]

        timestamp: str = f"{data['TradeDate']} {data['TradeTime']}"
        dt: datetime = datetime.strptime(timestamp, "%Y%m%d %H:%M:%S")
        dt = dt.replace(tzinfo=CHINA_TZ)

        trade: TradeData = TradeData(
            symbol=symbol,
            exchange=contract.exchange,
            orderid=orderid,
            tradeid=data["TradeID"],
            direction=DIRECTION_CTP2VT[data["Direction"]],
            offset=OFFSET_CTP2VT[data["OffsetFlag"]],
            price=data["Price"],
            volume=data["Volume"],
            datetime=dt,
            gateway_name=self.gateway_name
        )
        self.gateway.on_trade(trade)

    def OnRspQryInstrumentCommissionRate(self, data: dict, error: dict, reqid: int, last: bool):
        """
        请求查询合约手续费率响应，当执行 ReqQryInstrumentCommissionRate 后，该方法被调用。
        :param data: 合约手续费率
        :param error: 响应信息
        :param reqid: 返回用户操作请求的 ID，该 ID 由用户在操作请求时指定。
        :param last: 指示该次返回是否为针对 reqid 的最后一次返回。
        :return: 无
        """
        if last or error.get("ErrorID"):
            self.gateway.write_error(_("CtpTdApi：OnRspQryInstrumentCommissionRate 已完成。最后：{}，错误 ID：{}"
                                       .format(last, error.get('ErrorID', 'N/A'))), error)
            return
        if data is None or data['InstrumentID'] == '':
            return
        # timePrintLog(f'合约名称：{data.InstrumentID}')
        # timePrintLog(f'开仓手续费率：{data.OpenRatioByMoney}')
        # timePrintLog(f'开仓手续费：{data.OpenRatioByVolume}')
        # timePrintLog(f'平仓手续费率：{data.CloseRatioByMoney}')
        # timePrintLog(f'平仓手续费：{data.CloseRatioByVolume}')
        sec = re.sub(r'\d', '', data['InstrumentID'])

        # 需要判断section是否存在，如果不存在会报错，option不需要检查是否存在
        if not self.parser.has_section(sec):
            self.parser.add_section(sec)

        # 填写开仓手续费率
        opt = 'open_fee_rate'
        self.parser.set(sec, opt, str(data['OpenRatioByMoney']))

        # 填写开仓手续费
        opt = 'open_fee'
        self.parser.set(sec, opt, str(data['OpenRatioByVolume']))

        # 填写平仓手续费率
        opt = 'close_fee_rate'
        self.parser.set(sec, opt, str(data['CloseRatioByMoney']))

        # 填写平仓手续费
        opt = 'close_fee'
        self.parser.set(sec, opt, str(data['CloseRatioByVolume']))

        # 填写平今手续费率
        opt = 'close_today_fee_rate'
        self.parser.set(sec, opt, str(data['CloseTodayRatioByMoney']))

        # 填写平今手续费
        opt = 'close_today_fee'
        self.parser.set(sec, opt, str(data['CloseTodayRatioByVolume']))

        # 写入ini文件
        self.parser.write(open(GlobalPath.product_info_filepath, "w", encoding='utf-8'))


    def OnErrRtnOrderAction(self, data: dict, error: dict):
        """
        报单操作错误回报，当执行 ReqOrderAction 后有字段填写不对之类的CTP报错则通过此接口返回
        :param data: 报单操作
        :param error: 响应信息
        :return: 无
        """
        if error['ErrorID'] != 0 and error is not None:
            self.gateway.write_error(_("报单操作请求失败"), error)
        else:
            self.gateway.write_log(_('报单操作请求成功！'))


    def OnRspUserLogout(self, data: dict, error: dict, reqid: int, last: bool):
        """
        登出请求响应，当执行ReqUserLogout后，该方法被调用。
        :param data: 用户登出请求
        :param error: 响应信息
        :param reqid: 返回用户操作请求的 ID，该 ID 由用户在操作请求时指定。
        :param last: 指示该次返回是否为针对 reqid 的最后一次返回。
        :return: 无
        """
        self.gateway.write_log(_('交易账户：{} 已登出').format(data['UserID']))


    def connect(
        self,
        address: str,
        userid: str,
        password: str,
        brokerid: str,
        auth_code: str,
        appid: str
    ) -> None:
        """连接服务器"""
        self.userid = userid
        self.password = password
        self.broker_id = brokerid
        self.auth_code = auth_code
        self.appid = appid

        if not self.connect_status:
            path: Path = get_folder_path(self.gateway_name.lower())
            api_path_str = str(path) + "\\Td"
            self.gateway.write_log(_("CtpTdApi：尝试创建路径为 {} 的 API").format(api_path_str))
            try:
                self.createFtdcTraderApi(api_path_str.encode("GBK").decode("utf-8"))
                self.gateway.write_log(_("CtpTdApi：createFtdcTraderApi调用成功。"))
            except Exception as e_create:
                self.gateway.write_log(_("CtpTdApi：createFtdcTraderApi 失败！错误：{}").format(e_create))
                self.gateway.write_log(_("CtpTdApi：createFtdcTraderApi 回溯：{}").format(traceback.format_exc()))
                return

            self.subscribePrivateTopic(0)
            self.subscribePublicTopic(0)

            self.registerFront(address)
            self.gateway.write_log(_("CtpTdApi：尝试使用地址初始化 API：{}...").format(address))
            try:
                self.init()
                self.gateway.write_log(_("CtpTdApi：init 调用成功。"))
            except Exception as e_init:
                self.gateway.write_log(_("CtpTdApi：初始化失败！错误：{}").format(e_init))
                self.gateway.write_log(_("CtpTdApi：初始化回溯：{}").format(traceback.format_exc()))
                return

            self.connect_status = True
        else:
            self.gateway.write_log(_("CtpTdApi：已连接，正在尝试身份验证。"))
            self.authenticate()

    def authenticate(self) -> None:
        """发起授权验证"""
        if self.auth_failed:
            return

        ctp_req: dict = {
            "UserID": self.userid,
            "BrokerID": self.broker_id,
            "AuthCode": self.auth_code,
            "AppID": self.appid
        }

        self.req_id += 1
        self.reqAuthenticate(ctp_req, self.req_id)

    def login(self) -> None:
        """用户登录"""
        if self.login_failed:
            return

        ctp_req: dict = {
            "UserID": self.userid,
            "Password": self.password,
            "BrokerID": self.broker_id
        }

        self.req_id += 1
        self.reqUserLogin(ctp_req, self.req_id)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        if req.offset not in OFFSET_VT2CTP:
            self.gateway.write_log(_("请选择开平方向"))
            return ""

        if req.type not in ORDERTYPE_VT2CTP:
            self.gateway.write_log(_("当前接口不支持该类型的委托{}").format(req.type.value))
            return ""

        self.order_ref += 1

        tp: tuple = ORDERTYPE_VT2CTP[req.type]
        price_type, time_condition, volume_condition = tp

        ctp_req: dict = {
            "InstrumentID": req.symbol,
            "ExchangeID": req.exchange.value,
            "LimitPrice": req.price,
            "VolumeTotalOriginal": int(req.volume),
            "OrderPriceType": price_type,
            "Direction": DIRECTION_VT2CTP.get(req.direction, ""),
            "CombOffsetFlag": OFFSET_VT2CTP.get(req.offset, ""),
            "OrderRef": str(self.order_ref),
            "InvestorID": self.userid,
            "UserID": self.userid,
            "BrokerID": self.broker_id,
            "CombHedgeFlag": THOST_FTDC_HF_Speculation,
            "ContingentCondition": THOST_FTDC_CC_Immediately,
            "ForceCloseReason": THOST_FTDC_FCC_NotForceClose,
            "IsAutoSuspend": 0,
            "TimeCondition": time_condition,
            "VolumeCondition": volume_condition,
            "MinVolume": 1
        }

        self.req_id += 1
        n: int = self.reqOrderInsert(ctp_req, self.req_id)
        if n:
            self.gateway.write_log(_("委托请求发送失败，错误代码：{}").format(n))
            return ""

        orderid: str = f"{self.frontid}_{self.sessionid}_{self.order_ref}"
        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)

        return order.vt_orderid     # type: ignore

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        front_id, session_id, order_ref = req.orderid.split("_")

        ctp_req: dict = {
            "InstrumentID": req.symbol,
            "ExchangeID": req.exchange.value,
            "OrderRef": order_ref,
            "FrontID": int(front_id),
            "SessionID": int(session_id),
            "ActionFlag": THOST_FTDC_AF_Delete,
            "BrokerID": self.broker_id,
            "InvestorID": self.userid
        }

        self.req_id += 1
        self.reqOrderAction(ctp_req, self.req_id)

    def query_account(self) -> None:
        """查询资金"""
        self.req_id += 1
        self.reqQryTradingAccount({}, self.req_id)

    def query_position(self) -> None:
        """查询持仓"""
        if not symbol_contract_map:
            return

        ctp_req: dict = {
            "BrokerID": self.broker_id,
            "InvestorID": self.userid
        }

        self.req_id += 1
        self.reqQryInvestorPosition(ctp_req, self.req_id)

    def close(self) -> None:
        """关闭连接"""
        if self.connect_status:
            self.exit()


def adjust_price(price: float) -> float:
    """将异常的浮点数最大值（MAX_FLOAT）数据调整为0"""
    if price == MAX_FLOAT:
        price = 0
    return price
