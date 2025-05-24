#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : tts_mapping
@Date       : 2025/5/24 23:29
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: description
"""
from vnpy.trader.constant import Direction, Exchange, Offset, OptionType, OrderType, Product, Status

# Import TTS specific constants from the api
from ..api import (
    THOST_FTDC_CP_CallOptions,
    THOST_FTDC_CP_PutOptions,
    THOST_FTDC_D_Buy,
    THOST_FTDC_D_Sell,
    THOST_FTDC_OAS_Accepted,
    THOST_FTDC_OAS_Rejected,
    THOST_FTDC_OAS_Submitted,
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
    THOST_FTDC_PC_Combination,
    THOST_FTDC_PC_Futures,
    THOST_FTDC_PC_Options,
    THOST_FTDC_PC_SpotOption,
    THOST_FTDC_PD_Long,
    THOST_FTDC_PD_Short,
)

# 委托状态映射
STATUS_TTS2VT: dict[str, Status] = {
    THOST_FTDC_OAS_Submitted: Status.SUBMITTING,
    THOST_FTDC_OAS_Accepted: Status.SUBMITTING,
    THOST_FTDC_OAS_Rejected: Status.REJECTED,
    THOST_FTDC_OST_NoTradeQueueing: Status.NOTTRADED,
    THOST_FTDC_OST_PartTradedQueueing: Status.PARTTRADED,
    THOST_FTDC_OST_AllTraded: Status.ALLTRADED,
    THOST_FTDC_OST_Canceled: Status.CANCELLED
}

# 多空方向映射
DIRECTION_VT2TTS: dict[Direction, str] = {
    Direction.LONG: THOST_FTDC_D_Buy,
    Direction.SHORT: THOST_FTDC_D_Sell
}
DIRECTION_TTS2VT: dict[str, Direction] = {v: k for k, v in DIRECTION_VT2TTS.items()}
DIRECTION_TTS2VT[THOST_FTDC_PD_Long] = Direction.LONG
DIRECTION_TTS2VT[THOST_FTDC_PD_Short] = Direction.SHORT

# 委托类型映射
ORDERTYPE_VT2TTS: dict[OrderType, str] = {
    OrderType.LIMIT: THOST_FTDC_OPT_LimitPrice,
    OrderType.MARKET: THOST_FTDC_OPT_AnyPrice
}
ORDERTYPE_TTS2VT: dict[str, OrderType] = {v: k for k, v in ORDERTYPE_VT2TTS.items()}

# 开平方向映射
OFFSET_VT2TTS: dict[Offset, str] = {
    Offset.OPEN: THOST_FTDC_OF_Open,
    Offset.CLOSE: THOST_FTDC_OFEN_Close,
    Offset.CLOSETODAY: THOST_FTDC_OFEN_CloseToday,
    Offset.CLOSEYESTERDAY: THOST_FTDC_OFEN_CloseYesterday,
}
OFFSET_TTS2VT: dict[str, Offset] = {v: k for k, v in OFFSET_VT2TTS.items()}

# 交易所映射
EXCHANGE_TTS2VT: dict[str, Exchange] = {
    "CFFEX": Exchange.CFFEX,
    "SHFE": Exchange.SHFE,
    "CZCE": Exchange.CZCE,
    "DCE": Exchange.DCE,
    "GFEX": Exchange.GFEX,
    "INE": Exchange.INE,
    "SSE": Exchange.SSE,
    "SZSE": Exchange.SZSE,
    "NASD": Exchange.NASDAQ,
    "NYSE": Exchange.NYSE,
    "HKEX": Exchange.SEHK,
}
EXCHANGE_VT2TTS: dict[Exchange, str] = {v: k for k, v in EXCHANGE_TTS2VT.items()}

# 产品类型映射
PRODUCT_TTS2VT: dict[str, Product] = {
    THOST_FTDC_PC_Futures: Product.FUTURES,
    THOST_FTDC_PC_Options: Product.OPTION,
    THOST_FTDC_PC_SpotOption: Product.OPTION,
    THOST_FTDC_PC_Combination: Product.SPREAD,
    'E': Product.EQUITY,
    'B': Product.BOND,
    'F': Product.FUND, # Assuming 'F' from some TTS contexts might map to FUND
}

# 期权类型映射
OPTIONTYPE_TTS2VT: dict[str, OptionType] = {
    THOST_FTDC_CP_CallOptions: OptionType.CALL,
    THOST_FTDC_CP_PutOptions: OptionType.PUT
}
