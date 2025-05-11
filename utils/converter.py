#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica_dev
@FileName   : converter.py
@Date       : 2025/5/6 10:08
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 对象转换为 msgpack 可序列化的工具。
Tools for converting objects to msgpack serializable.
"""

from typing import Any, Dict, Union
from datetime import datetime
from decimal import Decimal

from utils.i18n import get_translator
from vnpy.trader.object import TickData, OrderData, TradeData, AccountData, ContractData, LogData, PositionData, \
    OrderRequest
from vnpy.trader.constant import Direction, OrderType, Exchange, Offset, Status, Product, OptionType

# Assuming logger setup is handled elsewhere or just use standard logging
import logging
logger = logging.getLogger(__name__)
_ = get_translator()


def convert_vnpy_obj_to_dict(obj: object) -> Any:
    """
    将 VNPY 对象转换为 msgpack 可序列化的字典。

    此函数主要用于 ZMQ 事件发布，支持多种 VNPY 对象类型：
    - TickData
    - OrderData
    - TradeData
    - AccountData
    - ContractData
    - LogData
    - PositionData
    
    Args:
        obj: 要转换的 VNPY 对象
        
    Returns:
        Any: 转换后的可序列化对象
        
    Note:
        此函数使用递归方式处理嵌套对象，并处理特殊类型如枚举和日期时间。

    Convert a VNPY object to a msgpack serializable dictionary.

    This function is mainly used for ZMQ event publishing and supports multiple VNPY object types:

    - TickData

    - OrderData

    - TradeData

    - AccountData

    - ContractData

    - LogData

    - PositionData

    Args:
    obj: VNPY object to be converted

    Returns:
    Any: The converted serializable object

    Note:
    This function uses recursion to process nested objects and handles special types such as enumerations and datetimes.
    """
    if isinstance(obj, (TickData, OrderData, TradeData, AccountData, ContractData, LogData)):
        d = obj.__dict__.copy() # Use a copy
        for key, value in d.items():
            if isinstance(value, (Direction, OrderType, Exchange, Offset, Status, Product, OptionType)):
                d[key] = value.value if value else None # Enum to value
            elif isinstance(value, datetime):
                d[key] = value.isoformat() if value else None # datetime to ISO string
            elif isinstance(value, Decimal):
                d[key] = str(value) if value is not None else None # Decimal to string
            elif isinstance(value, (list, tuple)):
                try:
                    d[key] = [convert_vnpy_obj_to_dict(item) for item in value] # Recursive list conversion
                except TypeError:
                    logger.warning(_("无法转换列表/元组中的项目，使用字符串表示。"))
                    d[key] = str(value)
            elif not isinstance(value, (str, int, float, bool, bytes, dict, type(None))): # bytes added
                if hasattr(value, '__dict__'):
                     d[key] = convert_vnpy_obj_to_dict(value) # Recursive for nested objects
                else:
                    d[key] = str(value) # Fallback to string for unknown nested types
        return d
    elif isinstance(obj, dict):
         return {k: convert_vnpy_obj_to_dict(v) for k, v in obj.items()} # Recursive dict conversion
    elif isinstance(obj, (list, tuple)):
        try:
             return [convert_vnpy_obj_to_dict(item) for item in obj] # Recursive list conversion
        except TypeError:
             logger.warning(_("无法转换列表/元组中的项目，使用字符串表示。"))
             return str(obj)
    elif isinstance(obj, (str, int, float, bool, bytes, type(None))): # bytes added
        return obj # Basic types are fine
    elif isinstance(obj, datetime):
         return obj.isoformat() # Standalone datetime
    elif isinstance(obj, Decimal):
        return str(obj) # Standalone Decimal
    elif isinstance(obj, (Direction, OrderType, Exchange, Offset, Status, Product, OptionType)):
        return obj.value if obj else None # Standalone Enum
    elif isinstance(obj, PositionData):
        d = obj.__dict__.copy()
        for key, value in d.items():
            if isinstance(value, (Direction, Exchange)):
                d[key] = value.value
            elif key == "extra" and value is None: # Handle None extra field
                pass # Keep as None or remove d[key]? Let's keep None for now.
        return d
    else:
        return obj # Return as is


# --- Helper Functions for Serialization/Deserialization ---
def convert_order_data_to_dict(obj: Union[OrderData, TradeData, AccountData, PositionData, datetime]) -> str | dict[
    str, Any] | AccountData | datetime:
    """
    将订单相关的 VNPY 对象转换为适合 RPC 调用的字典。

    此函数专门用于订单执行网关的 RPC 调用，主要处理：
    - OrderData
    - TradeData
    - AccountData
    - PositionData

    Args:
        obj: 要转换的 VNPY 对象

    Returns:
        Dict[str, Any]: 转换后的字典

    Note:
        此函数专注于订单执行相关的对象转换，与 convert_vnpy_obj_to_dict 相比更专注于特定业务场景。

    Converts an order-related VNPY object into a dictionary suitable for RPC calls.

    This function is specifically used for RPC calls of the order execution gateway, mainly processing:
    - OrderData
    - TradeData
    - AccountData
    - PositionData

    Args:
    obj: VNPY object to be converted

    Returns:
    Dict[str, Any]: The converted dictionary

    Note:
    This function focuses on object conversion related to order execution, and is more focused on specific
    business scenarios than convert_vnpy_obj_to_dict.
    """
    if isinstance(obj, (OrderData, TradeData)):  # Add TickData if needed here too
        d = obj.__dict__
        # Convert Enums and Datetime
        for key, value in d.items():
            if isinstance(value, (Direction, OrderType, Exchange, Offset, Status)):
                d[key] = value.value  # Use Enum value
            elif isinstance(value, datetime):
                d[key] = value.isoformat() if value else None
        return d
    elif isinstance(obj, datetime):
        return obj.isoformat()
    # Add AccountData handler
    elif isinstance(obj, AccountData):
        d = obj.__dict__
        return d
    # Add handlers for other types if needed
    else:
        # Fallback for basic types
        if isinstance(obj, (str, int, float, bool, list, tuple, dict, bytes, type(None))):
            return obj
        try:
            # Recursive attempt (use with caution)
            d = obj.__dict__
            for key, value in d.items():
                d[key] = convert_order_data_to_dict(value)
            return d
        except AttributeError:
            logger.warning(_("警告：convert_order_data_to_dict 中未处理的类型: {}。转换为字符串。").format(type(obj)))
            return str(obj)

def dict_to_order_request(data_dict: Dict[str, Any]) -> OrderRequest | None:
    """
    将字典转换回 vnpy OrderRequest 对象，订单执行网关调用。

    Args:
        data_dict: 要转换的字典

    Converts a dictionary back into a vnpy OrderRequest object.

    Args:
        data_dict: dictionary to be converted
    """
    try:
        direction = Direction(data_dict['direction'])
        order_type = OrderType(data_dict['type'])
        exchange = Exchange(data_dict['exchange'])
        offset = Offset(data_dict.get('offset', Offset.NONE.value))

        req = OrderRequest(
            symbol=data_dict['symbol'],
            exchange=exchange,
            direction=direction,
            type=order_type,
            volume=data_dict['volume'],
            price=data_dict.get('price', 0.0),
            offset=offset,
            reference=data_dict.get('reference', "rpc_gw")
        )
        return req
    except KeyError as err:
        logger.error(_("创建 OrderRequest 失败：缺少关键字段：{} from data {}").format(err, data_dict))
        return None
    except ValueError as err:
        logger.error(_("创建 OrderRequest 失败：无效的枚举值：{} from data {}").format(err, data_dict))
        return None
    except Exception as err:
        logger.exception(_("创建 OrderRequest 时发生未知错误：{} from data {}").format(err, data_dict))
        return None
