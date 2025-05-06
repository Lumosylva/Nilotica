#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica_dev
@FileName   : converter.py
@Date       : 2025/5/6 10:08
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: description
"""

from typing import Any
from datetime import datetime
from decimal import Decimal

# +++ Add VNPY imports needed for the converter +++
from vnpy.trader.object import TickData, OrderData, TradeData, AccountData, ContractData, LogData, PositionData
from vnpy.trader.constant import Direction, OrderType, Exchange, Offset, Status, Product, OptionType
# +++ End VNPY imports +++

# +++ Import logger +++
# Assuming logger setup is handled elsewhere or just use standard logging
import logging
logger = logging.getLogger(__name__)
# +++ End import logger +++

# +++ Add the converter function (adapted from DataRecorder) +++
def convert_vnpy_obj_to_dict(obj: object) -> Any:
    """Converts known VNPY objects to msgpack-serializable dicts/lists/primitives."""
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
                    logger.warning(f"Could not convert items in list/tuple for key '{key}', using string representation.")
                    d[key] = str(value)
            elif not isinstance(value, (str, int, float, bool, bytes, dict, type(None))): # bytes added
                if hasattr(value, '__dict__'):
                     d[key] = convert_vnpy_obj_to_dict(value) # Recursive for nested objects
                else:
                    # logger.warning(f"Unhandled type '{type(value)}' for key '{key}' in VNPY object, converting to string.")
                    d[key] = str(value) # Fallback to string for unknown nested types
        return d
    elif isinstance(obj, dict):
         return {k: convert_vnpy_obj_to_dict(v) for k, v in obj.items()} # Recursive dict conversion
    elif isinstance(obj, (list, tuple)):
        try:
             return [convert_vnpy_obj_to_dict(item) for item in obj] # Recursive list conversion
        except TypeError:
             logger.warning(f"Could not convert items in list/tuple, using string representation.")
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
            # Add other PositionData specific fields if needed
        return d
    else:
        # If it's not a known VNPY object or basic type, maybe it's directly serializable?
        # Let msgpack try, log if it fails later.
        # logger.debug(f"Object type '{type(obj)}' passed to msgpack without explicit conversion.")
        return obj # Return as is
# +++ End converter function +++
