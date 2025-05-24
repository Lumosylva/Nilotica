#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : global_vars
@Date       : 2025/5/22 23:26
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 全局变量
"""
import configparser
import os # 导入os模块

from config.constants.path import GlobalPath
from utils.file_helper import load_json_file, write_json_file, load_ini_file

# 交易账户登录信号，用于检查是否登录成功，True代表登录成功
# 每天 08：45，20：45进行初始化
md_login_success: bool = False
td_login_success: bool = False

# 合约手续费和保证金等
product_info = load_ini_file(GlobalPath.product_info_filepath)

# 合约与交易所映射
instrument_exchange_id_map = load_json_file(GlobalPath.instrument_exchange_id_filepath)
if len(instrument_exchange_id_map) == 0:
    write_json_file(GlobalPath.instrument_exchange_id_filepath, instrument_exchange_id_map)
