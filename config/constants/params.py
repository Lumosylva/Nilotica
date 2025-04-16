#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica_dev
@FileName   : params
@Date       : 2025/4/11 15:32
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 项目常用参数配置文件
"""


class Params(object):

    # ----------project配置信息---------------------
    # 期货合约与交易所映射信息文件名
    exchange_id_filename = "exchange_id.json"

    # 合约乘数及手续费信息文件名
    product_info_filename = "product_info.ini"

    # 节假日文件后缀名称
    holidays_filename = "_holidays.json"
    # ----------project配置信息---------------------

    # ----------项目中目录名称---------------------
    # 项目名称
    project_name = "Nilotica_dev"

    # 日志目录名
    log_dir_name = "logs"

    log_level = "INFO"

    log_filename = "nilotica.log"

    # 实时数据目录名
    tick_data_name = "recorded_data"

    # K线数据目录名
    kline_data_name = "kline_data"

    # 配置文件目录名
    project_files_dir_name = "project_files"

    # 回测数据目录名
    backtest_data_name = "backtest_data"

    # 回测结果目录名
    backtest_result_name = "backtest_result"

    # 回测报告目录名
    backtest_report_name = "backtest_report"
    # ----------项目中目录名称---------------------

    # ----------Log类常量----------------------
    # 文件名格式
    file_format = "%Y%m%d"

    # 日志文件中时间格式
    log_time_format = "%Y-%m-%d %H:%M:%S.%f"

    # 控制台打印的时间格式
    print_time_format = "%Y-%m-%d %H:%M:%S.%f"
    # ----------Log类常量-----------------------

    # ----------时间常量--------------------
    TIME_SIXTY_SECONDS = 60
