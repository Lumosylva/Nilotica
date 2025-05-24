#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : params
@Date       : 2025/4/11 15:32
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 项目常用参数配置文件
"""


class Params(object):

    # ----------------------------项目中目录名称----------------------------
    project_name = "Nilotica"  # 项目名称
    project_files_dir_name = "project_files"  # 配置文件目录名
    log_dir_name = "log"  # 日志目录名
    log_root_filename = "nilotica.log"
    tick_data_name = "recorded_data"  # 实时数据目录名
    tick_dev_data_name = "dev_recorded_data"  # 开发环境下实时数据目录名
    kline_data_name = "kline_data"  # K线数据目录名
    backtest_result_name = "backtest_result"  # 回测结果目录名
    backtest_report_name = "backtest_report"  # 回测报告目录名
    # ----------------------------项目中目录名称----------------------------

    # ----------------------------项目参数配置文件----------------------------
    brokers_config_filename = "brokers_config.json"  # 多源服务器节点配置文件名
    global_config_filename = "global_config.yaml"  # 全局配置文件名
    prod_config_filename = "prod_config.yaml"  # 生产环境配置文件名
    dev_config_filename = "dev_config.yaml"  # 开发环境配置文件名
    backtest_config_filename = "backtest_config.yaml"  # 回测配置文件名
    strategy_setting_filename = "strategies_setting.json"  # 策略配置文件名
    backtest_strategy_setting_filename = "backtest_strategies_setting.json"  # 回测策略配置文件名
    # ----------------------------项目参数配置文件----------------------------

    # ----------------------------项目信息文件名----------------------------
    instrument_exchange_id_filename = "instrument_exchange_id.json"  # 期货合约与交易所映射信息文件名
    product_info_filename = "product_info.ini"  # 合约乘数及手续费信息文件名
    backtest_product_info_filename = "backtest_product_info.ini"  # 回测模式下合约乘数及手续费信息文件名
    holidays_filename = "_holidays.json"  # 节假日文件后缀名称
    # ----------------------------项目信息文件名----------------------------

    # ------------------------------日志类常量------------------------------
    file_format = "%Y%m%d"  # 日志文件名格式
    log_time_format = "%Y-%m-%d %H:%M:%S.%f"  # 日志文件中时间格式
    print_time_format = "%Y-%m-%d %H:%M:%S.%f"  # 控制台打印的时间格式
    # ------------------------------日志类常量------------------------------

    # -------------------------------时间常量-------------------------------
    TIME_SIXTY_SECONDS = 60  # 60秒
    # -------------------------------时间常量-------------------------------
