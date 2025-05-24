#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : path
@Date       : 2025/4/11 15:51
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 完整路径常量，存储一些常用的项目目录和文件完整路径
"""
from pathlib import Path

from config.constants.params import Params
from utils.path import get_path_ins


class GlobalPath(object):
    # ------------------常用目录完整路径---------------------------------------------------------
    # 项目根路径
    project_root_path = Path(get_path_ins.get_project_dir())

    # 类似C:/Users/donny/.nilotica/log，日志目录完整路径
    log_dir_path = project_root_path / Params.log_dir_name

    # zmq_services/recorded_data，实时数据目录完整路径
    tick_data_path = project_root_path / "zmq_services" / Params.tick_data_name

    # zmq_services/kline_data，记录K线目录完整路径
    kline_data_path = project_root_path / "zmq_services" / Params.kline_data_name

    # config，配置目录完整路径
    config_dir_path = project_root_path / "config"

    # DEFAULT_GLOBAL_CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", GLOBAL_CONFIG_FILENAME)

    # config/project_files
    # 配置目录完整路径(用于保存配置文件: instrument_exchange_id.json、product_info.ini、2025_holidays.json等)
    project_files_path = config_dir_path / Params.project_files_dir_name

    #  config/project_files，holiday文件存放目录完整路径
    holiday_dir_path = project_files_path
    # ------------------常用目录完整路径---------------------------------------------------------

    # ------------------产品信息文件完整路径------------------------------------------------------
    # config/project_files/instrument_exchange_id.json，交易所配置信息文件完整路径
    instrument_exchange_id_filepath = project_files_path / Params.instrument_exchange_id_filename

    # config/project_files/product_info.ini，产品信息文件完整路径
    product_info_filepath = project_files_path / Params.product_info_filename

    # config/project_files/backtest_product_info.ini，回测产品信息文件完整路径
    backtest_product_info_filepath = project_files_path / Params.backtest_product_info_filename
    # ------------------产品信息文件完整路径------------------------------------------------------

    # ------------------运行及回测配置文件完整路径-------------------------------------------------
    # config/global_config.yaml，全局配置文件完整路径
    global_config_filepath = config_dir_path / Params.global_config_filename
    # ------------------运行及回测配置文件完整路径-------------------------------------------------
