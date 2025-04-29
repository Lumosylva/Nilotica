#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : path
@Date       : 2025/4/11 15:51
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 路径常量
"""
from pathlib import Path

from config.constants.params import Params
from utils.path import get_path_ins


class GlobalPath(object):

    # 项目根路径
    project_root_path = Path(get_path_ins.get_project_dir())

    # 日志目录完整路径
    log_dir_path = project_root_path / Params.log_dir_name

    # 实时数据完整路径
    tick_data_path = project_root_path / "zmq_services" / Params.tick_data_name

    # 记录K线完整路径
    kline_data_path = project_root_path / "zmq_services" / Params.kline_data_name

    # 配置目录完整路径(用于保存配置文件: exchange_id.json、product_info.ini、2025_holidays.json等)
    project_files_path = project_root_path / "config" / Params.project_files_dir_name

    # ------------------配置文件路径-----------------------------------------

    # 交易所配置信息文件完整路径
    exchange_id_filepath = project_files_path / Params.exchange_id_filename

    # 产品信息文件完整路径
    product_info_filepath = project_files_path / Params.product_info_filename

    # ------------------配置文件路径-----------------------------------------
