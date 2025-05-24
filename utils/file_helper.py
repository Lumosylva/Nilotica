#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : load_helper
@Date       : 2025/5/20 15:32
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 配置文件加载工具
"""
import configparser
import json
import os
from typing import Dict, Any, Tuple

import yaml

from utils.logger import logger, setup_logging, INFO
from utils.i18n import _



setup_logging(service_name=__name__, level=INFO)


def load_yaml_file(file_path: str) -> Dict[str, Any]:
    """
    加载 YAML 文件。

    Loads a YAML file.
    """
    if not os.path.exists(file_path):
        logger.error("Configuration file not found: {}".format(file_path))
        return {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        logger.debug("Successfully loaded YAML configuration from {}".format(file_path))
        return data if data else {}
    except yaml.YAMLError as e:
        logger.error("Unable to parse YAML file {}: {}".format(file_path, e))
        return {}
    except IOError as e:
        logger.error("Unable to read file {}: {}".format(file_path, e))
        return {}

def load_json_file(file_path: str) -> Dict[str, Any]:
    """
    加载 JSON 文件。

    Loads a JSON file.
    """
    if not os.path.exists(file_path):
        logger.info(_("未找到可选的 JSON 配置文件：{}").format(file_path))
        return {}
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        logger.info(_("已成功从 {} 加载 JSON 配置").format(file_path))
        return data if data else {}
    except json.JSONDecodeError as e:
        logger.error(_("无法解析 JSON 文件 {}: {}").format(file_path, e))
        return {}
    except IOError as e:
        logger.error(_("无法读取文件 {}: {}").format(file_path, e))
        return {}


def write_json_file(file_path: str, data: Dict[str, Any]) -> None:
    """
    将数据写入 JSON 文件。

    Writes the given data into a JSON file at the specified path.
    """
    try:
        with open(file_path, 'w', newline='\n', encoding='utf-8') as f:
            file_data = json.dumps(data, indent=4, ensure_ascii=False)
            f.write(file_data)
        logger.info(_("已成功将数据写入 JSON 文件 {}").format(file_path))
    except IOError as e:
        logger.error(_("无法写入文件 {}: {}").format(file_path, e))


def load_ini_file(file_path: str):
    """
    从指定路径加载INI配置文件。

    Args:
        file_path (str): INI配置文件的路径。

    Returns:
        ConfigParser: 加载的INI配置文件对象。

    说明：
        如果指定的文件不存在，则会创建一个空的INI文件。
        如果文件存在，则读取文件内容并返回一个ConfigParser对象。
        在创建空文件时，可以选择写入一些默认的空section或者注释，如果需要的话。

    """
    parser = configparser.ConfigParser()
    # 检查文件是否存在，如果不存在则创建一个空的ini文件
    if not os.path.exists(file_path):
        with open(file_path, 'w', encoding='utf-8') as f:
            # 可以选择写入一些默认的空section或者注释，如果需要的话
            # f.write("# Empty product_info.ini created by system\n")
            pass  # 创建一个空文件
    parser.read(file_path, encoding='utf-8')

    return parser

def load_product_info(filepath: str) -> Tuple[Dict, Dict]:
    """
    从 INI 文件加载佣金规则和乘数，订单执行网关调用。

    Loads commission rules and multipliers from an INI file.
    """
    parser = configparser.ConfigParser()
    if not os.path.exists(filepath):
        logger.warning(_("错误：产品信息文件未找到 {}").format(filepath))
        return {}, {}
    try:
        parser.read(filepath, encoding='utf-8')
    except Exception as err:
        logger.exception(_("错误：读取产品信息文件 {} 时出错：{}").format(filepath, err))
        return {}, {}
    commission_rules = {}
    contract_multipliers = {}
    for symbol in parser.sections():
        if not parser.has_option(symbol, 'contract_multiplier'):
            logger.warning(_("警告：文件 {} 中的 [{}] 缺少 'contract_multiplier'，跳过此合约。").format(filepath, symbol))
            continue
        try:
            contract_multiplier = parser.getfloat(symbol, 'contract_multiplier')
            contract_multipliers[symbol] = contract_multiplier
            rule = {
                "open_rate": parser.getfloat(symbol, 'open_rate', fallback=0.0),
                "close_rate": parser.getfloat(symbol, 'close_rate', fallback=0.0),
                "open_fixed": parser.getfloat(symbol, 'open_fixed', fallback=0.0),
                "close_fixed": parser.getfloat(symbol, 'close_fixed', fallback=0.0),
                "min_commission": parser.getfloat(symbol, 'min_commission', fallback=0.0)
            }
            commission_rules[symbol] = rule
        except ValueError as err:
            logger.warning(_("警告：解析文件 {} 中 [{}] 的数值时出错: {}，跳过此合约。").format(filepath, symbol, err))
        except Exception as err:
            logger.warning(
                _("警告：处理文件 {} 中 [{}] 时发生未知错误: {}，跳过此合约。").format(filepath, symbol, err))
    logger.info(
        _("从 {} 加载了 {} 个合约的乘数和 {} 个合约的手续费规则。").format(filepath, len(contract_multipliers),
                                                                          len(commission_rules)))
    return commission_rules, contract_multipliers