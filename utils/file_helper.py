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
from typing import Dict, Any

import yaml

from utils.logger import logger
from utils.i18n import _


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


def read_ini_file(file_path: str):
    parser = configparser.ConfigParser()
    parser.read(file_path, encoding='utf-8')
