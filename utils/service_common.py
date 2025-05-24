#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : runner_common
@Date       : 2025/5/22 16:47
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 命令行启动公共函数
"""
import argparse
import importlib
import sys
from argparse import Namespace

from utils.i18n import _
from utils.logger import logger, INFO, setup_logging
from vnpy.event import EventEngine
from vnpy_ctp import CtpGateway
from vnpy_tts import TtsGateway


def runner_args(arg_desc: str) -> Namespace:
    """
    定义命令行参数解析器。

    Args:
        arg_desc (str): 命令行工具的描述。

    Returns:
        argparse.Namespace: 包含解析后的命令行参数的命名空间对象。

    """
    setup_logging(service_name=__name__, level=INFO)
    parser = argparse.ArgumentParser(description=arg_desc)
    parser.add_argument(
        "--ctp-env",  # Renamed from --env for clarity
        default="",
        type=str,
        help="The CTP environment name defined in brokers_config.json (or for general service identification). Defaults to ''."
    )
    parser.add_argument(
        "--config-env",
        default=None,  # Changed from "dev" to None
        type=str,
        help="The configuration environment to load (e.g., 'dev', 'prod', 'backtest'). Overrides global_config.yaml."
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the minimum logging level."
    )
    # +++ Add --lang argument for overriding config language +++
    parser.add_argument(
        "--lang",
        type=str,
        default=None,  # Default is None, will use config or i18n default
        help="Language code for i18n (e.g., en, ""). Overrides configuration file setting."
    )
    args = parser.parse_args()

    # 正在使用的日志环境(Log environments being used)
    if args.ctp_env == "simnow" and '--ctp-env' not in sys.argv and '--env' not in sys.argv:
        logger.info(_("未指定 --ctp-env，使用默认 CTP 环境：{}"), args.ctp_env)
    if args.config_env:
        logger.info(_("使用配置环境：'{}'"), args.config_env)
    else:
        logger.info(_("未指定 --config-env，仅使用基本 global_config.yaml。"))

    return args

def get_gateway(event_engine: EventEngine, environment_name: str) -> CtpGateway | TtsGateway:
    """
    根据环境名称动态加载并实例化对应的网关（CTP或TTS）。

    Args:
        event_engine (EventEngine): 事件引擎实例
        environment_name (str): 环境名称（如 simnow、real、tts_xxx 等）

    Returns:
        gateway 实例
    """
    # 默认使用CTP
    if environment_name in ("simnow", "simnow7x24", "real"):
        gateway_lib = "vnpy_ctp"
        class_name = "CtpGateway"
        gateway_name = f"CTP_{environment_name}"
        gateway_module_name = f"{gateway_lib}.gateway.ctp_gateway"
    else:
        gateway_lib = "vnpy_tts"
        class_name = "TtsGateway"
        gateway_name = f"TTS_{environment_name}"
        gateway_module_name = f"{gateway_lib}.gateway.tts_gateway"


    try:
        gateway_module = importlib.import_module(gateway_module_name)
    except ImportError as e:
        logger.error(_("无法导入网关模块：{}，错误信息：{}" ).format(gateway_module_name, e))
        raise

    try:
        gateway_class = getattr(gateway_module, class_name)
    except AttributeError:
        logger.error(_("在模块 {} 中未找到类 {}" ).format(gateway_module_name, class_name))
        raise

    try:
        gateway = gateway_class(event_engine, gateway_name)
    except Exception as e:
        logger.error(_("实例化网关 {} 失败，错误信息：{}" ).format(class_name, e))
        raise

    logger.info(_("已成功加载并实例化网关：{}" ).format(gateway_name))
    return gateway
