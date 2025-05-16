#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : __init__.py
@Date       : 2025/4/28 20:28
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 日志模块，包含日志配置和日志记录器
Log module, including log configuration and logger
"""
import sys
from pathlib import Path
from typing import Optional

from loguru import logger

from utils.i18n import get_translator
from vnpy.trader.setting import SETTINGS
from vnpy.trader.utility import get_folder_path

# --- Default Configuration ---
DEFAULT_LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> - "
    "<level>{level}</level> - "
    "<cyan>{extra[service]}</cyan>"
    " <magenta>{extra[config_env]}</magenta> - "
    "<level>{message}</level>"
)
DEFAULT_SERVICE_NAME = "nilotica_default" # 默认服务名称(Default service name if not patched)
DEFAULT_LOG_ROTATION = "100 MB"  # 当文件超过 100MB 时(Rotate when file exceeds 100MB)
DEFAULT_LOG_RETENTION = "7 days" # 保留日志 7 天(Keep logs for 7 days)

# --- 全局记录器对象(Global Logger Object) ---
# 从 loguru 重新导出记录器对象(Re-export the logger object from loguru)
__all__ = ["logger", "setup_logging", "INFO", "DEBUG", "WARNING", "WARN", "ERROR", "FATAL", "CRITICAL", "NOTSET"]

# 从 vnpy 设置中读取日志级别(Read log level from vnpy settings)
DEFAULT_LOG_LEVEL: str = SETTINGS.get("log.level", "INFO")


CRITICAL = 50
FATAL = CRITICAL
ERROR = 40
WARNING = 30
WARN = WARNING
INFO = 20
DEBUG = 10
NOTSET = 0

_levelToName = {
    CRITICAL: 'CRITICAL',
    ERROR: 'ERROR',
    WARNING: 'WARNING',
    INFO: 'INFO',
    DEBUG: 'DEBUG',
    NOTSET: 'NOTSET',
}

_nameToLevel = {
    'CRITICAL': CRITICAL,
    'FATAL': FATAL,
    'ERROR': ERROR,
    'WARN': WARNING,
    'WARNING': WARNING,
    'INFO': INFO,
    'DEBUG': DEBUG,
    'NOTSET': NOTSET,
}

def get_level_name(level: int | str):
    if level is not None:
        if isinstance(level, int):
            return _levelToName.get(level)
        elif isinstance(level, str):
            return level.upper()
        else:
            raise TypeError('Expected a string or integer value for level')

# --- Setup Function ---
def setup_logging(
    level: str | int = DEFAULT_LOG_LEVEL, # 允许 int 类型(Allow int type)
    format_ft: str = DEFAULT_LOG_FORMAT,
    service_name: str = DEFAULT_SERVICE_NAME, # 服务名称现在主要用于文件名(Service name mainly for filename now)
    config_env: Optional[str] = None,
    rotation: str = DEFAULT_LOG_ROTATION,
    retention: str = DEFAULT_LOG_RETENTION,
    **kwargs  # 如果需要，将附加参数传递给 loguru.add()[Pass additional arguments to loguru.add() if needed]
):
    """
    为应用程序配置 loguru 记录器。

    参数：
        level（字符串 | int）：输出的最低日志级别（例如，“DEBUG”、“INFO”或 logging.INFO）。
        format_ft（字符串）：日志格式字符串（应包含 {extra[service]}）。
        service_name（字符串）：用于日志文件命名模式的名称。
        config_env（可选[字符串]）：配置环境的名称（例如，“dev”、“prod”）。
        rotation（字符串）：日志文件轮换条件（例如，“100 MB”、“1 天”）。
        retention（字符串）：保留旧日志文件的时间（例如，“7 天”、“1 个月”）。
        **kwargs：直接传递给 logger.add() 的附加关键字参数。

    Configures the loguru logger for the application.

    Args:
        level (str | int): The minimum log level to output (e.g., "DEBUG", "INFO", or logging.INFO).
        format_ft (str): The log format string (should include {extra[service]}).
        service_name (str): Name used for the log filename pattern.
        config_env (Optional[str]): Name of the configuration environment (e.g., 'dev', 'prod').
        rotation (str): Log file rotation condition (e.g., "100 MB", "1 day").
        retention (str): How long to keep old log files (e.g., "7 days", "1 month").
        **kwargs: Additional keyword arguments passed directly to logger.add().
    """
    try:
        logger.remove()
    except ValueError:
        pass

    # +++ Add i18n patcher function +++
    def i18n_patcher(record):
        """
        将翻译功能添加到loguru记录中

        Patches the translation function into the loguru record

        Args:
        record (dict): loguru记录(The loguru record)
        """
        translator = get_translator()  # 获取线程特定的翻译器(Get thread-specific translator)
        # Loguru 将原始消息（格式字符串或字面量）存储在 record["message"] 中
        # 如果传递了参数，则它们存储在 record["parameters"] 中
        # 我们需要翻译格式字符串 record["message"]

        # Loguru stores the original message (format string or literal) in record["message"]
        # If args were passed, they are in record["parameters"]
        # We want to translate the format string record["message"]
        if isinstance(record["message"], str):
            record["message"] = translator(record["message"])
    
    logger.configure(patcher=i18n_patcher) # Apply the patcher

    level_name = get_level_name(level)
    
    current_config_env = config_env if config_env else "base" # Use "base" if None

    def filter_func(record):
        record["extra"].setdefault("service", service_name)
        record["extra"].setdefault("config_env", current_config_env) # Always set a value
        return True

    # 如果意外通过 **kwargs 传递了自定义参数，则清除 kwargs
    # Clean kwargs from our custom parameter if it was accidentally passed via **kwargs
    kwargs_cleaned = kwargs.copy()
    if 'config_env' in kwargs_cleaned:
        del kwargs_cleaned['config_env']

    if SETTINGS.get("log.console", True):
        try:
            logger.add(
                sys.stderr,
                level=level_name,
                format=format_ft,
                colorize=True,
                filter=filter_func,
                **kwargs_cleaned
            )
        except Exception as e:
            logger.error(f"Error adding console logger: {e}", file=sys.stderr)

    if SETTINGS.get("log.file", False):
        log_path: Path = get_folder_path("log")
        file_sink = log_path.joinpath(f"{service_name}_{{time:YYYYMMDD}}.log")
        try:
            logger.add(
                sink=file_sink,
                level=level_name,
                format=format_ft,
                rotation=rotation,
                retention=retention,
                encoding="utf-8",
                enqueue=True,
                filter=filter_func,
                **kwargs_cleaned
            )
        except Exception as e:
             try:
                 logger.error(f"Error adding file logger for path '{file_sink}': {e}")
             except Exception as e_log:
                 logger.error(f"Error adding file logger for path '{file_sink}': {e_log}", file=sys.stderr)
