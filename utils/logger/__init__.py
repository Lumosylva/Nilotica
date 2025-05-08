#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : __init__.py
@Date       : 2025/4/28 20:28
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 日志模块
"""
import sys
from pathlib import Path
from typing import Optional

from loguru import logger

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
DEFAULT_SERVICE_NAME = "nilotica_default" # Default service name if not patched
DEFAULT_LOG_ROTATION = "100 MB"  # Rotate when file exceeds 100MB
DEFAULT_LOG_RETENTION = "7 days" # Keep logs for 7 days

# --- Global Logger Object ---
# Re-export the logger object from loguru
__all__ = ["logger", "setup_logging", "INFO", "DEBUG", "WARNING", "WARN", "ERROR", "FATAL", "CRITICAL", "NOTSET"]

# Read log level from vnpy settings
DEFAULT_LOG_LEVEL: str = SETTINGS.get("log.level", "INFO") # Use .get for safety


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
    level: str | int = DEFAULT_LOG_LEVEL, # Allow int type
    format_ft: str = DEFAULT_LOG_FORMAT,
    service_name: str = DEFAULT_SERVICE_NAME, # Service name mainly for filename now
    config_env: Optional[str] = None,
    rotation: str = DEFAULT_LOG_ROTATION,
    retention: str = DEFAULT_LOG_RETENTION,
    **kwargs  # Pass additional arguments to loguru.add() if needed
):
    """
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

    level_name = get_level_name(level)
    
    current_config_env = config_env if config_env else "base" # Use "base" if None

    def filter_func(record):
        record["extra"].setdefault("service", service_name)
        record["extra"].setdefault("config_env", current_config_env) # Always set a value
        return True

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
            print(f"Error adding console logger: {e}", file=sys.stderr)

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
                 print(f"Error adding file logger for path '{file_sink}': {e_log}", file=sys.stderr)
