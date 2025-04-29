#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica_dev
@FileName   : __init__.py
@Date       : 2025/4/28 20:28
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: description
"""
import sys
from pathlib import Path

from loguru import logger

from vnpy.trader.setting import SETTINGS
from vnpy.trader.utility import get_folder_path

# --- Default Configuration ---
DEFAULT_LOG_FORMAT = (
    "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> - "
    "<level>{level}</level> - " # Adjusted level padding
    "<cyan>{extra[service]}</cyan> - " # Added service name placeholder from extra
    "<level>{message}</level>"
)
DEFAULT_SERVICE_NAME = "nilotica_default" # Default service name if not patched
DEFAULT_LOG_ROTATION = "100 MB"  # Rotate when file exceeds 100MB
DEFAULT_LOG_RETENTION = "7 days" # Keep logs for 7 days

# --- Global Logger Object ---
# Re-export the logger object from loguru
__all__ = ["logger", "setup_logging"]

# Read log level from vnpy settings
DEFAULT_LOG_LEVEL: str = SETTINGS.get("log.level", "INFO") # Use .get for safety


def int_level_to_str(level: int) -> str:
    level_mapping = {
        50: "CRITICAL",
        40: "ERROR",
        30: "WARNING",
        20: "INFO",
        10: "DEBUG",
        0: "NOTSET"
    }
    return level_mapping.get(level, "UNKNOWN")

# --- Setup Function ---
def setup_logging(
    level: str | int = DEFAULT_LOG_LEVEL, # Allow int type
    format_ft: str = DEFAULT_LOG_FORMAT,
    service_name: str = DEFAULT_SERVICE_NAME, # Service name mainly for filename now
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
        rotation (str): Log file rotation condition (e.g., "100 MB", "1 day").
        retention (str): How long to keep old log files (e.g., "7 days", "1 month").
        **kwargs: Additional keyword arguments passed directly to logger.add().
    """
    # Remove default handler to avoid potential duplicates if called multiple times
    # or if loguru was imported elsewhere before setup
    try:
        logger.remove()
    except ValueError:
        # No handlers configured yet, safe to continue
        pass

    # --- Process log level for Loguru --- 
    if isinstance(level, str):
        level = level.upper()
    # If level is already an int (like logging.INFO), use it directly
    if isinstance(level, int):
        level = int_level_to_str(level)

    # --- Configure Console Logging ---
    # Read console setting from vnpy settings
    if SETTINGS.get("log.console", True): # Use .get for safety
        try:
            logger.add(
                sys.stderr,
                level=level, # Use processed level
                format=format_ft,
                colorize=True,  # Enable colors for console output
                # Add default service name to extra for logs before patch or outside patched context
                filter=lambda record: record["extra"].setdefault("service", service_name) is not None,
                **kwargs
            )
        except Exception as e:
            print(f"Error adding console logger: {e}", file=sys.stderr)

    # --- Configure File Logging ---
    # Read file logging setting from vnpy settings
    if SETTINGS.get("log.file", False): # Use .get for safety
        log_path: Path = get_folder_path("log") # Get log directory from vnpy
        # Use service_name in the filename pattern directly
        file_sink = log_path.joinpath(f"{service_name}_{{time:YYYYMMDD}}.log")
        try:
            logger.add(
                sink=file_sink,
                level=level, # Use processed level
                format=format_ft,
                rotation=rotation,
                retention=retention,
                encoding="utf-8",
                enqueue=True,  # Make file logging asynchronous for better performance
                # Add default service name to extra for logs before patch or outside patched context
                filter=lambda record: record["extra"].setdefault("service", service_name) is not None,
                **kwargs
            )
        except Exception as e:
             # Use logger itself if possible, otherwise print
             try:
                 logger.error(f"Error adding file logger for path '{file_sink}': {e}")
             except Exception as e:
                 print(f"Error adding file logger for path '{file_sink}': {e}", file=sys.stderr)

    # Initialize extra dict for safety, though patch should handle it
    # logger.configure(extra={"service": DEFAULT_SERVICE_NAME})

# --- Example of initial configuration (optional, can be removed if setup is always called explicitly) ---
# setup_logging() # Sets up basic INFO level logging to console by default when module is imported
# --- Patch logger to include service name in 'extra' ---
# All logs generated after this line within this scope will have the service name
# logger.patch(lambda record: record["extra"].update(service=DEFAULT_SERVICE_NAME)).info(
#     f"Logging patched with service name: {DEFAULT_SERVICE_NAME}"
# )


