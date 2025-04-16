import logging
import logging.handlers
import os
import sys
from pathlib import Path

from config.constants.params import Params

# --- Default Configuration ---
DEFAULT_LOG_LEVEL = Params.log_level
# DEFAULT_LOG_FORMAT = "%(asctime)s - %(service)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_LOG_FORMAT = "%(asctime)s - %(service)s - %(levelname)s - %(message)s"
DEFAULT_LOG_DIR = Params.log_dir_name # Relative to project root
DEFAULT_LOG_FILENAME = Params.log_filename
DEFAULT_LOG_MAX_BYTES = 10 * 1024 * 1024  # 10 MB
DEFAULT_LOG_BACKUP_COUNT = 5
DEFAULT_LOG_TO_CONSOLE = True
DEFAULT_LOG_TO_FILE = True

# Cache to prevent multiple configurations in the same process
_is_configured = False

class ServiceNameFilter(logging.Filter):
    """Injects the service name into the log record."""
    def __init__(self, service_name):
        super().__init__()
        self.service_name = service_name

    def filter(self, record):
        record.service = self.service_name
        return True

def setup_logging(
    service_name: str = "default_service",
    level: str | None = None,
    log_to_console: bool | None = None,
    log_to_file: bool | None = None,
    log_dir: str | None = None,
    log_filename: str | None = None,
    log_format: str | None = None,
    max_bytes: int | None = None,
    backup_count: int | None = None,
) -> None:
    """
    Configures the root logger for the application.

    Should be called once at the start of the main script of each service.

    Args:
        service_name: Name of the service/process logging (e.g., 'MarketDataGateway').
        level: Logging level (e.g., 'DEBUG', 'INFO'). Overrides default/env var.
        log_to_console: Force enable/disable console logging.
        log_to_file: Force enable/disable file logging.
        log_dir: Directory for log files. Relative paths are relative to project root.
        log_filename: Name of the log file.
        log_format: Log message format string.
        max_bytes: Maximum size of a log file before rotation.
        backup_count: Number of backup log files to keep.
    """
    global _is_configured
    if _is_configured:
        # Prevent re-configuration in the same process
        # logging.warning("Logger already configured.") # Optional warning
        return

    # Determine configuration values (Args > Env Vars > Defaults)
    log_level_str = level or os.environ.get("LOG_LEVEL", DEFAULT_LOG_LEVEL)
    log_format_str = log_format or os.environ.get("LOG_FORMAT", DEFAULT_LOG_FORMAT)
    should_log_to_console = log_to_console if log_to_console is not None else (os.environ.get("LOG_TO_CONSOLE", str(DEFAULT_LOG_TO_CONSOLE)).lower() == 'true')
    should_log_to_file = log_to_file if log_to_file is not None else (os.environ.get("LOG_TO_FILE", str(DEFAULT_LOG_TO_FILE)).lower() == 'true')
    log_directory = log_dir or os.environ.get("LOG_DIR", DEFAULT_LOG_DIR)
    log_file_name = log_filename or os.environ.get("LOG_FILENAME", DEFAULT_LOG_FILENAME)
    max_log_bytes = max_bytes or int(os.environ.get("LOG_MAX_BYTES", DEFAULT_LOG_MAX_BYTES))
    backup_log_count = backup_count or int(os.environ.get("LOG_BACKUP_COUNT", DEFAULT_LOG_BACKUP_COUNT))

    # --- Configure Root Logger ---
    # Use basicConfig for initial setup if no handlers exist, otherwise add handlers manually.
    # Getting the root logger first is safer if config might be called multiple times across modules
    # although our _is_configured flag should prevent the core logic from running twice.
    root_logger = logging.getLogger()

    # Ensure the root logger level is set correctly (acts as the primary filter)
    numeric_level = getattr(logging, log_level_str.upper(), logging.INFO)
    root_logger.setLevel(numeric_level)

    # Clear existing handlers to avoid duplicates if run in interactive/reloaded environments
    # Be cautious with this in complex scenarios, but often helpful during development.
    # In a stable service, the _is_configured flag is the main guard.
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close()


    # --- Create Formatter ---
    formatter = logging.Formatter(log_format_str)

    # --- Add Service Name Filter ---
    service_filter = ServiceNameFilter(service_name)

    # --- Console Handler ---
    if should_log_to_console:
        console_handler = logging.StreamHandler(sys.stdout) # Log INFO and above to stdout
        console_handler.setFormatter(formatter)
        console_handler.addFilter(service_filter) # Add filter to this handler
        # Set level for the handler itself (optional, root logger level usually controls)
        console_handler.setLevel(numeric_level)
        root_logger.addHandler(console_handler)

    # --- Rotating File Handler ---
    if should_log_to_file:
        # Ensure log directory exists (relative to project root)
        # Assuming this script is run from project root or path is adjusted
        project_root = Path(__file__).parent.parent # Go up two levels from logger/__init__.py
        log_path = project_root / log_directory
        try:
            log_path.mkdir(parents=True, exist_ok=True)
            log_file_full_path = log_path / log_file_name

            file_handler = logging.handlers.RotatingFileHandler(
                filename=log_file_full_path,
                maxBytes=max_log_bytes,
                backupCount=backup_log_count,
                encoding='utf-8' # Explicitly use utf-8 for files
            )
            file_handler.setFormatter(formatter)
            file_handler.setLevel(numeric_level)
            file_handler.addFilter(service_filter) # Add filter to this handler
            root_logger.addHandler(file_handler)
        except Exception as e:
            logging.error(f"Failed to configure file logging to {log_path / log_file_name}: {e}", exc_info=True)


    # --- Mark as configured ---
    _is_configured = True
    logging.info(f"Logging configured for service '{service_name}'. Level: {log_level_str}, Console: {should_log_to_console}, File: {should_log_to_file}")

# --- Convenience Re-export ---
# Allow users to import getLogger directly from this module
from logging import getLogger 