# utils/config_models.py
import datetime
import os  # For path resolution if needed directly in models (though ConfigManager handles it)
import re  # For vt_symbol validation
from typing import Any, Dict, List, Optional, Tuple

from pydantic import (
    AnyHttpUrl,
    BaseModel,
    FieldValidationInfo,
    FilePath,
    StrictFloat,
    StrictInt,
    constr,
    field_validator,
    root_validator,
)

# Get project root, assuming this file is in utils/
PROJECT_ROOT_FOR_MODELS = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

class PathsConfig(BaseModel):
    data_recording_path: str
    backtest_data_source_path: Optional[str] = None # Optional, will default or be copied
    product_info_ini: FilePath # Pydantic will check if the file exists based on CWD if not resolved
    strategies_config_path: Optional[str] = None # NEW: Optional path to strategies JSON

    # Resolved absolute paths - these will be populated by ConfigManager, not directly from YAML initially
    # So they are optional here from the perspective of initial parsing from YAML dict.
    data_recording_path_abs: Optional[str] = None
    backtest_data_source_path_abs: Optional[str] = None
    product_info_ini_abs: Optional[FilePath] = None # Keep FilePath if we expect it to be valid file post-resolution
    strategies_config_path_abs: Optional[str] = None # NEW: Absolute path for strategies JSON

    # Pydantic's FilePath on 'product_info_ini' will perform an initial existence check
    # relative to the CWD *at the time of model instantiation from the raw dict*.
    # If ConfigManager resolves paths *before* giving the dict to this Pydantic model,
    # then 'product_info_ini' in the dict given to Pydantic might already be absolute,
    # or we might directly populate 'product_info_ini_abs'.
    # The current ConfigManager design resolves paths *after* initial load and merge,
    # stores them in _config_data, and then Pydantic validation would occur.
    # A @root_validator could be used for more complex cross-field validation after resolution.

class ZmqAddressesConfig(BaseModel):
    market_data_pub: str
    market_data_rep: str
    order_gateway_pub: str
    order_gateway_rep: str
    risk_alert_pub: str

    backtest_data_pub: str
    backtest_order_report_pub: str
    backtest_order_request_pull: str

    @field_validator('*', mode='before')
    def check_zmq_protocol(cls, v, info: FieldValidationInfo):
        if not isinstance(v, str):
            # This allows Pydantic to raise its own more specific TypeError if a field expects str but gets non-str
            return v # Pass through to let Pydantic handle basic type error for non-string
        if not (v.startswith("tcp://") or v.startswith("ipc://") or v.startswith("inproc://")):
            raise ValueError(f"ZMQ address for {info.field_name} ('{v}') must start with 'tcp://', 'ipc://', or 'inproc://'")
        return v

class LoggingConfig(BaseModel):
    default_level: str = "INFO"

    @field_validator('default_level', mode='after')
    def level_must_be_valid(cls, value):
        if not isinstance(value, str):
             # Pydantic will handle if it's not a string, this is more for semantic validation.
             # Consider raising TypeError directly if you want to be strict here.
             pass
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if value.upper() not in valid_levels:
            raise ValueError(f"Invalid log level: {value}. Must be one of {valid_levels}")
        return value.upper()

# +++ New SystemConfig model +++
class SystemConfig(BaseModel):
    language: Optional[str] = "en" # Default to English if not specified

    @field_validator('language', mode='before')
    def validate_language_code(cls, v):
        if v is not None: # Only validate if a language is provided
            if not isinstance(v, str):
                raise TypeError("Language code must be a string.")
            # Example validation for specific codes - adjust as needed
            # if v.lower() not in ["en", "zh_cn"]: # Case-insensitive check
            #     raise ValueError(f"Unsupported language code: '{v}'. Supported: en, zh_CN")
        return v
# +++ End New SystemConfig model +++

# +++ New Models for other top-level keys +++
class ServiceSettingsConfig(BaseModel):
    publish_batch_size: int = 1000
    recorder_batch_size: int = 100

class EngineCommunicationConfig(BaseModel):
    market_data_timeout_seconds: float = 30.0
    initial_tick_grace_period_multiplier: float = 2.0
    ping_interval_seconds: float = 5.0
    ping_timeout_ms: int = 2500
    rpc_timeout_ms: int = 3000
    rpc_retries: int = 1
    heartbeat_timeout_seconds: float = 10.0

class OrderSubmissionConfig(BaseModel):
    default_cool_down_seconds: int = 2

# --- Define RiskManagementConfig ---
class RiskManagementConfig(BaseModel):
    max_position_limits: Dict[str, int] = {}
    max_pending_orders_per_contract: int = 5
    global_max_pending_orders: int = 20
    margin_usage_limit: float = 0.8
    futures_trading_sessions: List[Tuple[str, str]] = []

    @field_validator('futures_trading_sessions', mode='before')
    def validate_session_format(cls, sessions):
        if not isinstance(sessions, list):
            return sessions # Let Pydantic handle basic type error
        for item in sessions:
            if not (isinstance(item, (list, tuple)) and len(item) == 2 and 
                    isinstance(item[0], str) and isinstance(item[1], str)):
                raise ValueError("Each session in futures_trading_sessions must be a list/tuple of two strings (e.g., [\"09:00\", \"11:30\"])")
            try:
                # Basic HH:MM format check
                time_format = "%H:%M"
                datetime.datetime.strptime(item[0], time_format)
                datetime.datetime.strptime(item[1], time_format)
            except ValueError:
                raise ValueError(f"Invalid time format in session {item}. Must be HH:MM.")
        return sessions

    @field_validator('margin_usage_limit')
    def margin_must_be_ratio(cls, v):
        if not (0 < v <= 1):
            raise ValueError(f"margin_usage_limit must be between 0 (exclusive) and 1 (inclusive), got {v}")
        return v

# +++ Add BacktesterSettingsConfig +++
class BacktesterSettingsConfig(BaseModel):
    start_date: Optional[str] = None # Or date type if you prefer to parse directly
    end_date: Optional[str] = None   # Or date type
    initial_capital: Optional[float] = None
    # Add other backtester specific settings here with their types
    # slippage_pct: Optional[float] = None
    # transaction_fee_pct: Optional[float] = None

    class Config:
        extra = 'allow' # Allow other arbitrary settings within backtester_settings
# +++ End Add +++

class GlobalConfigStructure(BaseModel):
    logging: LoggingConfig
    paths: PathsConfig
    zmq_addresses: ZmqAddressesConfig
    service_settings: ServiceSettingsConfig
    engine_communication: EngineCommunicationConfig
    order_submission: OrderSubmissionConfig
    default_subscribe_symbols: List[str]
    risk_management: RiskManagementConfig
    backtester_settings: Optional[BacktesterSettingsConfig] = None # NEW
    system: Optional[SystemConfig] = None # MODIFIED: Added system configuration

    # To allow other keys from global_config.yaml to pass through without validation during gradual adoption:
    class Config:
        extra = 'forbid'
        # extra = 'ignore' # Drops extra fields
        # If 'extra' is not set (default is 'forbid'), Pydantic will raise an error for unknown fields.

# Example of how ConfigManager might use this:
# merged_config_dict = ... # This is the dict after YAML load and merge
# try:
#     validated_config = GlobalConfigStructure(**merged_config_dict)
#     # Now use validated_config.paths.data_recording_path, etc.
#     # resolved paths would be done before or after this validation step.
# except ValidationError as e:
#     logger.error(f"Global configuration validation failed:\\\\n{e}")
#     # Handle error: raise, exit, or use defaults 

# +++ Strategy Configuration Models +++
class ThresholdStrategySettingModel(BaseModel):
    entry_threshold: float # Changed from StrictFloat
    profit_target_ticks: StrictInt
    stop_loss_ticks: StrictInt
    price_tick: float # Changed from StrictFloat
    order_volume: float # Changed from StrictFloat
    order_price_offset_ticks: StrictInt

    class Config:
        extra = 'allow' # Allow other strategy-specific settings

class SingleStrategyConfigModel(BaseModel):
    strategy_class: constr(min_length=3) # Basic check for non-empty string
    vt_symbol: constr(min_length=3)      # Basic check for non-empty string
    setting: Dict[str, Any] # Changed from ThresholdStrategySettingModel to allow flexible settings
                                         # In future, could be Union of different setting models or a base model

    @field_validator('strategy_class')
    def strategy_class_format(cls, v):
        if '.' not in v:
            raise ValueError('strategy_class must be a valid Python import path (e.g., module.ClassName)')
        return v

    @field_validator('vt_symbol')
    def vt_symbol_format(cls, v):
        # Basic check for SYMBOL.EXCHANGE format
        if not re.match(r"^[A-Za-z0-9]+(\.[A-Za-z0-9_]+)?\.[A-Z]+$", v):
            raise ValueError(f'vt_symbol "{v}" is not in a valid format (e.g., SA509.CZCE or rb2410_HOT.SHFE)')
        return v

# This will be the top-level model for the entire strategies_setting.json content
AllStrategiesConfigModel = Dict[str, SingleStrategyConfigModel]

# +++ End Strategy Configuration Models +++

# Example of how ConfigManager might use this: 