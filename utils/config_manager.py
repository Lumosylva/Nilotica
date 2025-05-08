import yaml
import os
import json
from typing import Any, Dict, Optional, List
import copy
from pydantic import BaseModel, ValidationError, parse_obj_as

from utils.logger import logger

# +++ Import Pydantic models +++
from utils.config_models import GlobalConfigStructure, AllStrategiesConfigModel # +++ Add AllStrategiesConfigModel
from vnpy.trader.utility import load_json

# --- Constants ---
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
GLOBAL_CONFIG_FILENAME = "global_config.yaml"
DEFAULT_STRATEGIES_CONFIG_FILENAME = "strategies_setting.json" # Default filename

STRATEGIES_CONFIG = {  }
STRATEGIES_CONFIG_FILENAME: str = "strategies_setting.json"
STRATEGIES_CONFIG.update(load_json(STRATEGIES_CONFIG_FILENAME))

DEFAULT_GLOBAL_CONFIG_PATH = os.path.join(PROJECT_ROOT, "config", GLOBAL_CONFIG_FILENAME)
ENV_CONFIG_FILENAME_TEMPLATE = "{env}_config.yaml"


class ConfigManager:
    """
    Manages loading and accessing system configurations from YAML and JSON files,
    supporting environment-specific overrides and Pydantic validation.
    """
    def __init__(
        self,
        global_config_path: str = DEFAULT_GLOBAL_CONFIG_PATH,
        environment: Optional[str] = None
    ):
        """
        Initializes the ConfigManager.

        Args:
            global_config_path: Path to the base global configuration file.
            environment: The name of the environment (e.g., 'dev', 'prod', 'backtest').
                         If provided, loads and merges {environment}_config.yaml.
        """
        self._global_config_path = global_config_path
        self._environment = environment
        self._env_config_path: Optional[str] = None
        self._config_data: GlobalConfigStructure | Dict[str, Any] = {} # Can be model or dict if validation fails
        self._strategies_data: Dict[str, Any] = {} # Will store validated strategy data (or raw if validation fails)
        
        self._load_configs()

    @staticmethod
    def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively merges override dict into a **copy** of the base dict.
        Override values take precedence.
        """
        merged = copy.deepcopy(base) # Start with a deep copy of the base
        for key, value in override.items():
            if isinstance(value, dict) and key in merged and isinstance(merged[key], dict):
                # If both values are dicts, recurse
                merged[key] = ConfigManager._deep_merge(merged[key], value)
            else:
                # Otherwise, override value in merged copy
                merged[key] = value
        return merged

    @staticmethod
    def _load_yaml(file_path: str) -> Dict[str, Any]:
        """
        Loads a YAML file.
        """
        if not os.path.exists(file_path):
            # Consider raising a custom ConfigError or FileNotFoundError
            logger.error(f"Configuration file not found: {file_path}")
            return {}
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            logger.info(f"Successfully loaded YAML configuration from: {file_path}")
            return data if data else {}
        except yaml.YAMLError as e:
            logger.error(f"Failed to parse YAML file {file_path}: {e}")
            return {}
        except IOError as e:
            logger.error(f"Failed to read file {file_path}: {e}")
            return {}

    @staticmethod
    def _load_json(file_path: str) -> Dict[str, Any]:
        """
        Loads a JSON file.
        """
        if not os.path.exists(file_path):
            logger.info(f"Optional JSON configuration file not found: {file_path}")
            return {}
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            logger.info(f"Successfully loaded JSON configuration from: {file_path}")
            return data if data else {}
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON file {file_path}: {e}")
            return {}
        except IOError as e:
            logger.error(f"Failed to read file {file_path}: {e}")
            return {}

    def _load_configs(self):
        """Loads base config and environment-specific config, validates, then merges."""
        logger.info(f"Project root determined as: {PROJECT_ROOT}")
        
        # 1. Load Base Global Config
        base_config = self._load_yaml(self._global_config_path)
        if not base_config:
             logger.warning(f"Base global config ({self._global_config_path}) failed to load or is empty.")

        # 2. Load Environment-Specific Config (if environment is set)
        env_specific_config = {}
        if self._environment:
            env_filename = ENV_CONFIG_FILENAME_TEMPLATE.format(env=self._environment)
            self._env_config_path = os.path.join(PROJECT_ROOT, "config", env_filename)
            logger.info(f"Attempting to load environment config for '{self._environment}' from: {self._env_config_path}")
            env_specific_config = self._load_yaml(self._env_config_path)
            if not env_specific_config:
                 logger.info(f"Environment config file ({self._env_config_path}) not found or empty. Will use base config values for overridden keys if any.")
        else:
             logger.info("No environment specified, using base config only.")

        # 3. Merge Configurations
        merged_config_data: Dict[str, Any] = base_config
        if env_specific_config: # Only merge if env_specific_config has content
            logger.info(f"Merging environment config ('{self._environment}') into base config.")
            merged_config_data = self._deep_merge(base_config, env_specific_config)
        
        # +++ 4. Validate merged configuration using Pydantic model +++
        validated_model_instance = None
        if merged_config_data: # Proceed only if there's some config data to validate
            try:
                # Pass the merged dictionary to the Pydantic model for validation
                # The **merged_config_data unpacks the dictionary into keyword arguments
                validated_model_instance = GlobalConfigStructure(**merged_config_data)
                # If validation is successful, store the Pydantic model instance
                self._config_data = validated_model_instance 
                logger.info("Global configuration successfully validated and loaded into Pydantic model.")
            except ValidationError as e:
                logger.error(f"Global configuration validation failed! Errors:\n{e}")
                # Option A: Exit the program on validation failure
                logger.critical("Critical configuration errors. Aborting program startup.")
                raise SystemExit("Configuration validation failed. Check logs for details.")
                # Option B: Fallback to unvalidated data (less safe)
                # logger.warning("Using unvalidated configuration data due to validation errors.")
                # self._config_data = merged_config_data # Store the raw dict if falling back
            except Exception as e_generic:
                logger.error(f"An unexpected error occurred during Pydantic model instantiation for global config: {e_generic}")
                logger.critical("Critical error during global configuration processing. Aborting program startup.")
                raise SystemExit("Unexpected error during global configuration processing.")
        else:
            logger.warning("Merged global configuration data is empty. No Pydantic validation performed.")
            self._config_data = {} # Ensure _config_data is an empty dict
        # --- End Pydantic Validation ---

        # 5. Load Strategies Config (remains a dict for now, Pydantic model can be added later)
        # self._strategies_data = STRATEGIES_CONFIG # This is a simple dict assignment # REMOVE OLD LINE

        # 6. Resolve paths AFTER validation (if successful and self._config_data is a Pydantic model)
        if isinstance(self._config_data, GlobalConfigStructure):
            temp_dict_for_path_resolution = self._config_data.model_dump() 
            self._resolve_paths_on_dict(temp_dict_for_path_resolution) # This adds _abs keys to temp_dict_for_path_resolution
            try:
                # Update the model instance with the dictionary that now includes resolved paths
                self._config_data = GlobalConfigStructure(**temp_dict_for_path_resolution)
                logger.info("Global Pydantic model re-validated and updated with resolved absolute paths.")
            except ValidationError as e:
                logger.error(f"Error re-validating global config model after path resolution: {e}. Resolved paths might not be accessible via Pydantic model.")
                # If re-validation fails, self._config_data remains the model instance *before* path resolution tried to update it.
                # Access to resolved paths would then rely on the dictionary-based fallback in get_global_config or separate storage.
                # For now, we log the error and proceed. The original Pydantic object (pre-path-resolution) is still in self._config_data.
                # To be safer, we might revert self._config_data to a dict here or handle more gracefully.
                # However, the _abs fields are Optional in the model, so this re-validation should ideally pass if _resolve_paths_on_dict works correctly.

        elif isinstance(self._config_data, dict) and self._config_data: # If initial validation failed and we used raw dict (not current path)
            self._resolve_paths_on_dict(self._config_data) # Resolve paths on the raw dict

        # --- Load and Validate Strategies Config --- 
        raw_strategies_data = {}
        # Try loading from configured path first
        configured_strategies_path_abs = None
        if isinstance(self._config_data, GlobalConfigStructure) and hasattr(self._config_data, 'paths') and self._config_data.paths:
            configured_strategies_path_abs = getattr(self._config_data.paths, 'strategies_config_path_abs', None)
        elif isinstance(self._config_data, dict) and 'paths' in self._config_data and isinstance(self._config_data['paths'], dict):
            configured_strategies_path_abs = self._config_data['paths'].get('strategies_config_path_abs')

        if configured_strategies_path_abs:
            logger.info(f"Attempting to load strategies configuration from configured path: {configured_strategies_path_abs}")
            raw_strategies_data = self._load_json(configured_strategies_path_abs) # Use internal _load_json for consistency
            if not raw_strategies_data:
                logger.warning(f"Strategies config file specified at '{configured_strategies_path_abs}' was not found or is empty.")

        # If not loaded from configured path (or path not set), try default user location
        if not raw_strategies_data:
            logger.info(f"No strategies loaded from specific config path, or path not set. Trying default user location for '{DEFAULT_STRATEGIES_CONFIG_FILENAME}'...")
            raw_strategies_data = load_json(DEFAULT_STRATEGIES_CONFIG_FILENAME)
        
        if raw_strategies_data: # load_json returns {} if file not found or empty, _load_json also does
            try:
                validated_strategies = parse_obj_as(AllStrategiesConfigModel, raw_strategies_data)
                self._strategies_data = {name: model.model_dump() for name, model in validated_strategies.items()}
                logger.info(f"Strategies configuration successfully validated and loaded.")
            except ValidationError as e_strat:
                logger.error(f"Strategies configuration validation failed! Errors:\n{e_strat}")
                logger.warning("Using unvalidated raw strategies data due to validation errors. Behavior might be unpredictable.")
                self._strategies_data = raw_strategies_data # Fallback to raw
            except Exception as e_strat_generic:
                logger.error(f"An unexpected error occurred during Pydantic model instantiation for strategies config: {e_strat_generic}")
                self._strategies_data = raw_strategies_data # Fallback to raw
        else:
            logger.warning(f"No strategies config file found or is empty (checked specific path and default). No strategies loaded.")
            self._strategies_data = {} # Ensure it's an empty dict
        # --- End Load and Validate Strategies Config ---

    @staticmethod
    # Renamed original _resolve_paths to avoid confusion
    def _resolve_paths_on_dict(config_dict_to_modify: Dict[str, Any]):
        """Resolves relative paths in the configuration dictionary to absolute paths."""
        paths_section = config_dict_to_modify.get('paths')
        if not isinstance(paths_section, dict):
            # If paths config is entirely missing, Pydantic would have caught it if PathsConfig was not Optional itself.
            # Assuming GlobalConfigStructure.paths is not Optional for now.
            logger.warning("'paths' section is missing or not a dictionary in the configuration. Cannot resolve paths.")
            return

        def _resolve_single_path(key_rel: str, key_abs: str, section: dict):
            rel_path = section.get(key_rel)
            if rel_path:
                abs_p = os.path.join(PROJECT_ROOT, rel_path) if not os.path.isabs(rel_path) else rel_path
                section[key_abs] = os.path.normpath(abs_p)
                logger.info(f"Resolved '{key_rel}': {rel_path} -> {section.get(key_abs)}")
            return section.get(key_abs)

        _resolve_single_path('data_recording_path', 'data_recording_path_abs', paths_section)
        
        # Resolve backtest_data_source_path, defaulting to data_recording_path_abs if not specified
        resolved_backtest_path = _resolve_single_path('backtest_data_source_path', 'backtest_data_source_path_abs', paths_section)
        if not resolved_backtest_path and paths_section.get('data_recording_path_abs'):
            paths_section['backtest_data_source_path_abs'] = paths_section['data_recording_path_abs']
            logger.info(f"'backtest_data_source_path' defaults to 'data_recording_path_abs': {paths_section.get('backtest_data_source_path_abs')}")

        _resolve_single_path('product_info_ini', 'product_info_ini_abs', paths_section)
        _resolve_single_path('strategies_config_path', 'strategies_config_path_abs', paths_section) # NEW

    def get_global_config(self, key: str, default: Any = None) -> Any:
        keys = key.split('.')
        value = self._config_data
        try:
            if isinstance(value, GlobalConfigStructure):
                # Access attributes on the Pydantic model
                for k in keys:
                    # Check if k is an attribute of the current Pydantic model object (value)
                    if hasattr(value, k):
                        value = getattr(value, k)
                        # If the new value is itself a Pydantic model, loop continues to access its attributes.
                        # If it's a final value (e.g. str, int, list, non-Pydantic dict), it will be returned.
                    else:
                        # If key is not a direct attribute, it could be an extra field if Config.extra = 'allow'
                        # and the value is a dict. Or Pydantic V1 model access via .dict()
                        if isinstance(value, BaseModel) and value.model_extra and k in value.model_extra:
                             value = value.model_extra[k]
                        elif isinstance(value, dict) and k in value: # Fallback for dicts within Pydantic model if not sub-model
                             value = value[k]
                        else:
                             return default # Key not found as an attribute or extra
                return value
            elif isinstance(value, dict):
                # Original dictionary access
                for k in keys:
                        value = value[k]
                return value
            else:
                logger.warning(f"Config data (type: {type(value)}) is neither a Pydantic model nor a dict. Cannot get key '{key}'.")
            return default
        except (KeyError, AttributeError, TypeError):
            # logger.warning(f"Error accessing key '{key}'. Returning default: {default}")
            return default

    def get_all_global_configs(self) -> Dict[str, Any]:
        if isinstance(self._config_data, GlobalConfigStructure):
            if hasattr(self._config_data, 'model_dump'):
                return self._config_data.model_dump(exclude_unset=False, by_alias=False) # Pydantic V2+, include all fields
            else:
                return self._config_data.dict(exclude_unset=False, by_alias=False) # Pydantic V1, include all fields
        elif isinstance(self._config_data, dict):
            return self._config_data.copy()
        logger.warning("Cannot get all global configs: _config_data is not a Pydantic model or dict.")
        return {}

    def get_strategies_config(self) -> Dict[str, Any]:
        """Returns the loaded strategies configuration."""
        return self._strategies_data.copy() # Return a copy

    def get_data_recording_path(self) -> Optional[str]:
        """Convenience method to get the absolute data recording path."""
        return self.get_global_config("paths.data_recording_path_abs")

    def get_backtest_data_source_path(self) -> Optional[str]:
        """Convenience method to get the absolute backtest data source path."""
        return self.get_global_config("paths.backtest_data_source_path_abs")

    # +++ Add convenience method for product info path +++
    def get_product_info_path(self) -> Optional[str]:
        """Convenience method to get the absolute product info INI path."""
        return self.get_global_config("paths.product_info_ini_abs")

# Example Usage (for testing this module directly)
if __name__ == '__main__':
    print(f"Project Root from ConfigManager module: {PROJECT_ROOT}")
    # Test with default paths and no environment
    manager_no_env = ConfigManager()
    print("\n--- No Environment (Base Global Config) ---")
    print(f"Market Data Pub: {manager_no_env.get_global_config('zmq_addresses.market_data_pub')}")
    print(f"Strategy SA509_Trend_1 profit_target_ticks: {manager_no_env.get_strategies_config().get('SA509_Trend_1', {}).get('setting', {}).get('profit_target_ticks')}")
    
    print("\n--- Development Environment ('dev') ---")
    # Ensure config/dev_config.yaml exists and potentially overrides some values
    # Also ensure config/strategies_setting.json exists (or a path is specified in dev_config.yaml)
    manager_dev = ConfigManager(environment="dev")
    print(f"Market Data Pub (dev): {manager_dev.get_global_config('zmq_addresses.market_data_pub')}")
    print(f"Default Log Level (dev): {manager_dev.get_global_config('logging.default_level')}")
    dev_strategies = manager_dev.get_strategies_config()
    print(f"Strategy SA509_Trend_1 order_volume (dev): {dev_strategies.get('SA509_Trend_1', {}).get('setting', {}).get('order_volume')}")

    # Test accessing a Pydantic model field directly (if validation passed)
    if isinstance(manager_dev._config_data, GlobalConfigStructure):
        print(f"Accessing via Pydantic model: dev logging level = {manager_dev._config_data.logging.default_level}")
    else:
        print("Config data is not a Pydantic model instance for dev.")

    # Test with a missing global config
    # print("\n--- Test Missing Global Config ---")
    # manager_missing = ConfigManager(global_config_path="config/non_existent_global.yaml")
    # print(f"Market Data Pub (missing global): {manager_missing.get_global_config('zmq_addresses.market_data_pub')}") # Should be None or default
    # print(f"Strategies (missing global): {manager_missing.get_strategies_config()}") # Should be empty

    print("\n--- Backtest Environment ('backtest') ---")
    # Ensure config/backtest_config.yaml exists and is populated
    manager_backtest = ConfigManager(environment="backtest")
    if manager_backtest._config_data: # Check if config data was loaded
        print(f"Product Info Path (backtest): {manager_backtest.get_global_config('paths.product_info_ini_abs')}")
        print(f"Strategies Config Path (backtest): {manager_backtest.get_global_config('paths.strategies_config_path_abs')}")
        print(f"Default Log Level (backtest): {manager_backtest.get_global_config('logging.default_level')}")
        print(f"Backtester Start Date (backtest): {manager_backtest.get_global_config('backtester_settings.start_date')}")
        backtest_strategies = manager_backtest.get_strategies_config()
        print(f"Loaded Backtest Strategies: {backtest_strategies}")
        if backtest_strategies:
            print(f"Example Backtest Strategy 'BACKTEST_EXAMPLE_Strategy_1' setting 'param1': {backtest_strategies.get('BACKTEST_EXAMPLE_Strategy_1', {}).get('setting', {}).get('param1')}")
    else:
        print("Failed to load or validate configuration for backtest environment.")

    # How to use in other modules:
    # from utils.config_manager import ConfigManager
    #
    # # Initialize once, perhaps in your main service script
    # config_service = ConfigManager()
    #
    # # Access values
    # md_pub_url = config_service.get_global_config("zmq_addresses.market_data_pub", "tcp://localhost:5555")
    # recording_path = config_service.get_data_recording_path()
    # strategy_configs = config_service.get_strategies_config() 