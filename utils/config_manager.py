import configparser
import copy
import json
import os
from typing import Any, Dict, Optional, Tuple

import yaml
from pydantic import BaseModel, ValidationError, TypeAdapter

from config.constants.params import Params
from config.constants.path import GlobalPath
from utils.config_models import AllStrategiesConfigModel, GlobalConfigStructure
from utils.i18n import _, setup_language
from utils.logger import logger, setup_logging, INFO
from vnpy.trader.utility import load_json

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

GLOBAL_CONFIG_FILENAME = Params.global_config_filename
DEFAULT_STRATEGIES_CONFIG_FILENAME: str = Params.strategy_setting_filename
# 加载默认策略配置文件(Load the default policy configuration file)
STRATEGIES_CONFIG = {  }
STRATEGIES_CONFIG.update(load_json(DEFAULT_STRATEGIES_CONFIG_FILENAME))

DEFAULT_GLOBAL_CONFIG_PATH = GlobalPath.global_config_filepath
ENV_CONFIG_FILENAME_TEMPLATE = "{env}_config.yaml"


class ConfigManager:
    """
    管理从 YAML 和 JSON 文件加载和访问系统配置，支持特定于环境的覆盖和 Pydantic 验证。

    Manages loading and accessing system configurations from YAML and JSON files,
    supporting environment-specific overrides and Pydantic validation.
    """
    def __init__(
        self,
        global_config_path: str = DEFAULT_GLOBAL_CONFIG_PATH,
        environment: Optional[str] = None
    ):
        """
        初始化 ConfigManager。

        Initializes the ConfigManager.
        Args:
            global_config_path：基本全局配置文件的路径。
            环境：环境的名称（例如，"dev"、"prod"、"backtest"）。
            如果提供，则加载并合并{environment}_config.yaml。

            global_config_path: Path to the base global configuration file.
            environment: The name of the environment(e.g., 'dev', 'prod', 'backtest').
            If provided, loads and merges {environment}_config.yaml.
        """
        self._global_config_path = global_config_path
        self._environment = environment
        self._env_config_path: Optional[str] = None
        self._config_data: GlobalConfigStructure | Dict[str, Any] = {} # Can be model or dict if validation fails
        self._strategies_data: Dict[str, Any] = {} # Will store validated strategy data (or raw if validation fails)
        setup_logging(service_name=self.__class__.__name__)
        self._load_configs()

    @staticmethod
    def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        递归地将覆盖字典合并到基础字典的副本中。
        覆盖值优先。

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

    @staticmethod
    def _load_json(file_path: str) -> Dict[str, Any]:
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


    def _load_configs(self):
        """
        加载基本配置和特定于环境的配置，验证，然后合并。
        在配置加载后设置语言。

        Loads base config and environment-specific config, validates, then merges.
        Sets up language after configurations are loaded.
        """
        # 1. Load Base Global Config
        base_config = self._load_yaml(self._global_config_path)
        if not base_config:
             logger.warning("The base global configuration ({}) failed to load or is empty.".format(self._global_config_path))

        language_to_set = "en"  # 默认语言
        # 从配置中获取语言设置，并调用 setup_language
        if base_config and isinstance(base_config.get("system"), dict):
            language_to_set = base_config["system"].get("language", "")

        # 调用 setup_language 来设置全局翻译
        # 此时，如果 setup_language 内部有日志，它们将使用该函数内部定义的 _
        # setup_language 成功后，全局 _ 将使用正确的翻译器
        setup_language(language_to_set, PROJECT_ROOT)

        # 2. Load Environment-Specific Config (if environment is set)
        env_specific_config = {}
        if self._environment:
            env_filename = ENV_CONFIG_FILENAME_TEMPLATE.format(env=self._environment)
            self._env_config_path = os.path.join(PROJECT_ROOT, "config", env_filename)
            logger.info(_("尝试从以下位置加载环境 '{}' 的配置: {}").format(self._environment, self._env_config_path))
            env_specific_config = self._load_yaml(self._env_config_path)
            if not env_specific_config:
                 logger.info(_("环境配置文件 ({}) 未找到或为空。如果有任何被覆盖的键，将使用基本配置值。").format(self._env_config_path))
        else:
             logger.info(_("未指定环境，仅使用基本配置。"))

        # 3. Merge Configurations
        merged_config_data: Dict[str, Any] = base_config
        if env_specific_config: # Only merge if env_specific_config has content
            logger.info(_("将环境配置 ('{}') 合并到基本配置中。").format(self._environment))
            merged_config_data = self._deep_merge(base_config, env_specific_config)

        # +++ 4. Validate merged configuration using Pydantic model +++
        if merged_config_data: # Proceed only if there's some config data to validate
            try:
                # Pass the merged dictionary to the Pydantic model for validation
                # The **merged_config_data unpacks the dictionary into keyword arguments
                validated_model_instance = GlobalConfigStructure(**merged_config_data)
                # If validation is successful, store the Pydantic model instance
                self._config_data = validated_model_instance 
                logger.info(_("全局配置已成功验证并加载到 Pydantic 模型中。"))
            except ValidationError as e:
                logger.error(_("全局配置验证失败！错误: {}").format(e))
                # Option A: Exit the program on validation failure
                logger.critical(_("关键配置错误。中止程序启动。"))
                raise SystemExit(_("配置验证失败。查看日志了解详情。"))
                # Option B: Fallback to unvalidated data (less safe)
                # logger.warning("Using unvalidated configuration data due to validation errors.")
                # self._config_data = merged_config_data # Store the raw dict if falling back
            except Exception as e_generic:
                logger.error(_("在全局配置的 Pydantic 模型实例化过程中发生意外错误: {}").format(e_generic))
                logger.critical(_("全局配置处理过程中发生关键错误。中止程序启动。"))
                raise SystemExit(_("全局配置处理过程中发生意外错误。"))
        else:
            logger.warning(_("合并的全局配置数据为空。未执行 Pydantic 验证。"))
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
                logger.info(_("全局 Pydantic 模型已重新验证并更新了已解析的绝对路径。"))
            except ValidationError as e:
                logger.error(_("路径解析后重新验证全局配置模型时出错: {}。可能无法通过 Pydantic 模型访问已解析的路径。").format(e))
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
            logger.info(_("尝试从配置的路径加载策略配置: {}").format(configured_strategies_path_abs))
            raw_strategies_data = self._load_json(configured_strategies_path_abs) # Use internal _load_json for consistency
            if not raw_strategies_data:
                logger.warning(_("在 '{}' 指定的策略配置文件未找到或为空。").format(configured_strategies_path_abs))

        # If not loaded from configured path (or path not set), try default user location
        if not raw_strategies_data:
            logger.info(_("未从特定配置路径加载策略，或路径未设置。尝试默认用户位置 '{}'...").format(DEFAULT_STRATEGIES_CONFIG_FILENAME))
            raw_strategies_data = load_json(DEFAULT_STRATEGIES_CONFIG_FILENAME)
        
        if raw_strategies_data: # load_json returns {} if file not found or empty, _load_json also does
            try:
                validated_strategies = TypeAdapter(AllStrategiesConfigModel).validate_python(raw_strategies_data)
                self._strategies_data = {name: model.model_dump() for name, model in validated_strategies.items()}
                logger.info(_("策略配置已成功验证并加载。"))
            except ValidationError as e_strat:
                logger.error(_("策略配置验证失败！错误: {}").format(e_strat))
                logger.warning(_("由于验证错误，使用未验证的原始策略数据。行为可能不可预测。"))
                self._strategies_data = raw_strategies_data # Fallback to raw
            except Exception as e_strat_generic:
                logger.error(_("在策略配置的 Pydantic 模型实例化过程中发生意外错误: {}").format(e_strat_generic))
                self._strategies_data = raw_strategies_data # Fallback to raw
        else:
            logger.warning(_("未找到策略配置文件或为空（已检查特定路径和默认路径）。未加载任何策略。"))
            self._strategies_data = {} # Ensure it's an empty dict
        # --- End Load and Validate Strategies Config ---

    @staticmethod
    def _resolve_paths_on_dict(config_dict_to_modify: Dict[str, Any]):
        """
        将配置字典中的相对路径解析为绝对路径。

        Resolves relative paths in the configuration dictionary to absolute paths.
        """
        paths_section = config_dict_to_modify.get('paths')
        if not isinstance(paths_section, dict):
            # If paths config is entirely missing, Pydantic would have caught it if PathsConfig was not Optional itself.
            # Assuming GlobalConfigStructure.paths is not Optional for now.
            logger.warning(_("配置中缺少 'paths' 部分或不是字典。无法解析路径。"))
            return

        def _resolve_single_path(key_rel: str, key_abs: str, section: dict):
            rel_path = section.get(key_rel)
            if rel_path:
                abs_p = os.path.join(PROJECT_ROOT, rel_path) if not os.path.isabs(rel_path) else rel_path
                section[key_abs] = os.path.normpath(abs_p)
                logger.info(_("已解析 '{}': {} -> {}").format(key_rel, rel_path, section.get(key_abs)))
            return section.get(key_abs)

        _resolve_single_path('data_recording_path', 'data_recording_path_abs', paths_section)
        
        # Resolve backtest_data_source_path, defaulting to data_recording_path_abs if not specified
        resolved_backtest_path = _resolve_single_path('backtest_data_source_path', 'backtest_data_source_path_abs', paths_section)
        if not resolved_backtest_path and paths_section.get('data_recording_path_abs'):
            paths_section['backtest_data_source_path_abs'] = paths_section['data_recording_path_abs']
            logger.info(_("'backtest_data_source_path' 默认为 'data_recording_path_abs': {}").format(paths_section.get('backtest_data_source_path_abs')))

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
                logger.warning(_("配置数据（类型: {}）既不是 Pydantic 模型也不是字典。无法获取键 '{}'。").format(type(value), key))
            return default
        except (KeyError, AttributeError, TypeError):
            logger.warning(_("访问键 '{}' 时出错。返回默认值：{}").format(key, default))
            return default

    def get_all_global_configs(self) -> Dict[str, Any]:
        if isinstance(self._config_data, GlobalConfigStructure):
            if hasattr(self._config_data, 'model_dump'):
                return self._config_data.model_dump(exclude_unset=False, by_alias=False) # Pydantic V2+, include all fields
            else:
                # Pydantic V1, or if model_dump is somehow not available (though it should be for BaseModel)
                return self._config_data.model_dump(exclude_unset=False, by_alias=False) # MODIFIED: Use model_dump
        elif isinstance(self._config_data, dict):
            return self._config_data.copy()
        logger.warning(_("无法获取所有全局配置：_config_data 不是 Pydantic 模型或字典。"))
        return {}

    def get_strategies_config(self) -> Dict[str, Any]:
        """
        返回已加载的策略配置。

        Returns the loaded strategies configuration.
        """
        return self._strategies_data.copy() # Return a copy

    def get_data_recording_path(self) -> Optional[str]:
        """
        获取绝对数据记录路径的便捷方法。

        Convenience method to get the absolute data recording path.
        """
        return self.get_global_config("paths.data_recording_path_abs")

    def get_backtest_data_source_path(self) -> Optional[str]:
        """
        获取绝对回测数据源路径的便捷方法。

        Convenience method to get the absolute backtest data source path.
        """
        return self.get_global_config("paths.backtest_data_source_path_abs")

    def get_product_info_path(self) -> Optional[str]:
        """
        获取绝对产品信息 INI 路径的便捷方法。

        Convenience method to get the absolute product info INI path.
        """
        return self.get_global_config("paths.product_info_ini_abs")

    @staticmethod
    def load_product_info(filepath: str) -> Tuple[Dict, Dict]:
        """
        从 INI 文件加载佣金规则和乘数，订单执行网关调用。

        Loads commission rules and multipliers from an INI file.
        """
        parser = configparser.ConfigParser()
        if not os.path.exists(filepath):
            logger.error(_("错误：产品信息文件未找到 {}").format(filepath))
            return {}, {}
        try:
            parser.read(filepath, encoding='utf-8')
        except Exception as err:
            logger.exception(_("错误：读取产品信息文件 {} 时出错：{}").format(filepath, err))
            return {}, {}
        commission_rules = {}
        contract_multipliers = {}
        for symbol in parser.sections():
            if not parser.has_option(symbol, 'multiplier'):
                logger.warning(_("警告：文件 {} 中的 [{}] 缺少 'multiplier'，跳过此合约。").format(filepath, symbol))
                continue
            try:
                multiplier = parser.getfloat(symbol, 'multiplier')
                contract_multipliers[symbol] = multiplier
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
