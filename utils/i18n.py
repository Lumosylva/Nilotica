#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : i18n
@Date       : 2025/5/9 10:20
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 封装i18n逻辑
Encapsulate i18n logic
"""
import gettext
import logging
import os
import threading

# 获取一个专门用于i18n模块的logger(Get a logger specifically for the i18n module)
i18n_logger = logging.getLogger(__name__)
i18n_logger.setLevel(logging.INFO)
if not i18n_logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    i18n_logger.addHandler(handler)

# 为每个线程存储翻译对象，确保线程安全(Store translation objects for each thread to ensure thread safety)
_thread_locals = threading.local()

# 预定义的翻译消息(Predefined translation messages)
I18N_MESSAGES = {
    "zh_CN": {
        "language_set": "语言设置为: {}, 翻译器已为当前线程加载。路径: {}",
        "translation_file_not_found": "未找到语言 '{}' 的翻译文件 (messages.mo)，将使用原文。检查 messages.mo 路径: {}",
        "language_load_error": "加载语言 '{}' 时发生错误: {}"
    },
    "en": {
        "language_set": "Language set to: {}, translator loaded for current thread. Path: {}",
        "translation_file_not_found": "Translation file (messages.mo) not found for language '{}', will use original text. Check messages.mo path: {}",
        "language_load_error": "Error occurred while loading language '{}': {}"
    }
}

def _default_translator(s: str) -> str:
    """
    默认翻译函数 (不翻译)，直到 setup_language 被调用

    Default translation function (no translation) until setup_language is called
    :param s:
    :return:
    """
    return s

# 初始化默认翻译器
_thread_locals.translator = _default_translator
current_language_code = None

def get_translator():
    """
    获取当前线程的翻译函数，如果未设置则返回默认的原文返回函数

    Get the translation function of the current thread. If not set, return the default original text return function.
    :return:
    :rtype: callable
    """
    return getattr(_thread_locals, 'translator', _default_translator)

def get_i18n_message(key: str, language_code: str = "en") -> str:
    """
    获取预定义的国际化消息

    Get predefined internationalized messages
    :param key: "language_set" or "translation_file_not_found" or "language_load_error"
    :param language_code: "en" or "zh_CN"
    :return:
    :rtype: str
    """
    return I18N_MESSAGES.get(language_code, I18N_MESSAGES["en"]).get(key, key)

def setup_language(language_code: str, project_root: str):
    """
    根据给定的语言代码和项目根路径，初始化 gettext 翻译。

    Initializes a gettext translation for the given language code and project root path.
    :param language_code: "en" or "zh_CN"
    :param project_root: the root directory of your project
    """
    global current_language_code
    localedir = os.path.join(project_root, 'locales')
    current_language_code = language_code
    
    try:
        translation = gettext.translation(
            'messages',
            localedir=localedir,
            languages=[language_code],
            fallback=True
        )
        # 将翻译函数存储在线程本地存储中
        _thread_locals.translator = translation.gettext
        # 使用预定义的翻译消息
        i18n_logger.info(get_i18n_message("language_set", language_code).format(language_code, localedir))
    except FileNotFoundError:
        _thread_locals.translator = _default_translator
        i18n_logger.warning(get_i18n_message("translation_file_not_found", language_code).format(
            language_code, os.path.join(localedir, language_code, 'LC_MESSAGES', 'messages.mo')))
    except Exception as e:
        _thread_locals.translator = _default_translator
        i18n_logger.error(get_i18n_message("language_load_error", language_code).format(language_code, str(e)))

def get_current_language() -> str | None:
    """
    获取当前设置的语言代码

    Get the currently set language code
    :return:
    :rtype: str
    """
    return current_language_code

# --- vnpy 相关常量翻译示例 ---(vnpy related constant translation examples)
VNPY_DIRECTIONS_MAP = {
    "多": "Long",
    "空": "Short",
}

VNPY_OFFSETS_MAP = {
    "开": "Open",
    "平": "Close",
    "平今": "Close Today",
    "平昨": "Close Yesterday",
}

def display_vnpy_direction(vnpy_direction_value: str) -> str:
    """
    翻译 VnPy 的方向值 (如 '多', '空')

    Translate VnPy direction values (e.g. '多', '空')
    """
    translator = get_translator()
    english_key = VNPY_DIRECTIONS_MAP.get(vnpy_direction_value, vnpy_direction_value)
    return translator(english_key)

def display_vnpy_offset(vnpy_offset_value: str) -> str:
    """
    翻译 VnPy 的开平标志值 (如 '开', '平')

    Translate VnPy offset flags values (e.g. '开', '平')
    """
    translator = get_translator()
    english_key = VNPY_OFFSETS_MAP.get(vnpy_offset_value, vnpy_offset_value)
    return translator(english_key)

# 可以在其他模块中 import _ from utils.i18n 并调用 _()
# 但为了在 logging Formatter 中使用，我们将提供一个可导入的 get_translator()
# 或者，logger formatter可以直接从这里导入 _ ，如果它在formatter实例化时被正确设置

# You can import _ from utils.i18n and call _() in other modules
# But for use in logging Formatter, we will provide an importable get_translator()
# Alternatively, the logger formatter can import _ directly from here if it is set correctly when the formatter is instantiated
