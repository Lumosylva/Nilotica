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

# 为每个线程存储翻译对象，确保线程安全(Store translation objects for each thread to ensure thread safety)
_thread_locals = threading.local()

# 初始化默认翻译器和语言代码
_thread_locals.translator = gettext.NullTranslations().gettext
_thread_locals.language_code = None

def get_translator():
    """
    获取当前线程的翻译函数，如果未设置则返回默认的原文返回函数

    Get the translation function of the current thread. If not set, return the default original text return function.
    :return:
    :rtype: callable
    """
    return getattr(_thread_locals, 'translator', gettext.NullTranslations().gettext) # Fallback if not set for some reason

def setup_language(language_code: str, project_root: str):
    """
    根据给定的语言代码和项目根路径，初始化 gettext 翻译。

    Initializes a gettext translation for the given language code and project root path.
    :param language_code: "en" or "zh_CN"
    :param project_root: the root directory of your project
    """
    localedir = os.path.join(project_root, 'locales')
    _thread_locals.language_code = language_code

    try:
        translation = gettext.translation(
            'messages',
            localedir=localedir,
            languages=[language_code],
            fallback=True  # fallback=True ensures NullTranslations is returned if .mo not found
        )
        _thread_locals.translator = translation.gettext
        _ = _thread_locals.translator # Use the translator directly for log messages

        if isinstance(translation, gettext.NullTranslations):
            i18n_logger.warning(
                _("未找到语言'{}'（或其后备语言）的翻译文件 (messages.mo)，将使用原文。搜索范围：{}").format(
                    language_code=language_code,
                    localedir=localedir # Path where it looked, not the specific .mo path
                )
            )
        else:
            i18n_logger.info(
                _("语言设置为：{}，翻译器已为当前线程加载。路径：{}").format(
                    language_code, localedir
                )
            )
    except Exception as e:
        # If any other exception occurs (e.g., issues with localedir itself, permissions)
        _thread_locals.translator = gettext.NullTranslations().gettext
        _ = _thread_locals.translator # Use the null translator for this log message
        i18n_logger.error(
            _("初始化语言'{}'的 gettext 时出错：{}。恢复原始文本。").format(
                language_code, str(e)
            )
        )

def get_current_language() -> str | None:
    """
    获取当前设置的语言代码

    Get the currently set language code
    :return:
    :rtype: str
    """
    return getattr(_thread_locals, 'language_code', None) # 从线程本地存储读取

def display_vnpy_direction(vnpy_direction_value: str) -> str:
    """
    翻译 VnPy 的方向值 (如 '多', '空')

    Translate VnPy direction values (e.g. '多', '空')
    """
    translator = get_translator()
    return translator(vnpy_direction_value)

def display_vnpy_offset(vnpy_offset_value: str) -> str:
    """
    翻译 VnPy 的开平标志值 (如 '开', '平')

    Translate VnPy offset flags values (e.g. '开', '平')
    """
    translator = get_translator()
    return translator(vnpy_offset_value)

# 为了在 logging Formatter 中使用，我们将提供一个可导入的 get_translator()
# For use in logging Formatter, we will provide an importable get_translator()
