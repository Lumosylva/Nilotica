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
import logging
import os
import threading
import polib     # 引入 polib

# 获取一个专门用于i18n模块的logger(Get a logger specifically for the i18n module)
i18n_logger = logging.getLogger(__name__)

# 为每个线程存储翻译对象，确保线程安全(Store translation objects for each thread to ensure thread safety)
_thread_locals = threading.local()

# 初始化默认翻译器和语言代码
# 我们仍然需要一个回退机制，如果 polib 加载失败
def _null_translator(s: str) -> str:
    return s

_thread_locals.translator = _null_translator
_thread_locals.language_code = ""

def get_translator():
    """
    获取当前线程的翻译函数，如果未设置则返回默认的原文返回函数

    Get the translation function of the current thread. If not set, return the default original text return function.
    :return:
    :rtype: callable
    """
    return getattr(_thread_locals, 'translator', _null_translator)

def setup_language(language_code: str, project_root: str):
    """
    根据给定的语言代码和项目根路径，初始化翻译。
    现在使用 polib 加载 .mo 文件。

    Initializes translations for the given language code and project root path.
    Now uses polib to load .mo files.
    :param language_code: "en" (translate to English), "" (empty string for no translation, use original text),
    or other (unsupported, fallback to no translation).
    :param project_root: the root directory of your project
    """
    localedir = os.path.join(project_root, 'locales')
    _ = _null_translator # Default _ for logging if setup fails early

    # 检查 language_code 的情况
    if language_code == "":
        _thread_locals.language_code = ""
        _thread_locals.translator = _null_translator
        # 可选：如果希望在明确设置为空字符串时不进行日志记录，可以在此返回
        # i18n_logger.info(_("Language is set to empty string. No translation will be applied (original text will
        # be used)."))
        return # 如果语言代码是空字符串，则不加载任何 .mo 文件
    elif language_code == "en":
        _thread_locals.language_code = "en"
        mo_file_path = os.path.join(localedir, language_code, 'LC_MESSAGES', 'messages.mo')
    else:
        i18n_logger.warning(_("Unsupported language code: '{}'. Falling back to no translation (original text will be used).").format(language_code))
        _thread_locals.language_code = "" # 或 language_code，取决于是否想记录不支持的码
        _thread_locals.translator = _null_translator
        return # 不支持的语言，不加载 .mo 文件

    # 如果是 "en"，继续加载 .mo 文件
    try:
        if not os.path.exists(mo_file_path):
            _thread_locals.translator = _null_translator
            _ = _thread_locals.translator
            i18n_logger.warning(
                _("Translation file (messages.mo) not found for language '{}' at {}, will use original text.").format(
                    language_code, mo_file_path))
            return

        mo_file = polib.mofile(mo_file_path)
        
        if not mo_file or len(mo_file) == 0:
            _thread_locals.translator = _null_translator
            _ = _thread_locals.translator
            i18n_logger.warning(
                 _("Failed to load translations or .mo file is empty for language '{}' using polib from {}, will use original text.").format(language_code, mo_file_path))
        else:
            def polib_translator(msgid: str) -> str:
                entry = mo_file.find(msgid)
                return entry.msgstr if entry and entry.msgstr else msgid
            
            _thread_locals.translator = polib_translator
            _ = _thread_locals.translator
            i18n_logger.info(
                _("Language set to: {}, translator loaded using polib for current thread. Path: {}").format(
                    language_code, mo_file_path
                )
            )

    except Exception as e:
        _thread_locals.translator = _null_translator
        _ = _thread_locals.translator
        i18n_logger.error(
            _("Error occurred while initializing translations for language '{}' using polib: {}. Falling back to original text.").format(
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
    return getattr(_thread_locals, 'language_code', None)

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

# 可以在其他模块中 import _ from utils.i18n 并调用 _()
# 但为了在 logging Formatter 中使用，我们将提供一个可导入的 get_translator()
# 或者，logger formatter可以直接从这里导入 _ ，如果它在formatter实例化时被正确设置

# You can import _ from utils.i18n and call _() in other modules
# But for use in logging Formatter, we will provide an importable get_translator()
# Alternatively, the logger formatter can import _ directly from here if it is set correctly when the formatter is instantiated

# 新增: 全局翻译函数
def _(text: str) -> str:
    """
    主要的翻译函数，供其他模块使用。
    动态检索当前线程的翻译器。

    Main translation function to be used by other modules.
    Dynamically retrieves the current thread's translator.
    """
    return get_translator()(text)
