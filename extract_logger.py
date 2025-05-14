#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : extract_logger
@Date       : 2025/5/13 10:25
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 提取 Python 中 logger.info("xxx")... 或 logger.info(self._("xxx")) 或 logger.info(_("xxx")) 双引号包裹的字符串。
"""
import datetime
import json
import re
import sys

def extract_log_messages(py_file_path) -> list:
    """
    提取 Python 中 logger.info("xxx")... 或 logger.info(self._("xxx")) 或 logger.info(_("xxx")) 双引号包裹的字符串。
    支持以下格式：
    1. logger.info("message")、logger.debug("message")...
    2. logger.info(self._("message"))、logger.debug(self._("message"))...
    3. logger.info(_("message"))、logger.debug(_("message"))...
    """
    try:
        with open(py_file_path, 'r', encoding='utf-8') as file:
            content = file.read()

        # 使用正则表达式匹配三种格式：
        # 1. 直接的双引号内容
        # 2. self._() 中的双引号内容
        # 3. _() 中的双引号内容
        pattern_info = r'logger\.(?:info|debug|warning|error|exception|critical|fatal)\(\s*(?:(?:self\.)?_\()?f?["\'](.*?)["\'](?:\))?\s*\)'
        matches = re.findall(pattern_info, content)

        return matches

    except FileNotFoundError:
        print(f"错误：文件 {py_file_path} 未找到。")
        return []
    except Exception as e:
        print(f"读取文件时出错：{e}")
        return []


if __name__ == "__main__":
    output_file = datetime.datetime.now().strftime("%Y%m%d_%H%M%S") + "_output.json"

    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        # file_path = input("请输入 Python 文件路径: ")
        file_path = "./zmq_services/run_strategy_engine.py"

    log_messages: list = extract_log_messages(file_path)
    if log_messages:
        print("\n提取的 logger 消息：")
        for idx, message in enumerate(log_messages, 1):
            print(f"{idx}. {message}")
        data_dict = {key: "" for key in log_messages}  # 创建字典，键来自列表，初始值为空字
        with open(output_file, 'w', encoding='utf-8') as json_file:
            json.dump(data_dict, json_file, ensure_ascii=False, indent=4)
        print("JSON文件已创建")
    else:
        print("\n未找到 logger 消息！")
