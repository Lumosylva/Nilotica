#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : extract_logger
@Date       : 2025/5/13 10:25
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: description
"""
import json
import re
import sys

def extract_log_messages(py_file_path) -> list:
    """
    提取 Python 文件中 logger.info 调用的字符串。
    """
    try:
        with open(py_file_path, 'r', encoding='utf-8') as file:
            content = file.read()

        # 使用正则表达式匹配双引号内的内容
        # pattern_info = r'logger\.info\(\s*f?["\'](.*?)["\']\s*\)'
        pattern_info = r'logger\.(?:info|debug|warning|error|exception|critical|fatal)\(\s*f?["\'](.*?)["\']\s*\)'
        matches = re.findall(pattern_info, content)

        return matches

    except FileNotFoundError:
        print(f"错误：文件 {py_file_path} 未找到。")
        return []
    except Exception as e:
        print(f"读取文件时出错：{e}")
        return []

if __name__ == "__main__":
    output_file = "output.json"

    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    else:
        # file_path = input("请输入 Python 文件路径: ")
        file_path = "./zmq_services/strategy_engine.py"

    log_messages: list = extract_log_messages(file_path)
    if log_messages:
        print("\n提取的 logger 消息：")
        for idx, message in enumerate(log_messages, 1):
            print(f"{idx}. {message}")
        data_dict = {key: "" for key in log_messages}  # 创建字典，键来自列表，初始值为空字
        print(f"data_dict: {data_dict}")
        with open(output_file, 'w', encoding='utf-8') as json_file:
            json.dump(data_dict, json_file, ensure_ascii=False, indent=4)
        print(f"JSON文件已创建：{output_file}")
    else:
        print("\n未找到 logger 消息！")
