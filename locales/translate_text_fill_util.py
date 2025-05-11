#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : translate_text_fill_util.py
@Date       : 2025/5/9 12:07
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 文本翻译工具 - 自动填充 .po 文件中的 msgstr 字段，保留原始结构
Text translation tool - automatically fills in msgstr fields in .po files, preserving original structure
"""
import json
import re
import sys
from pathlib import Path

from utils.logger import logger

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


def load_json_file(file_path):
    """加载JSON文件并返回其内容"""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except FileNotFoundError:
        logger.error(f"错误：文件 '{file_path}' 未找到。")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"错误：无法解析JSON文件 '{file_path}'。错误信息：{e}")
        return None


def process_po_file(po_file_path, json_data):
    """
    处理PO文件，填充msgstr字段，保留原有结构和msgid

    Process PO files, fill in msgstr fields, retain original structure and msgid
    :param po_file_path: PO文件路径
    :param json_data: JSON数据
    """
    try:
        with open(po_file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()

        new_lines = []
        i = 0
        current_msgid_content = None
        is_header_msgid = False
        header_block_has_been_fully_processed = False

        while i < len(lines):
            line = lines[i]
            stripped_line = line.strip()

            if stripped_line.startswith('msgid'):
                current_msgid_lines = []
                current_msgid_content = ""
                
                current_msgid_lines.append(line)
                match = re.match(r'msgid\s+"(.*)"', stripped_line)
                if match:
                    current_msgid_content = match.group(1)
                
                if current_msgid_content == "" and not header_block_has_been_fully_processed:
                    is_header_msgid = True
                else:
                    is_header_msgid = False

                temp_i = i + 1
                while temp_i < len(lines) and lines[temp_i].strip().startswith('"'):
                    current_msgid_lines.append(lines[temp_i])
                    inner_match = re.match(r'"((?:[^"\\]|\\.)*)"', lines[temp_i].strip())
                    if inner_match:
                        current_msgid_content += inner_match.group(1)
                    temp_i += 1
                
                new_lines.extend(current_msgid_lines)
                i = temp_i
                continue

            elif stripped_line.startswith('msgstr'):
                original_msgstr_first_line = line
                original_msgstr_continuation_lines = []
                
                temp_i = i + 1
                while temp_i < len(lines) and lines[temp_i].strip().startswith('"'):
                    original_msgstr_continuation_lines.append(lines[temp_i])
                    temp_i += 1
                
                if current_msgid_content is not None and not is_header_msgid:
                    translation_from_json = json_data.get(current_msgid_content)
                    
                    if translation_from_json is not None:
                        clean_translation = translation_from_json.rstrip()
                        escaped_translation = clean_translation.replace('\\', '\\\\').replace('"', '\\"')
                        
                        new_lines.append(f'msgstr "{escaped_translation}"\n')
                        logger.info(f"已填充 msgid '{current_msgid_content}' 的 msgstr: {clean_translation}")
                    else:
                        new_lines.append(original_msgstr_first_line)
                        new_lines.extend(original_msgstr_continuation_lines)
                        logger.warning(f"未找到 '{current_msgid_content}' 的翻译，保留原msgstr块")
                
                else:
                    new_lines.append(original_msgstr_first_line)
                    new_lines.extend(original_msgstr_continuation_lines)
                    if is_header_msgid:
                        logger.info("保留文件头部 msgstr 块")
                        header_block_has_been_fully_processed = True
                    else:
                        logger.info("保留孤立的 msgstr 块 (无前序msgid)")

                current_msgid_content = None
                is_header_msgid = False
                i = temp_i
                continue

            else:
                new_lines.append(line)
                i += 1
        
        with open(po_file_path, 'w', encoding='utf-8') as file:
            file.writelines(new_lines)

        logger.info("PO文件处理完成！")

    except Exception as e:
        logger.error(f"处理PO文件时出错：{e}")
        raise


def main():
    locales_dir = project_root / "locales"
    json_file_path = locales_dir / "language.json"
    po_file_path = locales_dir / "en" / "LC_MESSAGES" / "messages.po"

    logger.info(f"JSON文件路径：{json_file_path}")
    logger.info(f"PO文件路径：{po_file_path}")

    json_data = load_json_file(json_file_path)
    if json_data is None:
        return

    process_po_file(po_file_path, json_data)


if __name__ == "__main__":
    main()
