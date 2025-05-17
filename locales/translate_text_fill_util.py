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
import os
import sys
from pathlib import Path
import polib # 导入 polib

from utils.logger import logger, setup_logging

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

setup_logging(service_name=os.path.basename(sys.argv[0]))


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


def process_po_file_with_polib(po_file_path, json_data):
    """
    使用 polib 处理PO文件，填充msgstr字段。
    """
    try:
        po = polib.pofile(po_file_path)
        updated_count = 0
        not_found_count = 0

        for entry in po:
            # 跳过头部条目 (msgid="") 和已翻译的条目 (除非需要强制覆盖)
            if entry.msgid == "" or entry.obsolete: # 跳过头部和废弃条目
                continue

            # entry.msgid 已经是正确解码和拼接的多行内容
            translation_from_json = json_data.get(entry.msgid)

            if translation_from_json is not None:
                # 如果json中的翻译与现有的msgstr不同，或者msgstr为空
                if entry.msgstr != translation_from_json or not entry.msgstr:
                    entry.msgstr = translation_from_json
                    logger.info(f"已更新 msgid '{entry.msgid}' 的 msgstr: {entry.msgstr}")
                    updated_count += 1
            else:
                if not entry.msgstr: # 只记录那些在JSON中找不到且本身也为空的
                    logger.warning(f"未在JSON中找到 msgid '{entry.msgid}' 的翻译，且msgstr为空。")
                    not_found_count +=1
                # 如果JSON中没有，但.po文件本身有翻译，则保留 .po 文件中的翻译

        po.save(po_file_path)
        logger.info(f"PO文件处理完成！共更新 {updated_count} 个条目。有 {not_found_count} 个空条目未在JSON中找到翻译。")

    except Exception as e:
        logger.error(f"处理PO文件时出错：{e}")
        logger.error(f"Traceback: {e.__traceback__}")
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

    process_po_file_with_polib(po_file_path, json_data)


if __name__ == "__main__":
    main()
