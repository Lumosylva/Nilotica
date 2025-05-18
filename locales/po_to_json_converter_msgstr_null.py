#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : po_to_json_converter_msgstr_null.py
@Date       : 2025/05/17
@Author     : Your Name/Donny
@Email      : your_email@example.com
@Software   : PyCharm
@Description: 从 .po 文件中提取 msgstr 为空的条目的 msgid，并保存为 JSON 文件。
             Extracts msgids from a .po file where msgstr is empty and saves them to a JSON file.
"""
import json
import os
import sys
from pathlib import Path
import polib


# 将项目根目录添加到 sys.path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

# 尝试导入项目日志配置，如果失败则使用基本配置
try:
    from utils.logger import logger, setup_logging
    setup_logging(service_name=os.path.basename(sys.argv[0]))
except ImportError:
    import logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    logger.info("Project-specific logger not found, using basic logging.")


def extract_empty_msgstr_keys(po_file_path: Path, json_file_path: Path):
    """
    读取 PO 文件，提取 msgstr 为空的条目的 msgid，并以 {msgid: ""} 格式保存为 JSON 文件。

    Args:
        po_file_path (Path): 输入的 .po 文件路径。
        json_file_path (Path): 输出的 .json 文件路径。
    """
    if not po_file_path.exists():
        logger.error(f"错误：PO 文件 '{po_file_path}' 未找到。")
        return

    empty_msgstr_keys = {}
    try:
        po = polib.pofile(str(po_file_path))
        processed_count = 0
        extracted_count = 0

        for entry in po:
            processed_count +=1
            # 跳过头部条目 (msgid="") 和废弃条目
            if entry.obsolete or not entry.msgid:
                continue

            # 检查 msgstr 是否为空
            if not entry.msgstr: # msgstr is an empty string
                empty_msgstr_keys[entry.msgid] = ""
                extracted_count += 1
        
        with open(json_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(empty_msgstr_keys, json_file, ensure_ascii=False, indent=4)
        
        logger.info(f"处理了 {processed_count} 个条目。")
        logger.info(f"成功将 {extracted_count} 个 msgstr 为空的条目的 msgid 从 '{po_file_path.name}' 提取到 '{json_file_path.name}'。")

    except Exception as e:
        logger.error(f"处理 PO 文件 '{po_file_path}' 或写入 JSON 文件 '{json_file_path}' 时出错：{e}")
        logger.exception("详细错误信息:")

def main():
    locales_dir = project_root / "locales"
    # 注意：根据你的最新需求，输入文件是 messages.po
    po_file_input_path = locales_dir / "en" / "LC_MESSAGES" / "messages.po"  
    json_file_output_path = locales_dir / "empty_msgstr_keys.json"  # 新的输出文件名

    logger.info(f"开始提取 msgstr 为空的条目...")
    logger.info(f"输入 PO 文件路径：{po_file_input_path}")
    logger.info(f"输出 JSON 文件路径：{json_file_output_path}")

    extract_empty_msgstr_keys(po_file_input_path, json_file_output_path)
    logger.info(f"提取完成。")

if __name__ == "__main__":
    main() 