#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : po_to_json_converter.py
@Date       : 2025/05/17
@Author     : Your Name/Donny
@Email      : your_email@example.com
@Software   : PyCharm
@Description: 将 .po 文件中的 msgid 和 msgstr 提取并转换为 JSON 文件。
             Extracts msgid and msgstr from a .po file and converts them to a JSON file.
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


def convert_po_to_json(po_file_path: Path, json_file_path: Path):
    """
    读取 PO 文件，提取 msgid 和 msgstr，并保存为 JSON 文件。

    Args:
        po_file_path (Path): 输入的 .po 文件路径。
        json_file_path (Path): 输出的 .json 文件路径。
    """
    if not po_file_path.exists():
        logger.error(f"错误：PO 文件 '{po_file_path}' 未找到。")
        return

    translations = {}
    try:
        po = polib.pofile(str(po_file_path))
        processed_count = 0
        skipped_empty_msgstr = 0

        for entry in po:
            # 跳过头部条目 (msgid="")、废弃条目以及没有翻译的条目 (msgstr为空)
            if entry.obsolete or not entry.msgid: # msgid 为空也跳过 (例如头部)
                continue

            if not entry.msgstr:
                logger.warning(f"跳过 msgid '{entry.msgid}' 因为 msgstr 为空。")
                skipped_empty_msgstr += 1
                continue
            
            # polib 会自动处理多行 msgid 和 msgstr 的拼接和解码
            translations[entry.msgid] = entry.msgstr
            processed_count += 1
        
        with open(json_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(translations, json_file, ensure_ascii=False, indent=4)
        
        logger.info(f"成功将 {processed_count} 个条目从 '{po_file_path.name}' 转换到 '{json_file_path.name}'。")
        if skipped_empty_msgstr > 0:
            logger.info(f"跳过了 {skipped_empty_msgstr} 个因为 msgstr 为空的条目。")

    except Exception as e:
        logger.error(f"处理 PO 文件 '{po_file_path}' 或写入 JSON 文件 '{json_file_path}' 时出错：{e}")
        logger.exception("详细错误信息:")

def main():
    locales_dir = project_root / "locales"
    # po_file_input_path = locales_dir / "vnpy.po"  # 输入的 PO 文件
    po_file_input_path = locales_dir / "en/LC_MESSAGES/messages.po"
    json_file_output_path = locales_dir / "vnpy_translations.json"  # 输出的 JSON 文件

    logger.info(f"开始转换过程...")
    logger.info(f"输入 PO 文件路径：{po_file_input_path}")
    logger.info(f"输出 JSON 文件路径：{json_file_output_path}")

    convert_po_to_json(po_file_input_path, json_file_output_path)
    logger.info(f"转换完成。")

if __name__ == "__main__":
    main() 