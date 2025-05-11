#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : compile_translations_util.py
@Date       : 2025/5/9 10:20
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description:
编译翻译文件：将 .po 文件编译为 .mo 文件
使用 pybabel 进行编译

Compile translation files: compile .po files into .mo files
Use pybabel for compilation
"""
import subprocess
from pathlib import Path

def compile_translations():
    """
    编译所有语言的翻译文件

    Compile translation files for all languages
    """
    locales_dir = Path(__file__).parent
    print(f"开始编译翻译文件，目录：{locales_dir}")
    
    # 遍历所有语言目录
    for lang_dir in locales_dir.iterdir():
        if not lang_dir.is_dir() or lang_dir.name.startswith('.'):
            continue
            
        lc_messages_dir = lang_dir / 'LC_MESSAGES'
        if not lc_messages_dir.exists():
            print(f"跳过 {lang_dir.name}：未找到 LC_MESSAGES 目录")
            continue
            
        po_file = lc_messages_dir / 'messages.po'
        mo_file = lc_messages_dir / 'messages.mo'
        
        if not po_file.exists():
            print(f"跳过 {lang_dir.name}：未找到 messages.po 文件")
            continue
            
        print(f"编译 {lang_dir.name} 的翻译文件...")
        try:
            # 使用 pybabel 命令编译
            result = subprocess.run(
                ['pybabel', 'compile', 
                 '-i', str(po_file),  # 输入文件
                 '-o', str(mo_file),  # 输出文件
                 '-l', lang_dir.name, # 语言代码
                 '-d', str(locales_dir)], # 翻译文件目录
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                print(f"成功：{lang_dir.name} 的翻译文件已编译")
            else:
                print(f"错误：编译 {lang_dir.name} 的翻译文件失败")
                print(f"错误信息：{result.stderr}")
                
        except Exception as e:
            print(f"错误：编译 {lang_dir.name} 的翻译文件时发生异常：{e}")
            
    print("编译完成")

if __name__ == '__main__':
    compile_translations() 