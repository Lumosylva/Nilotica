#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica
@FileName   : translate
@Date       : 2025/5/17 10:45
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 谷歌翻译
"""
import json
import time
import re
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import requests


def _setup_browser():
    """
    设置并返回Chrome浏览器实例
    """
    try:
        # 设置Chrome选项
        options = uc.ChromeOptions()
        # options.add_argument('--headless')  # 无头模式，取消注释即可启用
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--disable-gpu')
        
        # 初始化浏览器
        driver = uc.Chrome(options=options)
        return driver
    except Exception as e:
        print(f"浏览器初始化失败: {e}")
        return None

def wait_for_element(driver, by, value, timeout=10):
    """
    等待元素出现
    """
    try:
        element = WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((by, value))
        )
        return element
    except TimeoutException:
        print(f"等待元素超时: {value}")
        return None

def translate_text(driver, text, src_lang='auto', to_lang='en'):
    """
    使用Google翻译网页翻译文本
    """
    try:
        # 等待输入框加载
        textarea_ele = wait_for_element(driver, By.CLASS_NAME, "QFw9Te").find_element(By.TAG_NAME, "textarea")
        if not textarea_ele:
            return None

        # 清空输入框
        textarea_ele.clear()
        # 输入文本
        textarea_ele.send_keys(text)

        time.sleep(0.3)

        # 使用点号连接多个class
        div_elements = driver.find_elements(By.CSS_SELECTOR, ".VfPpkd-AznF2e-ZMv3u")
        if to_lang == 'en':
            ele = div_elements[1]
            btn_elements = ele.find_elements(By.TAG_NAME, "button")
            for button in btn_elements:
                btn_text = button.text
                print(f"button.text: {btn_text}")
                btn_selected = button.get_attribute('aria-selected')
                print(f"button.selected: {btn_selected}")
                if btn_text == 'English' or btn_text == '英语':
                    print("找到英语按钮")
                    if btn_selected == 'true':
                        print("已选中英语按钮")
                    else:
                        print("未选中英语按钮")
                        button.click()
                        print("点击英语按钮")
                        time.sleep(0.5)
                    break
        elif to_lang == 'zh-CN':
            ele = div_elements[1]
            btn_elements = ele.find_elements(By.TAG_NAME, "button")
            for button in btn_elements:
                btn_text = button.text
                print(f"button.text: {btn_text}")
                btn_selected = button.get_attribute('aria-selected')
                print(f"button.selected: {btn_selected}")
                if btn_text == 'Chinese (Simplified)' or btn_text == '中文（简体）':
                    print("找到简体中文按钮")
                    if btn_selected == 'true':
                        print("已选中简体中文按钮")
                    else:
                        print("未选中简体中文按钮")
                        button.click()
                        print("点击简体中文按钮")
                        time.sleep(0.5)
                    break

        else:
            pass

        
        # 等待翻译结果
        time.sleep(2.2)  # 给翻译一些时间
        
        # 获取翻译结果
        result = wait_for_element(driver, By.CLASS_NAME, "ryNqvb")
        if result:
            print(f"翻译结果: {result.text}")
            return result.text
        return None

    except Exception as e:
        print(f"翻译过程出错: {e}")
        return None

def click_button(driver, by, value):
    """
    点击按钮
    """
    try:
        button = wait_for_element(driver, by, value)
        if button:
            button.click()
            return True
        return False
    except Exception as e:
        print(f"点击按钮失败: {e}")
        return False

def get_page_content(driver, by, value):
    """
    获取页面内容
    """
    try:
        element = wait_for_element(driver, by, value)
        if element:
            return element.text
        return None
    except Exception as e:
        print(f"获取页面内容失败: {e}")
        return None

def open_webpage(url="https://translate.google.com/"):
    """
    打开指定网页
    """
    driver = _setup_browser()
    if driver:
        try:
            driver.get(url)
            print(f"成功打开网页: {url}")
            return driver
        except Exception as e:
            print(f"打开网页失败: {e}")
            driver.quit()
    return None

def is_chinese(text):
    """
    判断字符串是否主要为中文
    返回True如果中文字符占比超过50%
    """
    chinese_chars = len(re.findall(r'[\u4e00-\u9fff]', text))
    total_chars = len(text)
    return chinese_chars / total_chars > 0.5 if total_chars > 0 else False

def translator(text, src_lang='auto', to_lang='en'):
    googleapis_url = 'https://translate.googleapis.com/translate_a/single'
    url = '%s?client=gtx&sl=%s&tl=%s&dt=t&q=%s' % (googleapis_url, src_lang, to_lang,text)
    data = requests.get(url).json()
    res = ''.join([s[0] for s in data[0]])
    return res

def translate_json_keys(input_json_path, output_json_path):
    with open(input_json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    print(f"读取到 {len(data)} 个键")

    translated = {}
    total_keys = len(data)
    processed_keys = 0

    driver = None
    try:
        driver = open_webpage()
        print("启动成功")
    except Exception as e:
        print(f"启动失败: {e}")

    for key in data:
        processed_keys += 1
        try:
            # 对每个键单独进行语言检测
            if is_chinese(key):
                current_src_lang = 'zh-CN'
                current_to_lang = 'en'
                print(f"[{processed_keys}/{total_keys}] 检测到中文键，将翻译为英文")
            else:
                current_src_lang = 'en'
                current_to_lang = 'zh-CN'
                print(f"[{processed_keys}/{total_keys}] 检测到英文键，将翻译为中文")

            # 调用谷歌翻译
            value = translate_text(driver, key, src_lang=current_src_lang, to_lang=current_to_lang)
            print(f"原文: {key}")
            print(f"译文: {value}")
            translated[key] = value
            time.sleep(0.5)  # 防止请求过快被封
        except Exception as e:
            print(f"翻译失败: {key}, 错误: {e}")
            translated[key] = ""

    with open(output_json_path, 'w', encoding='utf-8') as f:
        json.dump(translated, f, ensure_ascii=False, indent=4)
    print(f"\n翻译完成，共处理 {total_keys} 个键，已保存到 {output_json_path}")

if __name__ == "__main__":
    input_path = 'D:/Project/PycharmProjects/Nilotica/locales/test.json'
    output_path = 'D:/Project/PycharmProjects/Nilotica/locales/tout_test.json'
    translate_json_keys(input_path, output_path)
