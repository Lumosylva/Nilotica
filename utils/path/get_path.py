#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@ProjectName: Nilotica_dev
@FileName   : get_path
@Date       : 2025/4/11 15:21
@Author     : Donny
@Email      : donnymoving@gmail.com
@Software   : PyCharm
@Description: 路径工具类
"""
import os

from config.constants.params import Params


class GetPath(object):

    def __init__(self):
        self._current_dir = os.getcwd()

        while os.path.basename(self._current_dir) != Params.project_name:
            self._current_dir = os.path.abspath(os.path.join(self._current_dir, '..'))
        self._project_dir = self._current_dir


    def get_project_dir(self):
        return self._project_dir

    def get_current_dir(self):
        return self._current_dir

    def set_project_dir(self, project_dir):
        self._project_dir = project_dir


if __name__ == '__main__':
    get_path = GetPath()
    print(get_path.get_current_dir())
    print(get_path.get_project_dir())
