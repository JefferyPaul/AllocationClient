# -*- coding: utf-8 -*-
# @Time    : 2020/11/18 14:31
# @Author  : Jeffery Paul
# @File    : process_pm.py


import os
import json
import datetime
import time
import collections
import csv
import sys

# 第三方库

# 设置项目目录
PATH_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(PATH_ROOT)
os.chdir(PATH_ROOT)
PATH_CONFIG = os.path.join(PATH_ROOT, 'Config', 'Config.json')
FILE_NAME = os.path.basename(__file__).replace('.py', '')

# 导入我的模块
from pkgs.logger import MyLogger


# 改变标准输出的默认编码，cmd
# import io
# sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf8')


def main():
    # 【1】下载

    # 【2】重构

    # 【3】检查
    pass


if __name__ == "__main__":
    path_logs = os.path.join(PATH_ROOT, 'logs')
    if not os.path.exists(path_logs):
        os.makedirs(path_logs)

    # 配置logging
    logger = MyLogger(name=FILE_NAME, is_file=True, output_root=path_logs)

    # 启动
    t_s = time.time()
    logger.info('Start')
    main()
