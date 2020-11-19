# -*- coding: utf-8 -*-
# @Time    : 2020/11/4 13:53
# @Author  : Jeffery Paul
# @File    : allocation.py


# Allocation.txt 的数据格式 和 类方法
class Allocation:
    def __init__(self, **kwargs):
        d = kwargs
        if 'scaler' not in d.keys():
            raise Exception

    @classmethod
    def read_file(cls, path):
        pass

    @classmethod
    def write_file(cls, path):
        pass

    def update_file(self, path):
        pass

