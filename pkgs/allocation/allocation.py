# -*- coding: utf-8 -*-
# @Time    : 2020/11/4 14:02
# @Author  : Jeffery Paul
# @File    : allocation.py


import os
from collections import defaultdict


"""
通过GenBaseAllocation 程序跑出来的，汇总Allocator结果（Allocation+.txt）的 Allocation.csv文件，
包含Portfolio中，所以Allocator的上下线意见(0 / 1)，以及Portfolio的Allocation.txt(比例分配)的汇总配比。
"""


class BaseAllocation(dict):
    def __init__(self, *args):
        super().__init__(*args)

    def _parse_trader(self):
        pass

    @classmethod
    def read_file(cls, path):
        my_allocation = BaseAllocation()
        if not os.path.isfile(path):
            print('不存在此文件: %s' % path)
            raise FileExistsError
        else:
            with open(path, encoding='utf-8') as f:
                l_lines = f.readlines()
            for line in l_lines:
                line = line.strip()
                if line == '':
                    continue
                if line.split(',') == 2:
                    print('Allocation.csv数据行错误:\n\t%s\n\t%s' % (path, line))
                    raise Exception
                trader_map = line.split(',')[0]
                allocation = float(line.split(',')[1])
                my_allocation[trader_map] = allocation
            return my_allocation

    def write_file(self, path):
        pass

    def get_selected_trader(self) -> list:
        l = []
        for trader_map, allocation in self.items():
            if float(allocation) == 0:
                continue
            else:
                trader = str(trader_map).split('\\')[-1].replace('.csv', '')
                l.append(trader)
        return l

    def _original_data(self):
        return self

    def gen_trader_allocation(self):
        d = defaultdict(float)
        for trader_path, allocation in self.items():
            trader_name = str(trader_path).split('\\')[-1].replace('.csv', '')
            d[trader_name] += float(allocation)
        return BaseAllocation(d)


