# -*- coding: utf-8 -*-
# @Time    : 2020/11/3 16:36
# @Author  : Jeffery Paul
# @File    : tickerinfo.py


def ticker_to_product(s: str):
    exchange = s.split('.')[-1]
    product_name = ''
    for i in s.split('.')[0]:
        if str(i).isdigit():
            product = product_name + '.' + exchange
            return product
        else:
            product_name += i
    product = product_name + '.' + exchange
    return product
