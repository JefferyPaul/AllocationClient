# -*- coding: utf-8 -*-
# @Time    : 2020/6/23 11:54
# @Author  : Jeffery Paul
# @File    : mydb.py


import pyodbc
from dbutils.pooled_db import PooledDB
import datetime

from pkgs.logger import MyLogger


logger = MyLogger(__file__)


"""
"""


class SQLSeverClient(object):
    __pool = None

    def __init__(self, mincached=10, maxcached=20, maxshared=10, maxconnections=50, blocking=True,
                 maxusage=100, setsession=None, reset=True,
                 host='127.0.0.1', db='test', user='root', pwd='123456',
                 charset='utf8'):
        """

        :param mincached:连接池中空闲连接的初始数量
        :param maxcached:连接池中空闲连接的最大数量
        :param maxshared:共享连接的最大数量
        :param maxconnections:创建连接池的最大数量
        :param blocking:超过最大连接数量时候的表现，为True等待连接数量下降，为false直接报错处理
        :param maxusage:单个连接的最大重复使用次数
        :param setsession:optional list of SQL commands that may serve to prepare
            the session, e.g. ["set datestyle to ...", "set time zone ..."]
        :param reset:how connections should be reset when returned to the pool
            (False or None to rollback transcations started with begin(),
            True to always issue a rollback for safety's sake)
        :param host:数据库ip地址
        :param port:数据库端口
        :param db:库名
        :param user:用户名
        :param pwd:密码
        :param charset:字符编码
        """

        if not self.__pool:
            self.__class__.__pool = PooledDB(
                pyodbc,
                mincached, maxcached, maxshared, maxconnections=maxconnections,
                blocking=blocking, maxusage=maxusage, setsession=setsession, reset=reset,
                SERVER=host, UID=user, PWD=pwd, DATABASE=db,
                charset=charset, use_unicode=True, DRIVER='{SQL Server}'
                # cursorclass=DictCursor
            )
        self._conn = None
        self._cursor = None
        self.__get_conn()

    def __get_conn(self):
        self._conn = self.__pool.connection()
        self._cursor = self._conn.cursor()

    def close(self):
        try:
            self._cursor.close()
            self._conn.close()
        except Exception as e:
            logger.error(e)

    def __execute(self, sql):
        count = self._cursor.execute(sql)
        logger.info(count)
        # print count
        return count

    @staticmethod
    def __dict_datetime_obj_to_str(result_dict):
        """把字典里面的datatime对象转成字符串，使json转换不出错"""
        if result_dict:
            result_replace = {k: v.__str__() for k, v in result_dict.items() if isinstance(v, datetime.datetime)}
            result_dict.update(result_replace)
        return result_dict

    def select_one(self, sql, param=()):
        """查询单个结果"""
        count = self.__execute(sql)
        result = self._cursor.fetchone()
        """:type result:dict"""
        result = self.__dict_datetime_obj_to_str(result)
        return count, result

    def select_many(self, sql, param=()):
        """
        查询多个结果
        :param sql: qsl语句
        :param param: sql参数
        :return: 结果数量和查询结果集
        """
        count = self.__execute(sql)
        result = self._cursor.fetchall()
        """:type result:list"""
        [self.__dict_datetime_obj_to_str(row_dict) for row_dict in result]
        return count, result

    # def execute(self, sql, param=()):
    #     count = self.__execute(sql, param)
    #     return count

    def execute(self, sql):
        # self._cursor.execute(sql)
        return self._cursor.execute(sql)

    def begin(self):
        """开启事务"""
        self._conn.autocommit(0)

    def end(self, option='commit'):
        """结束事务"""
        if option == 'commit':
            self._conn.autocommit()
        else:
            self._conn.rollback()

