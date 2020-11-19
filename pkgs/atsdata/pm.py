# -*- coding: utf-8 -*-
# @Time    : 2020/5/6 18:16
# @Author  : Jeffery Paul
# @File    : process_pm.py.bak


# 内置库
import os
import shutil
import time
from collections import namedtuple, defaultdict
import logging
from typing import Dict, List
# 第三方库
import pyodbc
from dbutils.pooled_db import PooledDB

# 本地库
from pkgs.logger import MyLogger

logger = MyLogger(__file__)


# 单个 trader pnl 数据
SingleTraderPnlLog = namedtuple(
    "SingleTraderPnlLog",
    ["Date", "Pnl", "Commission", "Slippage", "Capital"]
)


class TraderPnlLog(list):
    """
    记录 pm 数据库中，1个trader的pnl数据
    结构：
    [
        TraderPnlLog(),
        TraderPnlLog(),
    ]
    """
    def __init__(self, name='', strategy_name='', *args):
        super(TraderPnlLog, self).__init__(*args)
        self._name = name
        self._strategy_name = strategy_name

    def __str__(self):
        return "TraderPnlLog obj:\n\t name:%s\n\t strategy_name:%s\n\t len:%s" % (
            self._name, self._strategy_name, len(self)
        )

    # 增加类型检查
    def append(self, item):
        if isinstance(item, SingleTraderPnlLog):
            super(TraderPnlLog, self).append(item)
        else:
            raise TypeError

    def to_aps(self):
        # 需要时才调用
        from ..aps.apsdata import APSData
        aps_data: APSData = APSData.from_list(
            [(pnl_log.Date, pnl_log.Pnl) for pnl_log in self]
        )
        return aps_data


class PMdb:
    """
    PM -> 数据库信息
    """

    def __init__(
            self, host, db, user, pwd,
            logger: None or logging.Logger = None
    ):
        self.config = {
            "host": host,
            "db": db,
            "user": user,
            "pwd": pwd
        }

        # 初始化
        self._cursor = None
        self._conn = None

        #
        if isinstance(logger, logging.Logger):
            self._logger = logger
        else:
            self._logger = logging.Logger(name=__name__)

    def __str__(self):
        return 'PMdb -- host:%s,db:%s' % (self.config['host'], self.config['db'])

    def _connect(self):
        mincached = 10
        maxcached = 20
        maxshared = 10
        maxconnections = 50
        blocking = True
        maxusage = 100
        setsession = None
        reset = True
        charset = 'utf8'

        self._pool = PooledDB(
            pyodbc,
            mincached, maxcached, maxshared, maxconnections=maxconnections,
            blocking=blocking, maxusage=maxusage, setsession=setsession, reset=reset,
            SERVER=self.config['host'], UID=self.config['user'], PWD=self.config['pwd'], DATABASE=self.config['db'],
            charset=charset, use_unicode=True, DRIVER='{SQL Server}'
            # cursorclass=DictCursor
        )
        self._conn = self._pool.connection()
        self._cursor = self._conn.cursor()

    def close(self):
        try:
            self._cursor.close()
            self._conn.close()
        except Exception as e:
            self._logger.error(e)

    # 获取 trader 的 pnl
    # { trader: [{Date, Pnl. Commission, Slippage, Capital}, {}], }
    def trader_pnl(self, list_traders_id: list, start_date='20170101') -> dict:
        """
        连接数据库       [Platinum.PM].[dbo].[TraderLogDbo]
        获取 trader 的 pnl
        :param list_traders_id:
        :param start_date:
        :return: { trader_id: TraderPnlLog([SingleTraderPnlLog, SingleTraderPnlLog]), }
        """
        # 获取这些traders的 pnl
        sql = '''SELECT 
        [Date],
        [TraderId], 
        [Pnl], 
        [Commission], 
        [Slippage], 
        [Capital] 
        FROM [Platinum.PM].[dbo].[TraderLogDbo] 
        where date >= '%s' and traderId in ('%s') 
        ''' % (start_date, "' ,'".join(list_traders_id))

        if not self._cursor:
            self._connect()

        # 初始化。 如果没有 某个trader_id 的pnl返回，也会存在与dict中，能够被察觉
        # d_traders_pnl = defaultdict(TraderPnlLog)
        # d_traders_pnl = defaultdict(list)
        d_traders_pnl = {}
        for row in self._cursor.execute(sql).fetchall():
            trader_id = str(row[1])
            # d_traders_pnl[trader_id].append({
            #     'Date': str(row[0]),
            #     'Pnl': float(row[2]),
            #     'Commission': float(row[3]),
            #     'Slippage': float(row[4]),
            #     'Capital': float(row[5])
            # })
            if trader_id not in d_traders_pnl:
                d_traders_pnl[trader_id] = TraderPnlLog(name=trader_id)
            d_traders_pnl[trader_id].append(
                SingleTraderPnlLog(
                    Date=str(row[0]),
                    Pnl=float(row[2]),
                    Commission=float(row[3]),
                    Slippage=float(row[4]),
                    Capital=float(row[5])
                ))
        return d_traders_pnl

    # 查询 strategy 的 trader
    def strategy_traders_id(self, strategy_id: str) -> list:
        """
        连接数据库 [Platinum.PM].[dbo].[TraderDbo]
        :param strategy_id:
        :return:
        """
        sql = f'''
        SELECT [Id] 
        FROM [Platinum.PM].[dbo].[TraderDbo] 
        where strategyid = '{strategy_id}'
        '''

        if not self._cursor:
            self._connect()

        l_traders_id = []
        for row in self._cursor.execute(sql).fetchall():
            l_traders_id.append(row[0])
        return l_traders_id

    #
    def download_strategies_traders_pnl(
            self, strategy_ids: list, start_date: str = '20170101',
            output_root=None, by_strategy=True) -> dict:
        """

        :param strategy_ids:
        :param start_date:
        :param output_root: 是否输出为文件
        :param by_strategy: 是否按照strategy划分
        :return:
        """

        d_all_data = {}

        # 逐个 strategy 进行
        for num, strategy_id in enumerate(strategy_ids):
            # 下载 并 输出
            self._logger.info('%s 开始下载' % strategy_id)
            # 获取 strategies 的 trader pnl
            l_traders_id = self.strategy_traders_id(strategy_id=strategy_id)
            traders_pnl_data: Dict[TraderPnlLog] = self.trader_pnl(list_traders_id=l_traders_id, start_date=start_date)
            self._logger.info('%s 下载完成' % strategy_id)

            # 保存
            if by_strategy:
                d_all_data[strategy_id] = traders_pnl_data
            else:
                d_all_data.update(traders_pnl_data)
            # 输出
            if output_root:
                # 输出目录
                if by_strategy:
                    path_strategy_output = os.path.join(output_root, strategy_id)
                else:
                    path_strategy_output = output_root
                if os.path.isdir(path_strategy_output):
                    shutil.rmtree(path_strategy_output)
                    time.sleep(0.000001)
                os.makedirs(path_strategy_output)
                # 遍历 trader
                for trader_id in traders_pnl_data.keys():
                    tpl_obj: TraderPnlLog = traders_pnl_data[trader_id]
                    path_output_root = os.path.join(path_strategy_output, trader_id + '.csv')
                    path_output_file = os.path.join(path_output_root, 'AggregatedPnlSeries.csv')
                    os.makedirs(path_output_root)
                    # 输出文件
                    tpl_obj.to_aps().to_csv(path=path_output_file)

                self._logger.info('%s 输出完成' % strategy_id)

        return d_all_data

