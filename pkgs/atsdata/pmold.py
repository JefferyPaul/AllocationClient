# -*- coding: utf-8 -*-
# @Time    : 2020/11/18 16:18
# @Author  : Jeffery Paul
# @File    : pmold.py




import os
import collections
import threading
import pandas as pd
import pyodbc
from dbutils.pooled_db import PooledDB
import logging


def download_pm_strategy_traders_pnl(
        strategy_ids: list,
        host, db, user, pwd, sem_count=10,
        output_root=None, output_strategy=True,
        **kwags
):
    """
    下载 strategy 的 trader pnl;支持多 strategies
    :param strategy_ids: 策略id，
    :param host:
    :param db:
    :param user:
    :param pwd:
    :param sem_count:
    :param output_root:
    :param output_strategy:
    :param kwags:
    :return:
    """

    def _a_task_download_strategies_traders_pnl(strategy_id, output_root_path):
        with sem:
            logger.info('%s 开始下载' % strategy_id)
            # 获取 strategies 的 trader pnl
            data = get_strategy_traders_pnl(cursor=cursor, strategy_id=strategy_id)
            logger.info('%s 下载完成' % strategy_id)

            for trader_id, l_data in data.items():
                path_output_root = os.path.join(output_root_path, trader_id + '.csv')
                path_output_file = os.path.join(path_output_root, 'AggregatedPnlSeries.csv')
                os.makedirs(path_output_root)
                df = pd.DataFrame(l_data)
                df.sort_values(by='Date', inplace=True)
                df.loc[:, ['Date', 'Pnl']].to_csv(path_output_file, header=None, index=False)
            logger.info('%s 输出完成' % strategy_id)

    sem = threading.Semaphore(sem_count)
    my_threads = []
    # 建立数据库连接
    cursor = SQLSeverClient(host=host, db=db, user=user, pwd=pwd)
    for n_id in strategy_ids:
        if output_root:
            if output_strategy:
                path_strategy_output = os.path.join(output_root, n_id)
            else:
                path_strategy_output = output_root
        else:
            path_strategy_output = None
        t = threading.Thread(
            target=_a_task_download_strategies_traders_pnl,
            kwargs={
                'strategy_id': n_id,
                'output_root': path_strategy_output
            }
        )
        t.start()
        my_threads.append(t)

    for t in my_threads:
        t.join()
    cursor.close()


"""
内部方法，包含 SQL 语句
    下载数据函数
    两种数据格式：
    1) 单个id数据    (pd中的 records)
    a_data_list = [
        {
            "date": ,
            "Pnl": ,
            "Commission": ,
            "Slippage": ,
            "Capital": ,
        },
    ]
    这种形式可以直接用 df = pd.DataFrame(a_data_list)

    2) 多id组合数据 （如strategy下的所有traders 数据）
    d_data = {
        id1: a_data_list,
        id2: a_data_list,
    }
    这种形式可：
        df = pd.concat(
            [pd.DataFrame(a_data_list) for a_data_list in d_data.values()]
        )



"""


def get_strategy_traders_id(cursor: pyodbc.Cursor, strategy_id) -> list:
    # 获取pm_strategy_info  （主要用于获取 Init Capital信息）
    sql = f'''
    SELECT [Id] 
    FROM [Platinum.PM].[dbo].[TraderDbo] 
    where strategyid = '{strategy_id}' 
    '''

    l_traders_id = []
    for row in cursor.execute(sql).fetchall():
        l_traders_id.append(row[0])
    return l_traders_id


def get_traders_pnl(cursor: pyodbc.Cursor, list_traders_id: list, start_date='20170101') -> dict:
    """
        返回：
        {
            trader_id: [
                {
                    "date": ,
                    "Pnl": ,
                    "Commission": ,
                    "Slippage": ,
                    "Capital": ,
                },
            ],
        }
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

    # 初始化。 如果没有 某个trader_id 的pnl返回，也会存在与dict中，能够被察觉
    d_traders_pnl = collections.defaultdict(list)
    for row in cursor.execute(sql).fetchall():
        trader_id = str(row[1])
        d_traders_pnl[trader_id].append({
            'Date': str(row[0]),
            'Pnl': float(row[2]),
            'Commission': float(row[3]),
            'Slippage': float(row[4]),
            'Capital': float(row[5])
        })
    return d_traders_pnl


def get_strategy_traders_pnl(cursor: pyodbc.Cursor, strategy_id, start_date='20170101') -> dict:
    """
    :return:
    """
    l_traders_id = get_strategy_traders_id(cursor=cursor, strategy_id=strategy_id)
    d_traders_pnl = get_traders_pnl(cursor=cursor, list_traders_id=l_traders_id, start_date=start_date)
    return d_traders_pnl
