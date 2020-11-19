# -*- coding: utf-8 -*-
# @Time    : 2020/11/18 17:25
# @Author  : Jeffery Paul
# @File    : apsdata.py


import datetime


APS_FILE_NAME = 'AggregatedPnlSeries.csv'


class APSData(dict):
    def __init__(self, *args, **kwargs):
        # super(APSData, self).__init__(*args, **kwargs)
        super(APSData, self).__init__()
        if args or kwargs:
            self.update(*args, **kwargs)
        
    # 增加属性 检验和转换
    def __setitem__(self, key, value):
        # key 检查 转换
        try:
            if isinstance(key, datetime.date):
                s_key = key.strftime('%Y%m%d')
            elif isinstance(key, datetime.datetime):
                s_key = key.strftime('%Y%m%d')
            elif isinstance(key, int):
                s_key = datetime.datetime.strptime(str(key), '%Y%m%d').strftime('%Y%m%d')
            elif isinstance(key, float):
                if int(key) - key == 0:
                    s_key = datetime.datetime.strptime(str(int(key)), '%Y%m%d').strftime('%Y%m%d')
                else:
                    raise KeyError
            elif isinstance(key, str):
                s_key = datetime.datetime.strptime(str(key), '%Y%m%d').strftime('%Y%m%d')
            else:
                raise KeyError
        except:
            raise KeyError
        # value 检查 转换
        try:
            if isinstance(value, int) or isinstance(value, float):
                f_value = value
            elif isinstance(value, str):
                f_value = float(value)
            else:
                raise ValueError
        except:
            raise ValueError
        #
        super(APSData, self).__setitem__(s_key, f_value)

    def update(self, another):
        for key, value in another.items():
            self.__setitem__(key, value)

    def __repr__(self):
        return '%s(%s)' % (
            type(self).__name__,
            dict.__repr__(self)
        )

    def to_csv(self, path):
        with open(path, 'w', encoding='utf-8') as f:
            for key, value in sorted(self.items()):
                f.write('%s,%s\n' % (key, str(value)))

    def to_list(self) -> list:
        return sorted(self.items())

    """
    1: { date: pnl, date2: pnl2, }      目标结构
    2: [(date, pnl), (date2, pnl2), (), ]      接受 
    3: [{"date": date", "pnl": pnl}, {"date": date2, "pnl": pnl2}, {}, ]
    """

    @classmethod
    def from_list(cls, data: list):
        """
        接收的数据格式：
            [(date, pnl), ]
        :return:
        """
        if not isinstance(data, list):
            raise TypeError
        aps_obj = APSData()
        for num, n_data in enumerate(data):
            if len(n_data) != 2:
                raise ValueError
            else:
                date = n_data[0]
                pnl = n_data[1]
                if date in aps_obj.keys():
                    print('有重复的键: %s' % date)
                aps_obj[date] = pnl
        return aps_obj

    # @classmethod
    # def from_dict(cls, data: dict):
    #     """
    #     接收的数据格式:
    #         { date: pnl, }
    #     :return:
    #     """
    #     pass


class ASPFile:
    def __init__(self):
        pass


class APSS:
    def __init__(self):
        pass
