# -*- coding : utf-8 -*- #

__author__ = "Gallen_qiu"

import pymongo
from locale import *
# locale.setlocale( locale.LC_ALL, 'en_US.UTF-8' )
from pymongo.collection import Collection


def getData():
    # 建立连接
    client = pymongo.MongoClient('localhost', 27017)
    # 建立数据库
    db = client["XinlangFinance"]
    # 从原有的txt文件导入share_id：
    # 表的对象化
    mgtable = Collection(db, 'FinanceReport_data')
    data = mgtable.find({}, {"_id": 0})
    return data


def getData_n12():
    # 建立连接
    client = pymongo.MongoClient('localhost', 27017)
    # 建立数据库
    db = client["XinlangFinance"]
    # 表的对象化
    mgtable = Collection(db, 'FinanceReport_data_n12_06')
    data = mgtable.find({}, {"_id": 0})
    return data


def parse1():
    gd = getData()
    ol = []
    for report in gd:
        r = report
        for reportDate in r:
            if "-" not in reportDate:
                continue
            for reportType in r[reportDate]:
                for n in r[reportDate][reportType]:
                    try:
                        if n == "code" or n == 'orgId' or n == 'year':
                            pass
                        else:
                            r[reportDate][reportType][n] = float(r[reportDate][reportType][n].replace(",", "")) * 10000
                    except Exception as e:
                        print(e)
                        pass
        ol.append(r)
    try:
        # 建立连接
        client = pymongo.MongoClient('localhost', 27017)
        # 建立数据库
        db = client["XinlangFinance"]
        # 从原有的txt文件导入share_id：
        # 表的对象化
        mgtable = Collection(db, 'FinanceReport_data1')
        mgtable.insert_many(ol)
    except Exception as e:
        print("写入出错！！", e)


def parse2():
    gd_12 = getData_n12()
    ol = []
    for report in gd_12:
        r = report
        for n in r:
            try:
                if type(r[n]).__name__ == "list":
                    tmp = []
                    for i in r[n]:
                        tmp.append(float(i) * 10000)
                    r[n] = tmp
            except Exception as e:
                pass
        ol.append(r)
    try:
        # 建立连接
        client = pymongo.MongoClient('localhost', 27017)
        # 建立数据库
        db = client["XinlangFinance"]
        mgtable = Collection(db, 'FinanceReport_data_n12_06_1')
        mgtable.insert_many(ol)
    except Exception as e:
        print("写入出错！！", e)


if __name__ == '__main__':
    parse1()
    parse2()
