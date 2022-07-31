# -*- coding : utf-8 -*- #

__author__ = "Gallen_qiu"

import traceback

import Contants
from StockList import StockList

'''最近5年的财报，包括一季报，半年报，三季报，年度报告'''
import requests, json, time, pymongo
from bs4 import BeautifulSoup
from multiprocessing import Queue
from concurrent.futures import ThreadPoolExecutor
from pymongo.collection import Collection


class Xinalang():
    def __init__(self):
        self.queue = Queue()
        self.info = []
        self.dict_list = []

    def req(self, stock_json):
        try:
            s_json = json.loads(stock_json)
            code = s_json["code"]
            year = s_json["year"]
            url0 = 'http://money.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/{}/ctrl/{}/displaytype/4.phtml'.format(
                code, year)
            url1 = 'http://money.finance.sina.com.cn/corp/go.php/vFD_ProfitStatement/stockid/{}/ctrl/{}/displaytype/4.phtml'.format(
                code, year)
            url2 = 'http://money.finance.sina.com.cn/corp/go.php/vFD_CashFlow/stockid/{}/ctrl/{}/displaytype/4.phtml'.format(
                code, year)
            url_list = []
            url_list.extend([url0, url1, url2])
            all_data = {}
            for url in url_list:
                if "vFD_BalanceSheet" in url:
                    s_json["表类型"] = "资产负债表"
                elif "vFD_ProfitStatement" in url:
                    s_json["表类型"] = "利润表"
                else:
                    s_json["表类型"] = "现金流表"
                print(s_json["zwjc"], year, s_json["表类型"], url)
                headers = {}
                response = requests.get(url, headers=headers, timeout=5)
                soup = BeautifulSoup(response.content.decode("gb2312"), "lxml")
                '''报表日期'''
                trs = soup.select("tbody tr")
                datas = [{}, {}, {}, {}]
                for tr in trs:  # 每一行
                    tds = tr.select("td")
                    if tds:
                        try:
                            index = 1
                            for td in tds[1:]:  # 每一列
                                data = datas[index - 1]
                                data.update(s_json)
                                value = td.text  # 项目值
                                if value == "--":
                                    value = 0.00
                                try:
                                    data[tds[0].text] = float(value)
                                except:
                                    data[tds[0].text] = value
                                index = index + 1
                        except:
                            pass
                for data in datas:
                    if "表类型" in data:
                        if data["报表日期"] not in all_data:
                            all_data.update(s_json)
                            all_data["表类型"] = "财报"
                            all_data[data["报表日期"]] = {}
                        all_data[data["报表日期"]][data["表类型"]] = data
            self.dict_list.append(all_data)
        except TimeoutError:
            print("超时")
            self.info.append(stock_json)
        except Exception as e:
            traceback.print_exc()
            info = json.loads(stock_json)
            print(info["zwjc"], info["year"])

    def scheduler(self):
        year_list = [2016, 2017, 2018, 2019, 2020, 2021, 2022]
        sl = StockList(
            filepath=Contants.NEED_STOCK_PATH)
        result = sl.parse()
        for stock in result:
            for year in year_list:
                stock.year = year
                info_str = json.dumps(stock.__dict__)
                self.queue.put(info_str)
        pool = ThreadPoolExecutor(max_workers=1)
        while not self.queue.empty():
            pool.submit(self.req, self.queue.get())
        pool.shutdown()
        print("剩下：" + str(len(self.info)))
        while len(self.info) > 0:
            self.req(self.info.pop())
        self.write_json()

    def write_json(self):
        if len(self.dict_list) == 0:
            return
        # 建立连接
        client = pymongo.MongoClient('localhost', 27017)
        # 建立数据库
        db = client["XinlangFinance"]
        # 从原有的txt文件导入share_id：
        # 表的对象化
        mgtable = Collection(db, 'FinanceReport_data')
        mgtable.insert_many(self.dict_list)


if __name__ == '__main__':
    start_time = time.time()

    X = Xinalang()
    X.scheduler()

    print("总耗时：{}秒".format(time.time() - start_time))
