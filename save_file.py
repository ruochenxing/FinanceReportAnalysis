# -*- coding : utf-8 -*- #

__author__ = "Gallen_qiu"

import requests, json, time
from bs4 import BeautifulSoup
from multiprocessing import Queue
from concurrent.futures import ThreadPoolExecutor

import Contants
from StockList import StockList


class Xinalang():
    def __init__(self):
        self.queue = Queue()
        self.info = []
        self.json = []

    def req(self, stock_json):
        try:
            info = json.loads(stock_json)
            code = info["code"]
            year = info["year"]
            data_ = info
            url0 = 'http://money.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/{}/ctrl/{}/displaytype/4.phtml'.format(
                code, year)
            url1 = 'http://money.finance.sina.com.cn/corp/go.php/vFD_ProfitStatement/stockid/{}/ctrl/{}/displaytype/4.phtml'.format(
                code, year)
            url2 = 'http://money.finance.sina.com.cn/corp/go.php/vFD_CashFlow/stockid/{}/ctrl/{}/displaytype/4.phtml'.format(
                code, year)
            url_list = []
            url_list.extend([url0, url1, url2])
            data_year = []
            for url in url_list:
                print(info["zwjc"], url)
                headers = {}
                response = requests.get(url, headers=headers, timeout=5)
                soup = BeautifulSoup(response.content.decode("gb2312"), "lxml")
                '''报表日期'''
                trs = soup.select("tbody tr")
                data = {}
                for tr in trs:
                    tds = tr.select("td")
                    if tds:
                        try:
                            value = tds[1].text
                            if value == "--":
                                value = "0.00"
                            data[tds[0].text] = value
                        except:
                            pass
                data_year.append(data)
            data_["data"] = data_year
            self.json.append(json.dumps(data_))
        except TimeoutError:
            print("超时")
            print(info["zwjc"], info["year"])
            self.info.append(stock_json)
        except:
            print("其他错误")
            info = json.loads(stock_json)
            print(info["zwjc"], info["year"])

    def scheduler(self):
        year_list = [2019, 2020, 2021, 2022]
        sl = StockList(
            filepath=Contants.NEED_STOCK_PATH)
        result = sl.parse()
        for stock in result:
            for year in year_list:
                stock.year = year
                info_str = json.dumps(stock.__dict__)
                self.queue.put(info_str)
        pool = ThreadPoolExecutor(max_workers=8)
        while not self.queue.empty():
            pool.submit(self.req, self.queue.get())
        pool.shutdown()
        print("剩下：" + str(len(self.info)))
        while len(self.info) > 0:
            self.req(self.info.pop())
        self.write_json()

    def write_json(self):
        try:
            for j in self.json:
                with open('data.json', 'a') as f:
                    json.dump(j, f)
        except:
            print("写入出错！！")
            pass


if __name__ == '__main__':
    start_time = time.time()

    X = Xinalang()
    X.scheduler()

    print("总耗时：{}秒".format(time.time() - start_time))
