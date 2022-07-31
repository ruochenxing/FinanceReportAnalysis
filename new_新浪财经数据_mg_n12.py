# -*- coding : utf-8 -*- #

__author__ = "Gallen_qiu"

import Contants
from StockList import StockList

'''最近12个月的财报'''
import requests, json, pymongo, time
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from multiprocessing import Queue
from pymongo.collection import Collection


class Xinalang():
    def __init__(self):
        self.queue = Queue()
        self.info = []
        self.dict_list = []
        self.usa = UserAgent()

    def req(self, stock_json):
        # try:
        s_json = json.loads(stock_json)
        code = s_json["code"]
        url0 = 'http://vip.stock.finance.sina.com.cn/corp/go.php/vFD_BalanceSheet/stockid/{}/ctrl/part/displaytype/4.phtml'.format(
            code)
        url1 = 'http://vip.stock.finance.sina.com.cn/corp/go.php/vFD_ProfitStatement/stockid/{}/ctrl/part/displaytype/4.phtml'.format(
            code)
        url2 = 'http://vip.stock.finance.sina.com.cn/corp/go.php/vFD_CashFlow/stockid/{}/ctrl/part/displaytype/4.phtml'.format(
            code)
        url_list = []
        url_list.extend([url0, url1, url2])
        data = {}
        for url in url_list:
            # if "vFD_BalanceSheet" in url:
            #     s_json["表类型"] = "资产负债表"
            # elif "vFD_ProfitStatement" in url:
            #     s_json["表类型"] = "利润表"
            # else:
            #     s_json["表类型"] = "现金流表"
            print(s_json['zwjc'], url)
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "accept-encoding": "gzip,deflate,br",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
                "cache-control": "max-age=0",
                "upgrade-insecure-requests": "1",
                "user-agent": self.usa.random, }
            response = requests.get(url, headers=headers)  # ,headers=headers
            soup = BeautifulSoup(response.content.decode("gb2312"), "lxml")
            '''报表日期'''
            trs = soup.select("tbody tr")  # 一行
            for tr in trs:
                tds = tr.select("td")
                if tds[1:]:
                    value_list = []
                    for td in tds[1:]:  # 每一列
                        td = td.text
                        if td == "--":
                            td = 0.00
                        try:
                            value_list.append(float(td.replace(',', '')))
                        except:
                            value_list.append(td)
                    data[tds[0].text] = value_list
            s_json.update(data)
        self.dict_list.append(s_json)

    def scheduler(self):
        sl = StockList(
            filepath=Contants.NEED_STOCK_PATH)
        result = sl.parse()
        for stock in result:
            info_str = json.dumps(stock.__dict__)
            self.req(info_str)
            self.write_json()
            time.sleep(2)
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
        mgtable = Collection(db, 'FinanceReport_data_n12_06')
        mgtable.insert_many(self.dict_list)
        self.dict_list.pop()


if __name__ == '__main__':
    start_time = time.time()
    X = Xinalang()
    X.scheduler()
    print("总耗时：{}秒".format(time.time() - start_time))
