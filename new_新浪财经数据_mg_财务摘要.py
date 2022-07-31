# -*- coding : utf-8 -*- #

__author__ = "Gallen_qiu"

import Contants
from StockList import StockList

'''最近5年的财报财务摘要'''
import requests, json, pymongo, time
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from multiprocessing import Queue
from pymongo.collection import Collection
import ssl

ssl._create_default_https_context = ssl._create_unverified_context


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
        data_ = s_json
        url0 = 'http://vip.stock.finance.sina.com.cn/corp/go.php/vFD_FinanceSummary/stockid/{}.phtml'.format(code)
        url_list = []
        url_list.extend([url0])
        for url in url_list:
            print(s_json['zwjc'], url)
            headers = {
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
                "accept-encoding": "gzip,deflate,br",
                "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
                "cache-control": "max-age=0",
                "upgrade-insecure-requests": "1",
                "user-agent": self.usa.random, }
            response = requests.get(url, headers=headers, timeout=5)  # ,headers=headers
            html_text = response.content.decode("GBK")
            html1 = html_text.split("FundHoldSharesTable")[1].split("<!--财务摘要end-->")[0]
            html2 = html1.split("<!--分割数据的空行begin-->")
            for h in html2:
                li = {}
                soup = BeautifulSoup(h, "lxml")
                for tr in soup.select("tr"):
                    try:
                        key = tr.select("td")[0].text
                        value = tr.select("td")[1].text
                        if value == "\xa0":
                            value = None
                        elif key == '截止日期':
                            value = value
                        else:
                            value = float(value.replace("元", "").replace(",", ""))
                        li[key] = value
                    except:
                        pass
                if li != {}:
                    data_[li["截止日期"]] = li
            '''报表日期'''
        self.dict_list.append(data_)

    def scheduler(self):
        sl = StockList(
            filepath=Contants.NEED_STOCK_PATH)
        result = sl.parse()
        for stock in result:
            info_str = json.dumps(stock.__dict__)
            try:
                self.req(info_str)
            except:
                print("Retry!")
                self.req(info_str)
            self.write_json()
            time.sleep(1.5)
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
        mgtable = Collection(db, 'FinanceReport_data_cwzy')
        mgtable.insert_many(self.dict_list)
        self.dict_list.pop()


if __name__ == '__main__':
    start_time = time.time()
    X = Xinalang()
    X.scheduler()
    print("总耗时：{}秒".format(time.time() - start_time))
