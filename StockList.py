import json

import Contants


class Stock:
    code = ''  # 股票代码
    zwjc = ''  # 中文名
    pinyin = ''  # 拼音
    category = ''  # 类型
    orgId = ''  # 组织ID
    year = 0

    def __init__(self, code, zwjc, pinyin, category, orgId):
        self.code = code
        self.zwjc = zwjc
        self.pinyin = pinyin
        self.category = category
        self.orgId = orgId


class StockList:
    def __init__(self, filepath=Contants.ALL_STOCK_PATH):
        self.filepath = filepath
        self.stock_list = []

    def parse(self):
        file1 = open(self.filepath, 'rb').read()
        data = json.loads(file1)
        stockList = data['stockList']
        for item in stockList:
            stock = Stock(item['code'], item['zwjc'], item['pinyin'], item['category'], item['orgId'])
            # print(item)
            self.stock_list.append(stock)
        print("parse stock list success, size = " + str(len(self.stock_list)))
        return self.stock_list

    def get_stock_by_name(self, name):
        if len(self.stock_list) == 0:
            self.parse()
        for stock in self.stock_list:
            if name == stock.zwjc:
                return stock
        return None

    def get_stock_by_code(self, code):
        if len(self.stock_list) == 0:
            self.parse()
        for stock in self.stock_list:
            if code == stock.code:
                return stock
        return None


if __name__ == '__main__':
    sl = StockList()
    result = sl.parse()
